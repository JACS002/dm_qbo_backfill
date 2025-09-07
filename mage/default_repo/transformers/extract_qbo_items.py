# --- EXTRACT: QBO Items (REAL API) ---
# Cumple 7.1–7.5 (ver comentarios por sección).

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime, timezone
import base64
import time
import requests
import json


TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_BASE  = "https://sandbox-quickbooks.api.intuit.com"   # sandbox

# Filtro por defecto para Items
ITEM_FILTER_FIELD = "MetaData.LastUpdatedTime"

# Parámetros de robustez
MAX_ATTEMPTS_PER_REQ = 6
BACKOFF_BASE_SECONDS = 1.5
BACKOFF_CAP_SECONDS  = 30


def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _get_access_token():
    """
    Usa CLIENT_ID/CLIENT_SECRET + REFRESH_TOKEN para obtener un access_token.
    Cumple 7.2: OAuth 2.0 (se invoca por tramo).
    """
    client_id = get_secret_value('QBO_CLIENT_ID')
    client_secret = get_secret_value('QBO_CLIENT_SECRET')
    refresh_token = get_secret_value('QBO_REFRESH_TOKEN')

    if not all([client_id, client_secret, refresh_token]):
        raise Exception("Faltan secretos QBO: QBO_CLIENT_ID / QBO_CLIENT_SECRET / QBO_REFRESH_TOKEN")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)

    # Logging por fase
    print(json.dumps({
        "phase": "auth", "entity": "items", "ts": _now_utc_iso(),
        "status_code": resp.status_code, "ok": resp.ok
    }))

    if resp.status_code == 400 and "invalid_grant" in (resp.text or ""):
        raise PermissionError("invalid_grant: refresh_token inválido/expirado/rotado. Reautorizar QBO.")

    if resp.status_code != 200:
        raise Exception(f"Token error {resp.status_code}: {resp.text}")

    return resp.json()["access_token"]


def _qbo_time(iso_z: str) -> str:
    """
    Para campos TIMESTAMP (UTC) QuickBooks prefiere offset explícito +00:00.
    Cumple 7.2: coherencia UTC para filtros temporales.
    """
    return iso_z.replace('Z', '+00:00') if iso_z.endswith('Z') else iso_z


def _post_with_retries(url, headers, data, label="query"):
    """
    POST con reintentos/backoff y circuit breaker.
    rate limits y 5xx con backoff exponencial + límite de intentos
    """
    attempts = 0
    while True:
        attempts += 1
        try:
            resp = requests.post(url, headers=headers, data=data, timeout=60)
        except Exception as e:
            print(json.dumps({
                "phase": "extract", "entity": "items", "stage": label, "ts": _now_utc_iso(),
                "attempt": attempts, "transport_error": str(e)
            }))
            if attempts >= MAX_ATTEMPTS_PER_REQ:
                raise TimeoutError(f"circuit_breaker: transporte fallido {attempts} veces")
            time.sleep(min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS ** attempts))
            continue

        print(json.dumps({
            "phase": "extract", "entity": "items", "stage": label, "ts": _now_utc_iso(),
            "attempt": attempts, "status_code": resp.status_code
        }))

        if resp.status_code == 200:
            return resp

        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            if attempts >= MAX_ATTEMPTS_PER_REQ:
                raise TimeoutError(f"circuit_breaker: {resp.status_code} tras {attempts} intentos")
            time.sleep(min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS ** attempts))
            continue

        if resp.status_code == 401:
            raise PermissionError("401 Unauthorized (token expirado).")

        raise Exception(f"QBO POST error {resp.status_code}: {resp.text}")


def _build_item_sql(start_iso, end_iso, start_position, max_results):
    """
    Construye el SQL para Item según el campo de filtro elegido (timestamp).
    Rango [start, end) en UTC.
    filtros históricos por ventana.
    """
    start_val = _qbo_time(start_iso)
    end_val   = _qbo_time(end_iso)
    sql = (
        "select * from Item "
        f"where {ITEM_FILTER_FIELD} >= '{start_val}' "
        f"and   {ITEM_FILTER_FIELD} <  '{end_val}' "
        f"startposition {start_position} maxresults {max_results}"
    )
    return sql


def _qbo_query_items(access_token, realm_id, start_position=1, max_results=200,
                     start_iso=None, end_iso=None):
    """
    Ejecuta /query para traer Item por ventana temporal.
    filtros históricos (UTC) + paginación hasta agotar resultados.
    """
    if not (start_iso and end_iso):
        raise Exception("Faltan start_iso y end_iso para la consulta.")

    sql = _build_item_sql(start_iso, end_iso, start_position, max_results)

    url = f"{QBO_BASE}/v3/company/{realm_id}/query"   # sin minorversion
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/text",           # requerido por tu sandbox
    }

    resp = _post_with_retries(url, headers, sql, label="items.query")
    js = resp.json()
    qres = js.get("QueryResponse", {})
    rows = qres.get("Item", []) or []

    has_more = len(rows) == max_results
    next_pos = start_position + max_results if has_more else None

    # Métrica por página
    print(json.dumps({
        "phase": "extract", "entity": "items", "ts": _now_utc_iso(),
        "startpos": start_position, "returned_rows": len(rows), "has_more": has_more
    }))

    return rows, has_more, next_pos


def _fetch_items_window(access_token, realm_id, start_iso, end_iso, page_size):
    """
    Trae todos los Item en [start_iso, end_iso).
    Devuelve:
      - records: lista [{'id','payload','page_number'}]
      - pages_read: número de páginas leídas
      - rows_read: total de filas devueltas

    registra páginas y filas leídas por tramo.
    paginación completa.
    """
    records = []
    pos = 1
    page_number = 0
    total_rows = 0

    while True:
        page_number += 1
        try:
            rows, has_more, next_pos = _qbo_query_items(
                access_token, realm_id,
                start_position=pos, max_results=page_size,
                start_iso=start_iso, end_iso=end_iso,
            )
        except PermissionError:
            # Devolver control al caller para renovar token (7.2)
            raise

        for c in rows:
            records.append({"id": c["Id"], "payload": c, "page_number": page_number})
        total_rows += len(rows)

        if not has_more:
            break
        pos = next_pos

    return records, page_number, total_rows


def _normalize_tramos(data, **kwargs):
    """
    Normaliza la entrada a list[dict] con 'start','end','page_size','metrics'.
    (Los tramos provienen de chunk_fecha)
    """
    if data is None:
        fi = kwargs.get('fecha_inicio')
        ff = kwargs.get('fecha_fin')
        if fi and ff:
            return [{'start': fi, 'end': ff, 'page_size': 200, 'metrics': {
                'pages_read': 0, 'rows_read': 0,
                'rows_inserted': 0, 'rows_updated': 0,
                'duration_secs': 0.0, 'status': 'pending'
            }}]
        return []

    try:
        to_dict = getattr(data, 'to_dict', None)
        if callable(to_dict):
            return data.to_dict('records')
    except Exception:
        pass

    if isinstance(data, list):
        if data and isinstance(data[0], str):
            try:
                return [json.loads(s) for s in data]
            except Exception:
                pass
        return data

    if isinstance(data, str):
        try:
            obj = json.loads(data)
            if isinstance(obj, dict):
                return [obj]
            if isinstance(obj, list):
                return obj
        except Exception:
            return []

    return []


# ====== Bloque principal ======
@transformer
def transform(data=None, *args, **kwargs):
    """
    Input real de Mage: `data` (sale de chunk_fecha).
    Normalizamos a list[dict] con claves start/end/page_size y extraemos.

    Cumplimientos:
      -métricas por tramo (páginas, filas, duración; inserts/updates se llenan en exporter).
      - token por tramo, manejo de 401/invalid_grant, reintentos y paginación completa.
      - metadatos RAW por registro (ingested_at_utc, ventanas, page_number/page_size, request_payload).
      - UTC consistente y reprocesos seguros (idempotencia en exporter).
      - logging estructurado por fase.
    """
    tramos = _normalize_tramos(data, **kwargs)
    if not tramos:
        print("No hay tramos")
        return []

    realm_id = get_secret_value('QBO_REALM_ID')
    if not realm_id:
        raise Exception("Falta QBO_REALM_ID en Secrets.")

    out = []

    for t in tramos:
        start_iso = t.get('start')
        end_iso   = t.get('end')
        page_size = int(t.get('page_size', 200))
        metrics   = t.get('metrics') or {
            'pages_read': 0, 'rows_read': 0,
            'rows_inserted': 0, 'rows_updated': 0,
            'duration_secs': 0.0, 'status': 'pending'
        }

        if not start_iso or not end_iso:
            print(json.dumps({
                "phase": "extract", "entity": "items", "ts": _now_utc_iso(),
                "status": "skip", "reason": "tramo_sin_fechas",
                "start": start_iso, "end": end_iso
            }))
            continue

        # Token por tramo
        try:
            access_token = _get_access_token()
        except PermissionError as e:
            metrics['status'] = 'failed_auth'
            print(json.dumps({
                "phase": "auth", "entity": "items", "ts": _now_utc_iso(),
                "status": "failed", "error": str(e),
                "start": start_iso, "end": end_iso
            }))
            continue

        t0 = time.time()
        print(json.dumps({
            "phase": "extract", "entity": "items", "ts": _now_utc_iso(),
            "status": "start", "start": start_iso, "end": end_iso,
            "page_size": page_size, "filter_field": ITEM_FILTER_FIELD
        }))

        try:
            # Extrae toda la ventana
            records, pages_read, rows_read = _fetch_items_window(
                access_token, realm_id, start_iso, end_iso, page_size
            )
        except PermissionError:
            # Renueva token una vez y reintenta tramo
            access_token = _get_access_token()
            records, pages_read, rows_read = _fetch_items_window(
                access_token, realm_id, start_iso, end_iso, page_size
            )

        duration = time.time() - t0

        # Actualiza métricas tramo
        metrics['pages_read']    = int(pages_read)
        metrics['rows_read']     = int(rows_read)
        metrics['duration_secs'] = round(duration, 3)
        metrics['status']        = 'extracted'

        # Marca de ingesta UTC
        ingested_at = _now_utc_iso()

        # Empaquetar registros con metadatos RAW
        for r in records:
            page_number = r["page_number"]
            out.append({
                "id": r["id"],                      # PK de Item
                "payload": r["payload"],            # JSON completo de Item
                "ingested_at_utc": ingested_at,
                "extract_window_start_utc": start_iso,
                "extract_window_end_utc": end_iso,
                "page_number": page_number,
                "page_size": page_size,
                "request_payload": {
                    "start": start_iso,
                    "end": end_iso,
                    "page": page_number,
                    "page_size": page_size,
                    "filter_field": ITEM_FILTER_FIELD
                },
                # Idempotencia: en exporter via ON CONFLICT
            })

        # Log consolidado tramo (Cumple 7.5)
        print(json.dumps({
            "phase": "extract", "entity": "items", "ts": _now_utc_iso(),
            "status": "done",
            "start": start_iso, "end": end_iso,
            "pages_read": metrics['pages_read'],
            "rows_read": metrics['rows_read'],
            "duration_secs": metrics['duration_secs']
        }))

    # Resumen total
    print(json.dumps({
        "phase": "extract", "entity": "items", "ts": _now_utc_iso(),
        "status": "completed",
        "tramos": len(tramos), "total_records": len(out)
    }))

    return out
