if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
import psycopg
from datetime import datetime, timezone

def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


@data_exporter
def export_invoices_to_postgres(records, **kwargs) -> None:
    """
    Exporta registros a la tabla raw.qb_invoices en Postgres.

    Cumple:
      - Capa RAW con payload completo + metadatos obligatorios.
      - Idempotencia con ON CONFLICT (upsert por clave primaria id).

    Cumple:
      - Logging estructurado por fase "load" con métricas finales.
    """
    # integridad antes de abrir conexión 
    if not records:
        print(json.dumps({
            "phase": "load", "entity": "invoices", "ts": _now_utc_iso(),
            "status": "skip", "reason": "no_records"
        }))
        return

    # Secrets de conexión
    host = get_secret_value('PG_HOST')
    port = int(get_secret_value('PG_PORT'))
    db = get_secret_value('PG_DB')
    user = get_secret_value('PG_USER')
    password = get_secret_value('PG_PASSWORD')

    conn_str = f"host={host} port={port} dbname={db} user={user} password={password}"

    # Upsert y conteo de insert/update con RETURNING (xmax=0)
    # registrar filas insertadas/actualizadas
    # idempotencia (ON CONFLICT)
    sql = """
    INSERT INTO raw.qb_invoices (
        id, payload, ingested_at_utc,
        extract_window_start_utc, extract_window_end_utc,
        page_number, page_size, request_payload
    )
    VALUES (
        %(id)s, %(payload)s, %(ingested_at_utc)s,
        %(extract_window_start_utc)s, %(extract_window_end_utc)s,
        %(page_number)s, %(page_size)s, %(request_payload)s
    )
    ON CONFLICT (id) DO UPDATE SET
        payload = EXCLUDED.payload,
        ingested_at_utc = EXCLUDED.ingested_at_utc,
        extract_window_start_utc = EXCLUDED.extract_window_start_utc,
        extract_window_end_utc = EXCLUDED.extract_window_end_utc,
        page_number = EXCLUDED.page_number,
        page_size = EXCLUDED.page_size,
        request_payload = EXCLUDED.request_payload
    RETURNING (xmax = 0) AS inserted;
    """

    inserted = 0
    updated = 0
    skipped = 0  # métrica de omitidos si falta PK o metadatos

    # Validación mínima por registro
    def _valid(r):
        return all([
            r.get("id"),
            r.get("payload") is not None,
            r.get("ingested_at_utc"),
            r.get("extract_window_start_utc"),
            r.get("extract_window_end_utc"),
        ])

    # Inicio de fase de carga
    print(json.dumps({
        "phase": "load", "entity": "invoices", "ts": _now_utc_iso(),
        "status": "start", "incoming_records": len(records)
    }))

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for r in records:
                if not _valid(r):
                    skipped += 1
                    print(json.dumps({
                        "phase": "load", "entity": "invoices", "ts": _now_utc_iso(),
                        "status": "skipped", "reason": "invalid_row_min_requirements",
                        "id": r.get("id")
                    }))
                    continue

                params = {
                    "id": r["id"],
                    "payload": json.dumps(r["payload"]),  # JSONB en DDL
                    "ingested_at_utc": r["ingested_at_utc"],
                    "extract_window_start_utc": r["extract_window_start_utc"],
                    "extract_window_end_utc": r["extract_window_end_utc"],
                    "page_number": r.get("page_number"),
                    "page_size": r.get("page_size"),
                    "request_payload": json.dumps(r.get("request_payload")),
                }

                cur.execute(sql, params)
                was_insert = cur.fetchone()[0]
                if was_insert:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    total = inserted + updated
    # Reporte final
    print(json.dumps({
        "phase": "load", "entity": "invoices", "ts": _now_utc_iso(),
        "status": "done",
        "inserted": inserted, "updated": updated, "skipped": skipped,
        "total_processed": total, "total_input": len(records)
    }))

    print(f"[load invoices] Insertados={inserted} | Actualizados={updated} | Omitidos={skipped} | Total={total} (raw.qb_invoices)")
