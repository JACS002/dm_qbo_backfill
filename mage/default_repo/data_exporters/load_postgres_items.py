if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
import psycopg  # v3
from datetime import datetime, timezone


def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


@data_exporter
def export_items_to_postgres(records, **kwargs) -> None:
    """
    Exporta registros a la tabla raw.qb_items en Postgres.

    Cumple 7.3:
      - Capa RAW con payload completo + metadatos obligatorios.
      - Idempotencia con ON CONFLICT (upsert por clave primaria id).

    Cumple 7.5:
      - Logging estructurado por fase "load" con métricas finales.
    """
    # Guardrail de integridad
    if not records:
        print(json.dumps({
            "phase": "load", "entity": "items", "ts": _now_utc_iso(),
            "status": "skip", "reason": "no_records"
        }))
        return

    # Secrets de conexión
    host = get_secret_value('PG_HOST') or 'postgres'
    port = int(get_secret_value('PG_PORT') or 5432)
    db = get_secret_value('PG_DB') or 'dm'
    user = get_secret_value('PG_USER') or 'dm_user'
    password = get_secret_value('PG_PASSWORD') or 'dm_password'

    conn_str = f"host={host} port={port} dbname={db} user={user} password={password}"

    # Upsert y conteo insert/update con RETURNING (xmax=0)
    # registrar filas insertadas/actualizadas
    # idempotencia (ON CONFLICT)
    sql = """
    INSERT INTO raw.qb_items (
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
    updated  = 0
    skipped  = 0  # métrica de omitidos

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
        "phase": "load", "entity": "items", "ts": _now_utc_iso(),
        "status": "start", "incoming_records": len(records)
    }))

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for r in records:
                if not _valid(r):
                    skipped += 1
                    print(json.dumps({
                        "phase": "load", "entity": "items", "ts": _now_utc_iso(),
                        "status": "skipped", "reason": "invalid_row_min_requirements",
                        "id": r.get("id")
                    }))
                    continue

                params = {
                    "id": r["id"],
                    "payload": json.dumps(r["payload"]),  # DDL: JSONB recomendado
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
        "phase": "load", "entity": "items", "ts": _now_utc_iso(),
        "status": "done",
        "inserted": inserted, "updated": updated, "skipped": skipped,
        "total_processed": total, "total_input": len(records)
    }))

    print(f"[load items] Insertados={inserted} | Actualizados={updated} | Omitidos={skipped} | Total={total} (raw.qb_items)")
