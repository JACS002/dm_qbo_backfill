# Reemplaza ESTE import:
# from mage_ai.secrets import get_secret
# POR:
from mage_ai.data_preparation.shared.secrets import get_secret_value
from mage_ai.data_preparation.decorators import data_exporter
import json

try:
    import psycopg   # psycopg v3
    PSYCOPG3 = True
except ImportError:
    PSYCOPG3 = False
    try:
        import psycopg2
    except ImportError:
        psycopg2 = None

@data_exporter
def load_postgres_customers(records, *args, **kwargs):
    if not records:
        print("No hay registros para cargar.")
        return

    host = get_secret_value('PG_HOST') or 'postgres'
    port = int(get_secret_value('PG_PORT') or 5432)
    db = get_secret_value('PG_DB') or 'dm'
    user = get_secret_value('PG_USER') or 'dm_user'
    password = get_secret_value('PG_PASSWORD') or 'dm_password'

    conn_str = f"host={host} port={port} dbname={db} user={user} password={password}"
    sql = """
    INSERT INTO raw.qb_customers (
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
        request_payload = EXCLUDED.request_payload;
    """

    if PSYCOPG3:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                for r in records:
                    params = {
                        "id": r["id"],
                        "payload": json.dumps(r["payload"]),
                        "ingested_at_utc": r["ingested_at_utc"],
                        "extract_window_start_utc": r["extract_window_start_utc"],
                        "extract_window_end_utc": r["extract_window_end_utc"],
                        "page_number": r.get("page_number"),
                        "page_size": r.get("page_size"),
                        "request_payload": json.dumps(r.get("request_payload")),
                    }
                    cur.execute(sql, params)
            conn.commit()
    elif psycopg2 is not None:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                for r in records:
                    params = {
                        "id": r["id"],
                        "payload": json.dumps(r["payload"]),
                        "ingested_at_utc": r["ingested_at_utc"],
                        "extract_window_start_utc": r["extract_window_start_utc"],
                        "extract_window_end_utc": r["extract_window_end_utc"],
                        "page_number": r.get("page_number"),
                        "page_size": r.get("page_size"),
                        "request_payload": json.dumps(r.get("request_payload")),
                    }
                    cur.execute(sql, params)
            conn.commit()
    else:
        raise ImportError("Falta psycopg/psycopg2 en el contenedor Mage.")

    print(f"Cargados/actualizados {len(records)} registros en raw.qb_customers (idempotente).")
