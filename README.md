Backfill de QuickBooks Online (QBO) hacia Postgres usando Mage. Incluye 3 pipelines independientes:

qb_customers_backfill → RAW: raw.qb_customers

qb_invoices_backfill → RAW: raw.qb_invoices

qb_items_backfill → RAW: raw.qb_items

Cada pipeline:

Segmenta un rango [fecha_inicio, fecha_fin) por day|week|month|quarter|year (UTC).

Extrae por API QBO (SQL) con OAuth2 (refresh token).

Hace paginación completa con backoff exponencial y circuit breaker.

Carga idempotente a RAW (INSERT … ON CONFLICT).

Emite métricas por tramo (páginas, filas, insertados, actualizados, omitidos, duración) con logging JSON por fase (auth/extract/load).

Zona horaria operativa: America/Guayaquil (UTC−05). Todos los filtros y marcas de tiempo se gestionan en UTC.

Diagrama de arquitectura

<img width="637" height="751" alt="image" src="https://github.com/user-attachments/assets/b16fe115-7484-4204-a9e2-51cdf0a6ef34" />


Pasos para levantar contenedores y configurar el proyecto
1. Clona el repositorio

2. Levanta la stack:
docker compose up -d

3. Abre Mage:
http://localhost:6789

5. Abre pgAdmin y verifica conexión al Postgres del compose.

6. Carga los DDL de RAW (si tu Postgres no los autoejecuta al arrancar)

7. Verifica que existan: raw.qb_customers, raw.qb_invoices, raw.qb_items.

Gestión de secretos

- Configura los secretos en Mage:
  
QBO_CLIENT_ID

QBO_CLIENT_SECRET

QBO_REFRESH_TOKEN

QBO_REALM_ID

PG_HOST

PG_PORT

PG_DB

PG_USER

PG_PASSWORD

Evidencia:

<img width="1211" height="945" alt="image" src="https://github.com/user-attachments/assets/ec1813fa-60f8-4ac9-a506-ab452520a948" />


Definiciones del proyecto de Mage y de las pipelines

Pipelines:

- qb_customers_backfill
  - Bloques: chunk_fecha_customers, extract_qbo_customers, load_postgres_customers

- qb_invoices_backfill

  - Bloques: chunk_fecha_invoices, extract_qbo_invoices, load_postgres_invoices

- qb_items_backfill

  - Bloques: chunk_fecha-items, extract_qbo_items, load_postgres_items

Trigger one-time (por pipeline):

- Tipo: One-time (ad hoc)

- Runtime vars (obligatorias):

  - fecha_inicio: ISO UTC, ej. 2025-01-01T00:00:00Z

  - fecha_fin: ISO UTC, ej. 2025-01-31T00:00:00Z

- Runtime vars (opcionales):

  - chunk: day|week|month|quarter|year (default: day)

  - page_size: entero (default: 200)

Política post-ejecución:

- Tras finalizar correctamente, deshabilitar el trigger o eliminarlo para evitar re-ejecuciones accidentales.

Evidencia:
<img width="1838" height="928" alt="image" src="https://github.com/user-attachments/assets/f10ebf82-e556-439d-9e30-e62b28e72aa1" />
<img width="1373" height="189" alt="image" src="https://github.com/user-attachments/assets/19724fdf-90ec-4800-805c-74a77808f6b8" />
<img width="1894" height="911" alt="image" src="https://github.com/user-attachments/assets/1844f980-9a39-4696-a0a3-adf35c67950d" />
<img width="1581" height="358" alt="image" src="https://github.com/user-attachments/assets/09f9addd-5185-4d11-97eb-584f4e5da59a" />
<img width="1910" height="916" alt="image" src="https://github.com/user-attachments/assets/b4251b3a-116a-4644-acfb-a2aced2f97cb" />
<img width="1652" height="407" alt="image" src="https://github.com/user-attachments/assets/1c0c08a4-3e04-44f6-886b-5f56aa5a2aea" />
<img width="1905" height="889" alt="image" src="https://github.com/user-attachments/assets/c37e85c3-1653-4149-b077-129b31bb1177" />

Detalle de los tres pipelines: parámetros, segmentación, límites, reintentos, runbook

Parámetros de entrada (comunes)

- fecha_inicio / fecha_fin (UTC, ISO YYYY-MM-DDTHH:MM:SSZ)
- chunk (por defecto week)
- page_size (por defecto 200)

Segmentación temporal

- chunk_fecha divide [fecha_inicio, fecha_fin) en tramos de día/semana/mes/trimestre/año.
- Cada tramo se procesa independientemente (token, extracción, carga, métricas).

Límites y paginación

- maxresults = page_size (p. ej. 200)
- Avanza con startposition hasta que la página devuelva < page_size

Reintentos y tolerancia a fallos

- Reintentos con backoff exponencial y tope (HTTP 429/5xx)
- Circuit breaker por request (límite de intentos)
- Manejo de 401: se refresca el token una vez y se reintenta el tramo
- Registros de errores/transientes en logging JSON (fase auth/extract)

Carga a RAW e idempotencia

- INSERT … ON CONFLICT (id) DO UPDATE (idempotente)
- Persistimos payload JSONB completo + metadatos de ingesta y ventana

Runbook (resumen operativo)

- Falla de autenticación (invalid_grant/401) → Actualiza secretos, reejecuta ese tramo.
- Rate Limit / 5xx → Backoff + circuit breaker ya aplica; si falla, reejecuta el tramo.
- Reanudar → detecta el último tramo exitoso y lanza el siguiente.
- Verificación → COUNT(*) esperado, “días vacíos” y idempotencia (mismo tramo = mismo conteo).

Trigger one-time: UTC ↔ Guayaquil

- Entrada siempre en UTC (ej. 2025-01-01T00:00:00Z).
- Conversiones: Guayaquil = UTC − 5 horas (no usa DST).
  - 2025-01-01T00:00:00Z (UTC) ↔ 2024-12-31 19:00:00 (America/Guayaquil)
- Política: al terminar, deshabilitar o eliminar el trigger.

Esquema RAW: tablas, claves, metadatos, idempotencia

Tablas

- raw.qb_customers
- raw.qb_invoices
- raw.qb_items

Claves / columnas obligatorias

- id (PK)
- payload JSONB (respuesta completa de QBO)
- ingested_at_utc timestamptz
- extract_window_start_utc timestamptz
- extract_window_end_utc timestamptz
- page_number int
- page_size int
- request_payload JSONB (parámetros efectivos del request)

Idempotencia

- Definida por PK = id y ON CONFLICT (id) DO UPDATE.

Validaciones / Volumetría: cómo correrlas y cómo interpretar

Volumetría por rango (ejemplo)
SELECT 'customers' AS entity, COUNT(*) AS rows
FROM raw.qb_customers
WHERE extract_window_start_utc >= '2025-01-01'::timestamptz
  AND extract_window_end_utc   <= '2025-12-31'::timestamptz
UNION ALL
SELECT 'invoices', COUNT(*) FROM raw.qb_invoices
WHERE extract_window_start_utc >= '2025-01-01'::timestamptz
  AND extract_window_end_utc   <= '2025-12-31'::timestamptz
UNION ALL
SELECT 'items', COUNT(*) FROM raw.qb_items
WHERE extract_window_start_utc >= '2025-01-01'::timestamptz
  AND extract_window_end_utc   <= '2025-12-31'::timestamptz;


Evidencia:
<img width="1400" height="747" alt="image" src="https://github.com/user-attachments/assets/87118686-17e9-45bb-ba85-10528e812f42" />

Idempotencia
-- Antes de re-ejecutar el tramo
SELECT COUNT(*) FROM raw.qb_items

-- Re-ejecuta ese mismo tramo en Mage

-- Después: el valor DEBE ser idéntico (gracias a ON CONFLICT).

Evidencia:
<img width="1393" height="706" alt="image" src="https://github.com/user-attachments/assets/40dc21fc-b9a7-40ae-9c15-9c52bad01ee7" />

Troubleshooting

Autenticación (invalid_grant / 401)

- QBO_REFRESH_TOKEN inválido/expirado/rotado → actualiza secrets.
- Reejecuta solo el tramo fallido.

Paginación

- Si deja de paginar, verifica page_size y la lógica de startposition.

Límites / 5xx

- Verifica que se estén aplicando los reintentos con backoff y el circuit breaker.
- Si aún falla, reintenta el tramo; si persiste, reduce page_size.

Timezones

- Entrada/salida en UTC. Guayaquil = UTC − 5. Evita valores locales en fecha_inicio/fecha_fin.

Almacenamiento

- payload se guarda como JSONB.
- Si crece mucho: considera particionar por mes o archivar tramos antiguos.

Permisos

- Postgres: el usuario debe tener INSERT/UPDATE sobre raw.*.
- QBO: la app debe tener alcances para leer Customers/Invoices/Items.




