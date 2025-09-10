# 📦 Backfill QuickBooks Online (QBO) → Postgres con Mage

Este proyecto implementa la carga histórica de datos desde **QuickBooks Online (QBO)** hacia **Postgres**, usando **Mage** como orquestador.  
Incluye **3 pipelines independientes** para las entidades principales:

- `qb_customers_backfill` → tabla `raw.qb_customers`
- `qb_invoices_backfill` → tabla `raw.qb_invoices`
- `qb_items_backfill` → tabla `raw.qb_items`

---

## ⚙️ Características principales

- Segmentación de rangos `[fecha_inicio, fecha_fin)` en: `day | week | month | quarter | year` (UTC).  
- Extracción vía API QBO con **OAuth2** (refresh token).  
- Paginación completa con **backoff exponencial** y **circuit breaker**.  
- Carga **idempotente** en Postgres (`INSERT … ON CONFLICT`).  
- Emisión de métricas por tramo: páginas, filas, insertados, actualizados, omitidos, duración.  
- Logs en formato **JSON estructurado** por fase (`auth | extract | load`).  
- Operación en **UTC**. La zona horaria de referencia es `America/Guayaquil (UTC−05)`.  

---

## 🏗️ Arquitectura

![Arquitectura](https://github.com/user-attachments/assets/b16fe115-7484-4204-a9e2-51cdf0a6ef34)

---

## 🚀 Cómo levantar el proyecto

1. **Clonar el repositorio**  
   - `git clone https://github.com/usuario/repositorio.git`  
   - `cd repositorio`  

2. **Levantar la stack**  
   - `docker compose up -d`  

3. **Abrir Mage**  
   - En el navegador: [http://localhost:6789](http://localhost:6789)  

4. **Abrir pgAdmin**  
   - Validar conexión al Postgres levantado con `docker-compose`.  

5. **Cargar los DDL de RAW**  
   - Solo si Postgres no los autoejecuta al arrancar.  

6. **Verificar que existan las tablas**  
   - `raw.qb_customers`  
   - `raw.qb_invoices`  
   - `raw.qb_items`  

---

## 🔑 Gestión de secretos

Configurar en **Mage Secrets** los siguientes valores:

- `QBO_CLIENT_ID=xxxxxxxx`  
- `QBO_CLIENT_SECRET=xxxxxxxx`  
- `QBO_REFRESH_TOKEN=xxxxxxxx`  
- `QBO_REALM_ID=xxxxxxxx`  
- `PG_HOST=postgres`  
- `PG_PORT=5432`  
- `PG_DB=mi_base`  
- `PG_USER=mi_usuario`  
- `PG_PASSWORD=mi_password`

### 🔐 Gestión de secretos (propósito/rotación/responsables)
| Nombre              | Propósito                                   | Rotación recomendada              | Responsable     |
|---------------------|---------------------------------------------|-----------------------------------|-----------------|
| QBO_CLIENT_ID       | ID de la app QBO (OAuth2)                   | Al rotar credenciales QBO         | Data Eng / TI   |
| QBO_CLIENT_SECRET   | Secreto de la app QBO (OAuth2)              | Al rotar credenciales QBO         | Data Eng / TI   |
| QBO_REFRESH_TOKEN   | Refresh Token para emitir Access Tokens     | Si expira/rota o hay invalid_grant| Data Eng / TI   |
| QBO_REALM_ID        | Company ID de QBO                           | Estática (por compañía)           | Data Eng        |
| PG_HOST/PORT/DB     | Conexión a Postgres                         | Si cambia infraestructura          | Plataforma      |
| PG_USER/PASSWORD    | Credenciales de Postgres                    | Rotación periódica                | Plataforma      |

> Sin valores en el repo; solo nombres, propósito y política.

📸 Ejemplo:  
<img width="1211" height="945" alt="image" src="https://github.com/user-attachments/assets/ec1813fa-60f8-4ac9-a506-ab452520a948" />

---

## 📂 Definiciones de Pipelines

### `qb_customers_backfill`
- `chunk_fecha_customers`  
- `extract_qbo_customers`  
- `load_postgres_customers`  

### `qb_invoices_backfill`
- `chunk_fecha_invoices`  
- `extract_qbo_invoices`  
- `load_postgres_invoices`  

### `qb_items_backfill`
- `chunk_fecha_items`  
- `extract_qbo_items`  
- `load_postgres_items`  

---

## ⏱️ Triggers One-Time

- **Tipo**: One-time (ad hoc)  
- **Variables obligatorias**:  
  - `fecha_inicio`: ISO UTC, ej. `2025-01-01T00:00:00Z`  
  - `fecha_fin`: ISO UTC, ej. `2025-01-31T00:00:00Z`  
- **Variables opcionales**:  
  - `chunk`: `day | week | month | quarter | year` (default: `day`)  
  - `page_size`: entero (default: `200`)  
- **Política post-ejecución**: al finalizar, deshabilitar o eliminar el trigger para evitar reejecuciones accidentales.

#### 🕒 Documentación de la corrida (UTC ↔ Guayaquil)
- Inicio (UTC): `2025-01-01T00:00:00Z`
- Equivalente Guayaquil (UTC−05): `2024-12-31 19:00:00 America/Guayaquil`
- Parámetros: `fecha_inicio=2025-01-01T00:00:00Z`, `fecha_fin=2025-01-31T00:00:00Z`, `chunk=day`, `page_size=200`

**Política post-ejecución:** trigger **deshabilitado** tras finalizar OK (evita relanzamientos accidentales).

📸 Evidencia:  
<img width="1838" height="928" src="https://github.com/user-attachments/assets/f10ebf82-e556-439d-9e30-e62b28e72aa1" />  
<img width="1373" height="189" src="https://github.com/user-attachments/assets/19724fdf-90ec-4800-805c-74a77808f6b8" />  
<img width="1894" height="911" src="https://github.com/user-attachments/assets/1844f980-9a39-4696-a0a3-adf35c67950d" />  
<img width="1581" height="358" src="https://github.com/user-attachments/assets/09f9addd-5185-4d11-97eb-584f4e5da59a" />  
<img width="1910" height="916" src="https://github.com/user-attachments/assets/b4251b3a-116a-4644-acfb-a2aced2f97cb" />  
<img width="1652" height="407" src="https://github.com/user-attachments/assets/1c0c08a4-3e04-44f6-886b-5f56aa5a2aea" />  
<img width="1905" height="889" src="https://github.com/user-attachments/assets/c37e85c3-1653-4149-b077-129b31bb1177" />  

---

## 📊 Parámetros, Segmentación y Reintentos

- **Parámetros comunes**: `fecha_inicio`, `fecha_fin`, `chunk`, `page_size`.  
- **Segmentación temporal**: el bloque `chunk_fecha` divide el rango en intervalos y los procesa independientemente.  
- **Límites y paginación**: avanza con `startposition` hasta agotar resultados.  
- **Reintentos y tolerancia a fallos**:  
  - Backoff exponencial en 429/5xx.  
  - Circuit breaker por request.  
  - Manejo de 401 → refresca token una vez y reintenta.  

---

## 🗄️ Esquema RAW

### Tablas
- `raw.qb_customers`  
- `raw.qb_invoices`  
- `raw.qb_items`  

### Columnas obligatorias
- `id` (PK)  
- `payload JSONB`  
- `ingested_at_utc timestamptz`  
- `extract_window_start_utc timestamptz`  
- `extract_window_end_utc timestamptz`  
- `page_number int`  
- `page_size int`  
- `request_payload JSONB`  

### Idempotencia
- Definida con `ON CONFLICT (id) DO UPDATE`.  

---

## ✅ Validaciones y Volumetría

Ejemplo de volumetría por rango:

```sql
SELECT 'customers' AS entity, COUNT(*) 
FROM raw.qb_customers
WHERE extract_window_start_utc >= '2025-01-01' 
  AND extract_window_end_utc   <= '2025-12-31'
UNION ALL
SELECT 'invoices', COUNT(*) FROM raw.qb_invoices
WHERE extract_window_start_utc >= '2025-01-01' 
  AND extract_window_end_utc   <= '2025-12-31'
UNION ALL
SELECT 'items', COUNT(*) FROM raw.qb_items
WHERE extract_window_start_utc >= '2025-01-01' 
  AND extract_window_end_utc   <= '2025-12-31';
```
**Cómo interpretar:**
- **Días vacíos**: si `0` en un día hábil, revisar ese **tramo** (token/429/5xx/filtro).
- **Extract vs Load:** `rows_read` (logs) ≈ filas insertadas+actualizadas en RAW; desvíos grandes ⇒ revisar paginación o errores.
- **Idempotencia:** re-ejecutar el mismo tramo **no cambia** COUNT gracias a `ON CONFLICT (id)`.

## 📸 Evidencia

<img width="1400" height="747" src="https://github.com/user-attachments/assets/87118686-17e9-45bb-ba85-10528e812f42" />

---
## 🔁 Idempotencia

Antes de re-ejecutar el tramo:

```sql
SELECT COUNT(*) FROM raw.qb_items;
```
Después de re-ejecutar el mismo tramo → el valor DEBE ser idéntico gracias a ON CONFLICT.

## 📸 Evidencia

<img width="1393" height="706" src="https://github.com/user-attachments/assets/40dc21fc-b9a7-40ae-9c15-9c52bad01ee7" />

---

## 🛠️ Troubleshooting

- **Autenticación (invalid_grant / 401):** actualizar `QBO_REFRESH_TOKEN` en Mage Secrets y reejecutar el tramo fallido.  
- **Paginación:** revisar `page_size` y `startposition`.  
- **Errores 5xx / Rate Limit:** verificar reintentos con backoff; si persiste, reducir `page_size`.  
- **Timezones:** usar siempre **UTC** (Guayaquil = UTC−5).  
- **Almacenamiento:** el `payload` se guarda en **JSONB**; si crece demasiado, considerar particionar por mes o archivar.  
- **Permisos:**  
  - Postgres: usuario con privilegios `INSERT/UPDATE` sobre `raw.*`.  
  - QBO: app con permisos para leer **Customers**, **Invoices** e **Items**.

---

## 🗃️ Definiciones de Base de Datos (DDL)

```sql
CREATE SCHEMA IF NOT EXISTS raw;

-- Customers
CREATE TABLE IF NOT EXISTS raw.qb_customers (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);

-- Items
CREATE TABLE IF NOT EXISTS raw.qb_items (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);

-- Invoices
CREATE TABLE IF NOT EXISTS raw.qb_invoices (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);
```
---

## 🧪 Pruebas y Validaciones de Calidad

- **Volumetría:** por entidad y rango.  
- **Idempotencia:** reejecutar un tramo debe dar el mismo `COUNT(*)`.  
- **Spot-check del payload:** revisar campos clave en JSON.  
- **Verificación de metadatos:** comprobar timestamps y ventana de extracción.  

---

## 📋 Checklist de Aceptación

- [x] Mage y Postgres se comunican por nombre de servicio.  
- [x] Todos los secretos están en Mage (no en el repo).  
- [x] Pipelines aceptan `fecha_inicio` y `fecha_fin` en UTC.  
- [x] Trigger one-time configurado, ejecutado y luego deshabilitado.  
- [x] Esquema RAW creado con payload completo y metadatos.  
- [x] Idempotencia verificada (`ON CONFLICT`).  
- [x] Paginación y rate limits manejados y documentados.  
- [x] Validaciones de volumetría registradas como evidencia.  
- [x] Runbook de reintentos y reanudación disponible.  

---

## 📜 Licencia

MIT © 2025
