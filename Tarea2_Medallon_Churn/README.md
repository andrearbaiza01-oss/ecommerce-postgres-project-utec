# Plataforma de datos end-to-end: Excel → Postgres → Databricks (medallón) → modelo de churn

Proyecto de la **Tarea 2 — Construcción de Base de Datos** (Maestría en Ciencia de Datos).
Toma el Excel administrativo de *CORPORACIÓN PERUVIANS SUR S.A.C.*, lo persiste en
PostgreSQL con un modelo dimensional, construye un event stream de e-commerce y lo
procesa en Databricks con arquitectura medallón (Bronze → Silver → Gold), cerrando
con un modelo de **churn** productivo en la capa Gold.

---

## ⚠️ Lo primero: qué contiene realmente el Excel

Antes de construir nada revisé el archivo. Es importante decirlo con claridad porque
**no es un event stream**:

| Observación | Detalle |
|---|---|
| Volumen | **30 registros** (filas 4 a 33; la 34 es `TOTALES`) |
| Grano | **1 operación por cliente** — los 30 clientes son distintos, ninguno se repite |
| Temporalidad | **no hay ninguna columna de fecha, hora ni timestamp** |
| Identificadores | no hay `cliente_id`, `order_id` ni `session_id` |
| Naturaleza | es una **tabla de ventas** con columnas calculadas por fórmulas de Excel (`SUMAR.SI`, `BUSCARV`, etc.), más 3 hojas de reportes |
| País / ciudades | 100 % Perú; ciudades Tumbes, Loreto, Tacna; sucursales Puno, Arequipa, Juliaca |

**Consecuencia directa:** un modelo de churn necesita historia temporal por cliente
(recencia, frecuencia, tendencia). Con 30 filas sin fechas y una sola compra por cliente
es **matemáticamente imposible** entrenar o validar algo que no sea ruido.

### Cómo se resolvió, sin inventar datos a escondidas

Se separó estrictamente lo real de lo derivado, y queda marcado en la propia base:

| Capa | Contenido | Origen |
|---|---|---|
| `raw.*` | los 30 registros y los 4 parámetros, **tal cual** | 100 % real |
| `core.*` | modelo estrella: 30 hechos, 19 productos, 6 vendedores, 3 sucursales, 5 métodos de pago, 30 clientes | 100 % real |
| `mart.*` | los 8 reportes + BUSCARV del Excel reescritos en SQL | 100 % real, validado contra el Excel |
| `stream.evento` | **125 319 eventos** de e-commerce sobre 1 530 clientes y 18 meses | catálogos reales + **temporalidad y clientes adicionales simulados** |

En el event stream:

- **Real (del Excel):** productos, categorías, **precios**, sucursales, vendedores,
  métodos de pago, ciudades, mix Empresa/Persona natural y los 30 clientes originales
  (`core.dim_cliente.es_cliente_real = true`).
- **Simulado:** marcas de tiempo, sesiones, 1 500 clientes adicionales
  (`origen_registro = 'STREAM_SINTETICO'`) y la dinámica de comportamiento.
- **Reproducible:** semilla fija (`STREAM_SEED=20261117`) → misma data en cada corrida.
- **Sin etiquetas plantadas:** el abandono es un mecanismo *latente*. En la base
  **no existe ninguna columna de churn**; la etiqueta se deriva en la capa Gold a
  partir de la inactividad observada, exactamente como en un caso real.

Si mañana llega el event stream verdadero del e-commerce, **solo se reemplaza
`scripts/20_generar_event_stream.py`**: todo lo demás (Bronze → Silver → Gold → modelo)
funciona sin cambios.

---

## Arquitectura

```
                     base_datos_..._marielflor.xlsx
                                   │
                    scripts/10_cargar_excel.py (ETL idempotente, hash por fila)
                                   ▼
┌──────────────────────── POSTGRES 16 (Docker, puerto 5435) ────────────────────────┐
│  raw      landing 1:1 del Excel + parámetros + baseline de reportes               │
│  core     modelo estrella (5 dims + dim_fecha + fact_venta con cols. generadas)   │
│  stream   event stream append-only, particionado por mes + fn. incremental        │
│  mart     8 reportes del Excel en SQL + BUSCARV + funnel + RFM                    │
│  audit    bitácora de cargas, watermarks y 34 checks de calidad                   │
└───────────────────────────────────────────────────────────────────────────────────┘
                                   │
         JDBC  ó  scripts/30_exportar_para_databricks.py → Parquet → Volume
                                   ▼
┌──────────────────────────────── DATABRICKS ───────────────────────────────────────┐
│  BRONZE   ingesta fiel + linaje (_ingest_ts, _batch_id, _source_object)           │
│           snapshot de dimensiones  ·  append incremental del stream con watermark │
│  SILVER   tipado, deduplicación, normalización, dominios, CUARENTENA,             │
│           dimensiones conformadas (MERGE/SCD1), fact_venta, orden, sesión         │
│  GOLD     marts de negocio (reportes del Excel validados) · cliente_360 · RFM     │
│           churn_features point-in-time · modelo MLflow · churn_scores accionables │
│  OPS      pipeline_log · watermark · dq_result                                     │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Puesta en marcha

### Opción rápida (todo en un comando)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\00_bootstrap.ps1
```

Levanta Docker, aplica los 5 scripts SQL, carga el Excel, genera el event stream,
corre los checks y exporta los Parquet para Databricks.

### Paso a paso

```powershell
docker compose up -d
```

```powershell
python scripts\db.py
```

```powershell
docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -q -v ON_ERROR_STOP=1 -f /sql/01_schemas_raw.sql
```

```powershell
python scripts\10_cargar_excel.py
```

```powershell
python scripts\20_generar_event_stream.py
```

```powershell
python scripts\30_exportar_para_databricks.py
```

> Los otros cuatro scripts SQL (`02_` a `05_`) se aplican igual que el `01_`,
> en orden. El bootstrap ya lo hace por ti.

### Acceso

| Servicio | Dónde | Credenciales |
|---|---|---|
| PostgreSQL | `localhost:5435` / base `ecommerce_dw` | `ecom` / `ecom_2026` |
| pgAdmin | <http://localhost:5051> | `admin@tarea2.local` / `admin_2026` |

Credenciales de desarrollo local, definidas en `.env`. **No usar estos valores fuera
de tu máquina**: para cualquier despliegue real cambia `.env` (está fuera de la imagen
y no se publica).

Los datos **persisten** en el volumen Docker `ecom_tarea2_pgdata`: `docker compose down`
no los borra; solo `docker compose down -v` lo haría.

### Databricks

Ver **[databricks/README_IMPORT.md](databricks/README_IMPORT.md)**: importación de los
notebooks, carga de los Parquet al Volume, orden de ejecución, widgets, modo JDBC y el Job.

---

## Estructura del repositorio

```
tarea2/
├─ docker-compose.yml              Postgres 16 + pgAdmin, volúmenes persistentes
├─ .env / .env.example             configuración (puertos, credenciales, semilla)
├─ requirements.txt
├─ sql/
│  ├─ 01_schemas_raw.sql           esquemas, landing del Excel, tablas de auditoría
│  ├─ 02_core_modelo_estrella.sql  dimensiones + fact_venta con columnas GENERADAS
│  ├─ 03_stream_eventos.sql        event stream particionado + ingesta incremental
│  ├─ 04_vistas_mart.sql           reportes 1-8 + BUSCARV + funnel + RFM
│  └─ 05_calidad_datos.sql         34 checks con baseline del Excel
├─ scripts/
│  ├─ 00_bootstrap.ps1             orquestador local de punta a punta
│  ├─ db.py                        conexión, bitácora, utilidades
│  ├─ 10_cargar_excel.py           Excel → raw → core
│  ├─ 20_generar_event_stream.py   generador reproducible del event stream
│  ├─ 30_exportar_para_databricks.py  Postgres → Parquet + manifiesto
│  └─ 40_generar_diccionario.py    docs/DICCIONARIO_DATOS.md desde el catálogo real
├─ databricks/
│  ├─ 00_config_setup.py           parámetros, esquemas, helpers (se invoca con %run)
│  ├─ 01_bronze_ingesta.py
│  ├─ 02_silver_limpieza.py
│  ├─ 03_gold_marts.py
│  ├─ 04_gold_churn_features.py
│  ├─ 05_gold_churn_model.py
│  ├─ 06_calidad_y_monitoreo.py
│  ├─ 99_job_pipeline.json         definición del Job (6 tareas encadenadas)
│  └─ README_IMPORT.md
├─ docs/
│  ├─ ARQUITECTURA.md              decisiones de diseño y por qué
│  └─ DICCIONARIO_DATOS.md         31 objetos, todas sus columnas y reglas (autogenerado)
└─ data/export/                    Parquet generados (se suben a Databricks)
```

---

## El modelo de churn

### Definición

El e-commerce es un negocio **no contractual**: el cliente nunca "cancela", simplemente
deja de comprar. Por eso el churn se define por inactividad:

> Un cliente está en **churn** si, habiendo estado activo antes de la fecha de corte `T0`,
> **no coloca ninguna orden válida en los 90 días siguientes**.

```
   ventana de observación (features)       ventana de etiqueta
  ├───────────────────────────────┤T0├──────────────────────────┤ T0+90
   solo datos con event_date ≤ T0        churn = 0 órdenes en (T0, T0+90]
```

### Decisiones metodológicas

| Decisión | Por qué |
|---|---|
| Features **solo** con `event_date ≤ T0` | evita *data leakage*: ninguna variable mira el futuro |
| Población elegible: ≥ 1 orden válida en `[T0-180, T0]` | se modela **retención de la base activa**, no reactivación de inactivos |
| **6 cortes mensuales** (snapshots) | multiplica las filas de entrenamiento y permite validación temporal |
| Split **out-of-time** (2 cortes más recientes = test) | un split aleatorio inflaría las métricas: el mismo cliente aparece en varios cortes |
| Ponderación de clases (`weightCol`) | la clase churn es minoritaria |
| `CrossValidator` 3-fold **solo sobre train** | la selección de hiperparámetros no puede ver el test |

### Variables (≈ 55)

Recencia y ritmo (`recencia_dias`, `intervalo_medio_dias`, **`ratio_recencia_intervalo`**),
frecuencia (órdenes y sesiones a 30/90/365 días), monetario (monto, ticket, margen,
descuento), **tendencia** (variación 90 d vs 90 d previos en órdenes, monto y sesiones),
fricción (cancelaciones, devoluciones, tickets de soporte, pagos fallidos, desuscripciones),
engagement (vistas por sesión, abandono de carrito, tasa de conversión, diversidad de
categorías) y atributos del cliente (tipo, ciudad, canal, método de pago, sucursal).

### Resultados

Validación local de la metodología sobre la data generada (`sklearn`, mismo diseño de
ventanas y split out-of-time que los notebooks):

| Dataset | Valor |
|---|---|
| Filas de entrenamiento | 3 284 (6 cortes) |
| Tasa de churn global | 37,2 % |
| Train / Test | 2 051 / 1 233 |
| **AUC ROC (out-of-time)** | **0,835** (logística) · **0,850** (gradient boosting) |
| PR-AUC | 0,692 / 0,758 |

En Databricks, `05_gold_churn_model` entrena `LogisticRegression` y `GBTClassifier`,
promueve el de mejor AUC en test y **falla el notebook si el AUC cae por debajo de 0,70**
(check crítico `auc_test_mayor_0_70`).

### Salida accionable

`gold.churn_scores` entrega, por cliente activo: probabilidad, decil de riesgo,
segmento (`crítico`/`alto`/`medio`/`bajo`), prioridad comercial (`P1`–`P4`),
**ingreso en riesgo en S/.** (`prob × venta mensual × 3 meses`), **motivo principal**
legible y **acción sugerida**. Además:
`churn_lift_deciles` (lift y captura acumulada), `churn_importancia_variables`,
`churn_modelo_metricas`, `churn_resumen_ejecutivo` y `churn_monitoreo` (drift entre corridas).

---

## Calidad de datos

**En Postgres** — `CALL audit.sp_ejecutar_checks();` ejecuta **34 checks**:
completitud, consistencia `raw` ↔ `core`, integridad referencial, coherencia de
`ESTADO` con la ganancia y **comparación contra el baseline declarado en las hojas de
reportes del Excel**.

```powershell
docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -c "SELECT * FROM audit.v_ultimo_check WHERE resultado='FAIL';"
```

Estado actual: **34/34 PASS, 0 fallas críticas**.

**En Databricks** — cada capa escribe en `tarea2_ops.dq_result` y corta la ejecución
(`AssertionError`) si un check `ERROR` falla. Los eventos inválidos **no se descartan en
silencio**: van a `silver.evento_rechazos` con su motivo. `06_calidad_y_monitoreo`
reconcilia Excel → Bronze → Silver → Gold y verifica que
`bronze = silver + rechazos`.

### Trazabilidad de las fórmulas del Excel

Las columnas calculadas se replican **tres veces de forma independiente** y se comparan:

| Medida | Fórmula Excel | Postgres (columna generada) | Silver (PySpark) |
|---|---|---|---|
| `SUBTOTAL` | `=PRODUCT(I,J)` | `cantidad * precio` | ✔ recalculado y comparado |
| `IGV (18%)` | `=L*$M$2` | `subtotal * 0.18` | ✔ |
| `UTILIDAD` | `=L*$N$2` | `subtotal * 0.25` | ✔ |
| `COSTO UNITARIO` | `=J*$O$2` | `precio * 0.60` | ✔ |
| `TOTAL DE VENTA` | `=SUM(L,M)` | `subtotal * 1.18` | ✔ |
| `VENTA TOTAL ($)` | `=Q/$P$2` | `total / 3.75` | ✔ |
| `GANANCIA` | `=L-P` | `subtotal * 0.40` | ✔ |
| `ESTADO` | `=IF(S4>0,...)` | `CASE WHEN ganancia > 0 ...` | ✔ |

> **Detalle encontrado en el Excel:** los rótulos `N1:P1` (`Margen Util.`, `Factor Costo`,
> `TC`) están **desplazados una celda a la izquierda** respecto a los valores que
> realmente usan las fórmulas (`M2`=0,18 IGV · `N2`=0,25 margen · `O2`=0,60 factor
> costo · `P2`=3,75 TC). El ETL lee **por celda, no por rótulo**, y lo documenta en
> `raw.parametros_excel`.

### Cifras verificadas contra el Excel

| Concepto | Valor |
|---|---|
| Registros | 30 |
| Subtotal total | S/. 36 830,00 (Puno 15 336 · Arequipa 8 885 · Juliaca 12 609) |
| Total con IGV | S/. 43 459,40 |
| Ganancia | S/. 14 732,00 |
| Venta en USD | $ 11 589,17 |
| Operaciones sin conversión (`cantidad = 0`) | 7 de 30 → tasa de conversión 76,67 % |
| Empresas / Personas naturales | 16 / 14 |

---

## Decisiones técnicas destacadas

- **Columnas generadas en Postgres** (`GENERATED ALWAYS AS ... STORED`): las reglas del
  Excel viven en el motor, no en el código de carga. Es imposible insertar una fila con
  un IGV o una ganancia inconsistentes.
- **Event stream particionado** por `RANGE (event_ts)` con 36 particiones mensuales y una
  `DEFAULT` de seguridad (un check avisa si cae algo ahí).
- **Ingesta incremental real**: `stream.fn_eventos_incrementales(watermark)` en Postgres +
  `tarea2_ops.watermark` en Databricks. La segunda corrida no reprocesa nada.
- **Cuarentena, no borrado**: las filas inválidas se conservan con el motivo del rechazo.
- **Idempotencia** en todos los pasos: `TRUNCATE`+`INSERT` y `MERGE` por clave natural;
  cualquier script se puede reejecutar sin duplicar.
- **Doble modo de ingesta** (`jdbc` | `volume_files`) porque Databricks SaaS no alcanza un
  Postgres en `localhost`: el camino por Parquet + Volume funciona siempre, incluido Free Edition.
- **Hash SHA-256 por fila** en `raw.ventas_excel` para detectar cambios entre recargas.
- **`%run ./00_config_setup`** como único punto de configuración: sin credenciales ni
  rutas repartidas por los notebooks; el password de JDBC sale de un *secret scope*.

---

## Requisitos

- Docker Desktop
- Python 3.11+ con `pip install -r requirements.txt`
- Databricks Runtime 13.3 LTS+ (ML recomendado para el notebook 05)
