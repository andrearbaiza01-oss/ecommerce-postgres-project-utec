# Arquitectura y decisiones de diseño

Documento de sustento: **qué** se construyó, **por qué** así y **qué alternativas**
se descartaron.

---

## 1. Diagnóstico del origen

El Excel `base_datos_clientes_ventas_administracion_30_registros (1) (1)marielflor.xlsx`
tiene 4 hojas:

| Hoja | Contenido | Uso en la plataforma |
|---|---|---|
| `Base de datos` | 30 registros (filas 4–33), 20 columnas, fila 34 = `TOTALES` | `raw.ventas_excel` → `core.fact_venta` |
| `Reportes Básicos` | reportes 1–4 (CONTAR, SUMAR.SI, PROMEDIO.SI) | baseline de validación + vistas `mart.v_rep01..04` |
| `Reportes Avanzados` | reportes 5–8 (…SI.CONJUNTO, MAX/MIN) | baseline + `mart.v_rep05..08` |
| `Buscador BUSCARV` | buscador por nombre de cliente | `mart.fn_buscar_cliente(text)` |

### Hallazgos relevantes

1. **No hay dimensión temporal.** Ninguna columna de fecha. Esto bloquea por completo
   cualquier análisis de recencia, cohortes, tendencias o churn.
2. **Grano de una sola compra por cliente.** Los 30 clientes son distintos: no hay
   historia de compra que modelar.
3. **Rótulos de parámetros desalineados.** `N1='Margen Util.'`, `O1='Factor Costo'`,
   `P1='TC (S/. a $)'`, pero las fórmulas usan `M2=0.18` (IGV), `N2=0.25` (margen),
   `O2=0.60` (factor costo), `P2=3.75` (TC). El rótulo de `M2` falta y los otros tres
   están corridos una celda. **El ETL lee por celda, nunca por rótulo.**
4. **`ESTADO = 'Pérdida'` no significa pérdida contable**, sino `cantidad = 0`:
   7 de 30 operaciones registradas no se concretaron (ganancia 0). Son *no conversiones*,
   y así se modelan (`core.fact_venta.es_conversion`).
5. Columnas calculadas con fórmulas que dependen de celdas absolutas — se trasladaron a
   columnas generadas en la base para que la regla viva en el motor.

---

## 2. Por qué hay un event stream derivado

La consigna pide **arquitectura medallón con ingesta incremental** y un **modelo de churn**.
Ambas cosas exigen lo que el Excel no tiene:

| Requisito | Necesita | ¿Lo tiene el Excel? |
|---|---|---|
| Ingesta incremental | marca de tiempo o secuencia creciente | ❌ |
| Bronze append-only con watermark | eventos que lleguen en el tiempo | ❌ |
| Etiqueta de churn | ventana de observación + ventana futura | ❌ |
| Features de recencia/frecuencia/tendencia | ≥ 2 compras por cliente con fecha | ❌ |
| Entrenamiento y validación | cientos de filas, no 30 | ❌ |

### Alternativas evaluadas

| Alternativa | Por qué se descartó |
|---|---|
| Entrenar churn con las 30 filas | 30 observaciones y 1 compra por cliente: cualquier métrica sería ruido. Entregarlo sería deshonesto |
| Definir churn como `ESTADO = 'Pérdida'` | no es churn, es una no conversión puntual; no hay dimensión temporal. Serviría como clasificación de conversión, no de abandono |
| Omitir el modelo de churn | incumple el pedido explícito |
| Buscar un dataset público de e-commerce | abandonaría los datos del curso: las dimensiones, precios y reportes del Excel dejarían de usarse |
| **Event stream derivado de los catálogos reales** ✅ | conserva el 100 % de la data real del Excel, habilita el pipeline completo y es reemplazable en un paso |

### Reglas que hacen defendible la simulación

1. **Separación física y declarada**: el esquema `stream` tiene un comentario SQL que
   dice exactamente qué es real y qué es simulado; `stream.evento.es_sintetico` y
   `core.dim_cliente.es_cliente_real` lo marcan fila por fila.
2. **Los catálogos no se inventan**: productos, precios, categorías, sucursales,
   vendedores, métodos de pago, ciudades y el mix Empresa/Persona natural se **leen de
   `core.*`**, es decir del Excel.
3. **Reproducible**: `numpy.random.default_rng(STREAM_SEED)` y UUIDs derivados del mismo
   generador. Dos corridas producen datos idénticos.
4. **Sin etiqueta plantada**: el abandono es un proceso latente (riesgo mensual tipo
   geométrico, con deterioro de frecuencia y aumento de fricción en los 3 meses previos).
   **En la base no existe ninguna columna de churn.** La etiqueta se deriva en Gold por
   inactividad observada, como en producción.
5. **Reemplazable**: sustituir `scripts/20_generar_event_stream.py` por la carga del
   stream real no obliga a cambiar ni una línea de Bronze, Silver, Gold o del modelo.

---

## 3. Capa de persistencia: PostgreSQL

### Esquemas

| Esquema | Rol | Motivo de la separación |
|---|---|---|
| `raw` | landing 1:1 | permite auditar contra el archivo original y reprocesar sin volver al Excel |
| `core` | estrella conformada | la analítica no debe depender de la forma del archivo fuente |
| `stream` | event stream | patrón de carga distinto (append-only, alto volumen, particionado) |
| `mart` | vistas de negocio | desacopla el consumo del modelo físico |
| `audit` | control | la evidencia de calidad y linaje es un entregable, no un log perdido |

### Columnas generadas en `core.fact_venta`

Las 9 medidas derivadas son `GENERATED ALWAYS AS (...) STORED`. Postgres **no permite**
que una columna generada referencie otra generada, así que cada expresión se escribe en
función de las columnas base (`cantidad`, `precio_unitario_pen`).

Ventaja: es imposible insertar una fila con IGV, costo o ganancia inconsistentes, incluso
si alguien escribe por `psql` saltándose el ETL. Alternativa descartada: triggers
(más código, más difícil de auditar) o calcular en el ETL (la regla se escapa del motor).

Costo asumido: los parámetros quedan como literales (`0.18`, `0.25`, `0.60`, `3.75`) en la
definición. Por eso existe además `core.parametro_negocio`, versionado por fecha de
vigencia: si cambia el TC o el IGV, se registra ahí y la capa Silver recalcula con el
parámetro vigente. Cambiar la columna generada exigiría un `ALTER TABLE` deliberado —
que es justamente la fricción que se busca para una regla fiscal.

### Particionamiento del event stream

`PARTITION BY RANGE (event_ts)`, 36 particiones mensuales (2025-01 → 2027-12) más una
`DEFAULT`. La clave primaria es `(event_id, event_ts)` porque Postgres exige que la PK
incluya la clave de partición.

Se eligió rango mensual (no diario) para no generar cientos de particiones con pocas
filas. Un check de calidad verifica que `stream.evento_pdefault` esté vacía: si algo cae
ahí, falta crear la partición del período.

### Ingesta incremental

`stream.fn_eventos_incrementales(p_desde_ts, p_limite)` devuelve solo los eventos con
`ingestion_ts` posterior al watermark. Se usa `ingestion_ts` (cuándo llegó el dato) y no
`event_ts` (cuándo ocurrió) porque un evento puede llegar tarde: filtrar por `event_ts`
perdería los eventos retrasados para siempre.

---

## 4. Capas medallón en Databricks

### Bronze — fidelidad y linaje

| Regla | Implementación |
|---|---|
| Sin transformaciones de negocio | solo se agregan columnas de metadatos |
| Linaje obligatorio | `_ingest_ts`, `_batch_id`, `_source_system`, `_source_object`, `_ingest_mode` |
| Dimensiones | `overwrite` (snapshot completo: son chicas y cambian poco) |
| Event stream | `append` + watermark en `tarea2_ops.watermark` |
| Reproducibilidad | `recarga_completa=true` descarta el watermark y reingesta todo |
| Validación | conteos contra `_manifiesto.json`, el contrato del export |

Se particiona `stream_evento` por `anio_mes` (18 valores) y no por `event_date`
(≈ 550 valores): con 125 k filas, partir por día produciría archivos de ~200 filas —
el problema clásico de *small files*.

### Silver — el dato se vuelve confiable

Lo central: **nada se descarta en silencio**. Las filas que violan una regla van a
`silver.evento_rechazos` con `_motivo_rechazo`. Un check verifica la identidad
`bronze = silver + rechazos`: si no cuadra, se perdieron filas en el camino.

Reglas de dominio: `event_type` ∈ 13 valores, `canal` ∈ 5 valores, `order_placed` debe
tener `order_id` y monto > 0, cantidades no negativas, integridad referencial contra
`dim_cliente`.

Deduplicación por `event_uuid` con ventana `row_number()` conservando el `_ingest_ts` más
reciente: si un lote se reingesta, gana la versión más nueva.

Dimensiones con `MERGE` (SCD tipo 1). Se eligió tipo 1 sobre tipo 2 porque las
dimensiones de este dominio no tienen atributos con valor histórico (no hay cambios de
precio ni reasignaciones de sucursal en la fuente). Si en el futuro importara, el punto de
cambio está aislado en la función `merge_dim`.

Las fórmulas del Excel se **recalculan en PySpark** y se comparan contra los valores que
trajo el origen: un desvío > 0.01 corta el pipeline. Es la tercera verificación
independiente de la misma regla (Excel → Postgres → Spark).

### Gold — consumo y modelo

Dos familias de tablas:

1. **Trazabilidad del Excel**: `rep01` a `rep08` reproducen los 8 reportes y se validan
   contra el baseline (`Puno 15 336`, `Arequipa 8 885`, `Juliaca 12 609`, totales,
   mín/máx). Si estos checks pasan, la cadena completa no alteró ningún número de negocio.
2. **Analítica del e-commerce**: `kpi_mensual`, `ventas_por_*`, `funnel_conversion`,
   `cohortes_retencion`, `ordenes_detalle` y `cliente_360` (RFM).

Cada tabla declara su **grano** en el `COMMENT`. Toda la capa se reconstruye con
`overwrite` desde Silver: la Gold es siempre derivable, nunca es fuente de verdad.

---

## 5. Modelo de churn

### Definición y ventanas

Negocio **no contractual** → churn por inactividad:

> cliente activo antes de `T0` que **no coloca ninguna orden válida en (T0, T0+90]**.

```
   ventana de observación (features)       ventana de etiqueta
  ├───────────────────────────────┤T0├──────────────────────────┤ T0+90
```

Parámetros: `CHURN_DIAS_INACTIVIDAD = 90` (≈ 3× el intervalo típico entre compras de la
base) y elegibilidad `[T0-180, T0]` con al menos una orden válida.

### Por qué 6 snapshots mensuales

Un solo corte daría ~635 filas. Seis cortes mensuales dan **3 284 filas** y, sobre todo,
permiten **validación out-of-time**: se entrena con los 4 cortes antiguos y se evalúa con
los 2 más recientes.

Un `train_test_split` aleatorio sería un error metodológico aquí: el mismo cliente
aparece en varios snapshots, así que filas del mismo cliente caerían en train y test
inflando las métricas. El split por `fecha_snapshot` lo evita y además responde la
pregunta real: *¿el modelo entrenado con el pasado funciona sobre el futuro?*

### Prevención de fuga de información

- Todas las features se calculan con `event_date <= T0`. Sin excepción.
- La etiqueta mira exclusivamente `(T0, T0+90]`.
- El último corte entrenable es `trunc(fecha_max - 90 días)`: sin 90 días completos de
  ventana futura, la etiqueta sería falsamente "churn".
- Check `sin_leakage_recencia`: ninguna recencia puede ser negativa (lo sería si una
  feature hubiera visto el futuro).
- El `CrossValidator` corre **solo sobre train**.

### Variables destacadas

`ratio_recencia_intervalo` = `recencia_dias / intervalo_medio_dias`. Normaliza la
recencia por el ritmo propio de cada cliente: 40 días sin comprar es alarmante para quien
compra cada 15 días e irrelevante para quien compra cada 6 meses. Es el predictor clásico
en churn no contractual.

Además: tendencias a 90 días vs los 90 previos (órdenes, monto, sesiones), señales de
fricción (tickets, devoluciones, cancelaciones, pagos fallidos, desuscripciones) y
engagement (abandono de carrito, vistas por sesión, conversión).

### Modelos y métricas

`LogisticRegression` como línea base interpretable y `GBTClassifier` para capturar no
linealidades; se promueve el de mejor AUC en test. Desbalance (37 % churn) tratado con
`weightCol`.

Resultado de la validación local de la metodología (mismo diseño, `sklearn`):
**AUC 0,835 (LR) y 0,850 (GBT)** out-of-time; PR-AUC 0,692 / 0,758. El notebook corta si
el AUC en test baja de 0,70.

Se reportan ROC-AUC, PR-AUC (relevante con clases desbalanceadas), KS, precisión,
recall, F1, matriz de confusión y **tabla de lift por decil** — esta última es la que
usa el área comercial: cuántos churners se capturan contactando al 10 %, 20 %, 30 % de
la base.

### De la probabilidad a la acción

`gold.churn_scores` no entrega solo un score. Añade:

- **prioridad P1–P4**, cruzando riesgo con valor del cliente (no es lo mismo perder a un
  cliente de S/. 50/mes que a uno de S/. 2 000/mes);
- **ingreso en riesgo** = `prob × venta mensual × 3 meses`, para dimensionar el problema
  en soles y poder comparar contra el costo de la campaña de retención;
- **motivo principal** legible (p. ej. *"Lleva más del doble de su ciclo habitual sin
  comprar"*), porque un número sin explicación no se acciona;
- **acción sugerida** diferenciada por B2B/B2C y por tipo de fricción detectada.

---

## 6. Observabilidad

| Tabla | Para qué |
|---|---|
| `audit.carga_log` (PG) / `tarea2_ops.pipeline_log` (DBX) | qué corrió, cuántas filas, cuánto tardó |
| `audit.data_quality_result` / `tarea2_ops.dq_result` | resultado de cada check, por capa y severidad |
| `audit.watermark_ingesta` / `tarea2_ops.watermark` | hasta dónde se ingirió |
| `gold.churn_monitoreo` | distribución de scores y métricas por corrida → drift |
| `DESCRIBE HISTORY` (Delta) | time travel y auditoría de cada escritura |

Severidad `ERROR` corta el pipeline (`AssertionError`); `WARN` queda registrada sin
detener. El criterio: un dato incompleto es mejor que un dato incorrecto propagado a la
capa siguiente.

---

## 7. Limitaciones conocidas

1. **El event stream es simulado en su temporalidad.** Las métricas del modelo miden que
   la metodología es correcta y que el pipeline funciona de punta a punta; **no** son una
   estimación del churn real del negocio. Con data real, el número cambia; el código, no.
2. **Las 30 filas reales no alcanzan para modelar.** Se usan para lo que sí sirven:
   catálogos, precios, reglas de cálculo y validación de los reportes.
3. **SCD tipo 1**: no se conserva el histórico de cambios en dimensiones.
4. **`modo_ingesta=jdbc` requiere exponer el Postgres.** Databricks SaaS no alcanza
   `localhost`; el camino por Parquet + Volume es el recomendado y funciona siempre.
5. **Las credenciales de `.env` son de desarrollo local.** Para cualquier entorno
   compartido deben rotarse y moverse a un gestor de secretos.
