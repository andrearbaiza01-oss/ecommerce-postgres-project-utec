# Diccionario de datos

> Generado automaticamente desde el catalogo de PostgreSQL con
> `python scripts/40_generar_diccionario.py`. No editar a mano.

## Indice de esquemas

| Esquema | Proposito | Objetos |
|---|---|---|
| `raw` | Landing zone. Copia fiel del Excel, sin transformaciones de negocio. | 3 |
| `core` | Modelo dimensional (estrella). Fuente de verdad para analitica. | 8 |
| `stream` | Event stream de e-commerce, append-only y particionado por mes. | 2 |
| `mart` | Vistas de negocio: replican los reportes del Excel y extienden el analisis. | 13 |
| `audit` | Metadatos de ejecucion: cargas, watermarks y checks de calidad. | 5 |

---

## Esquema `raw`

*Landing zone. Copia fiel del Excel, sin transformaciones de negocio.*

### `raw.parametros_excel`  <sub>(tabla)</sub>

Parametros de calculo del Excel (IGV, margen, factor costo, tipo de cambio).

Filas (aprox.): **4**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `clave` | `text` | no |  |
| `valor` | `numeric(18,6)` | no |  |
| `celda` | `text` | no |  |
| `rotulo_excel` | `text` | si |  |
| `descripcion` | `text` | si |  |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `raw.reportes_excel`  <sub>(tabla)</sub>

Resultados declarados en las hojas de reportes del Excel: se usan como baseline de validacion.

Filas (aprox.): **53**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `raw_id` | `bigint` | no | default `nextval('raw.reportes_excel_raw_id_seq'::regclass)` |
| `hoja` | `text` | no |  |
| `reporte` | `text` | si |  |
| `descripcion` | `text` | si |  |
| `funcion` | `text` | si |  |
| `resultado` | `text` | si |  |
| `fila_excel` | `integer` | si |  |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `raw.ventas_excel`  <sub>(tabla)</sub>

Landing 1:1 de la hoja "Base de datos" (30 registros, filas 4..33 del Excel).

Filas (aprox.): **30**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `raw_id` | `bigint` | no | default `nextval('raw.ventas_excel_raw_id_seq'::regclass)` |
| `fila_excel` | `integer` | no |  |
| `nombre_cliente` | `text` | si |  |
| `tipo` | `text` | si |  |
| `pais` | `text` | si |  |
| `ciudad` | `text` | si |  |
| `producto_vendido` | `text` | si |  |
| `categoria_producto` | `text` | si |  |
| `sucursal` | `text` | si |  |
| `vendedor_responsable` | `text` | si |  |
| `cantidad_vendida` | `numeric(18,4)` | si |  |
| `precio_unitario_pen` | `numeric(18,4)` | si |  |
| `metodo_pago` | `text` | si |  |
| `subtotal` | `numeric(18,4)` | si |  |
| `igv_18` | `numeric(18,4)` | si |  |
| `utilidad` | `numeric(18,4)` | si |  |
| `costo_unitario` | `numeric(18,4)` | si |  |
| `costo_total` | `numeric(18,4)` | si |  |
| `total_de_venta` | `numeric(18,4)` | si |  |
| `venta_total_usd` | `numeric(18,6)` | si |  |
| `ganancia` | `numeric(18,4)` | si |  |
| `estado` | `text` | si |  |
| `archivo_origen` | `text` | no |  |
| `hoja_origen` | `text` | no |  |
| `hash_fila` | `character(64)` | no | SHA-256 del contenido de la fila: permite detectar cambios en recargas. |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

---

## Esquema `core`

*Modelo dimensional (estrella). Fuente de verdad para analitica.*

### `core.dim_cliente`  <sub>(tabla)</sub>

Dimension conformada de clientes. es_cliente_real=false marca clientes generados para el event stream.

Filas (aprox.): **1,530**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `cliente_sk` | `integer` | no |  |
| `cliente_nk` | `text` | no |  |
| `nombre_cliente` | `text` | no |  |
| `tipo_cliente` | `text` | no |  |
| `pais` | `text` | no | default `'Peru'::text` |
| `ciudad` | `text` | si |  |
| `segmento` | `text` | si |  |
| `origen_registro` | `text` | no | default `'EXCEL'::text`; EXCEL = uno de los 30 clientes reales; STREAM_SINTETICO = cliente generado para la simulacion del stream. |
| `es_cliente_real` | `boolean` | no | default `true` |
| `fecha_alta` | `date` | si |  |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.dim_fecha`  <sub>(tabla)</sub>

Filas (aprox.): **1,461**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `fecha_sk` | `integer` | no |  |
| `fecha` | `date` | no |  |
| `anio` | `smallint` | no |  |
| `trimestre` | `smallint` | no |  |
| `mes` | `smallint` | no |  |
| `nombre_mes` | `text` | no |  |
| `semana_iso` | `smallint` | no |  |
| `dia` | `smallint` | no |  |
| `dia_semana` | `smallint` | no |  |
| `nombre_dia` | `text` | no |  |
| `es_fin_semana` | `boolean` | no |  |
| `anio_mes` | `text` | no |  |
| `primer_dia_mes` | `date` | no |  |
| `ultimo_dia_mes` | `date` | no |  |

### `core.dim_metodo_pago`  <sub>(tabla)</sub>

Metodo de pago (Yape, Transferencia, Deposito, Tarjeta, Efectivo).

Filas (aprox.): **5**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `metodo_pago_sk` | `integer` | no |  |
| `metodo_pago_nk` | `text` | no |  |
| `nombre` | `text` | no |  |
| `instrumento` | `text` | no |  |
| `es_digital` | `boolean` | no | default `false` |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.dim_producto`  <sub>(tabla)</sub>

Catalogo de productos con precio de lista y costo derivado (factor 0.60).

Filas (aprox.): **19**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `producto_sk` | `integer` | no |  |
| `producto_nk` | `text` | no |  |
| `nombre_producto` | `text` | no |  |
| `categoria` | `text` | no |  |
| `precio_lista_pen` | `numeric(18,4)` | no |  |
| `costo_unitario_pen` | `numeric(18,4)` | si | **generada**: (precio_lista_pen * 0.60) |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.dim_sucursal`  <sub>(tabla)</sub>

Sucursales de atencion (Puno, Arequipa, Juliaca).

Filas (aprox.): **3**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `sucursal_sk` | `integer` | no |  |
| `sucursal_nk` | `text` | no |  |
| `nombre` | `text` | no |  |
| `departamento` | `text` | si |  |
| `macro_region` | `text` | si |  |
| `activa` | `boolean` | no | default `true` |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.dim_vendedor`  <sub>(tabla)</sub>

Vendedor responsable de la operacion.

Filas (aprox.): **6**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `vendedor_sk` | `integer` | no |  |
| `vendedor_nk` | `text` | no |  |
| `nombre` | `text` | no |  |
| `sucursal_sk` | `integer` | si |  |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.fact_venta`  <sub>(tabla)</sub>

Hecho transaccional de ventas (30 filas del Excel). Medidas derivadas garantizadas por columnas generadas.

Filas (aprox.): **30**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `venta_sk` | `bigint` | no | default `nextval('core.fact_venta_venta_sk_seq'::regclass)` |
| `fila_excel` | `integer` | no |  |
| `cliente_sk` | `integer` | no |  |
| `producto_sk` | `integer` | no |  |
| `sucursal_sk` | `integer` | no |  |
| `vendedor_sk` | `integer` | no |  |
| `metodo_pago_sk` | `integer` | no |  |
| `cantidad` | `numeric(18,4)` | no |  |
| `precio_unitario_pen` | `numeric(18,4)` | no |  |
| `subtotal_pen` | `numeric(18,4)` | si | **generada**: (cantidad * precio_unitario_pen) |
| `igv_pen` | `numeric(18,4)` | si | **generada**: ((cantidad * precio_unitario_pen) * 0.18) |
| `utilidad_pen` | `numeric(18,4)` | si | **generada**: ((cantidad * precio_unitario_pen) * 0.25) |
| `costo_unitario_pen` | `numeric(18,4)` | si | **generada**: (precio_unitario_pen * 0.60) |
| `costo_total_pen` | `numeric(18,4)` | si | **generada**: ((cantidad * precio_unitario_pen) * 0.60) |
| `total_venta_pen` | `numeric(18,4)` | si | **generada**: ((cantidad * precio_unitario_pen) * 1.18) |
| `total_venta_usd` | `numeric(18,6)` | si | **generada**: (((cantidad * precio_unitario_pen) * 1.18) / 3.75) |
| `ganancia_pen` | `numeric(18,4)` | si | **generada**: ((cantidad * precio_unitario_pen) * 0.40) |
| `estado` | `text` | si | **generada**: 
CASE
    WHEN (((cantidad * precio_unitario_pen) * 0.40) > (0)::numeric) THEN 'Ganancia'::text
    ELSE 'Perdida'::text
END |
| `es_conversion` | `boolean` | si | **generada**: (cantidad > (0)::numeric); FALSE cuando cantidad=0: operacion registrada sin conversion (ESTADO=Perdida en el Excel). |
| `hash_fila` | `character(64)` | no |  |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `core.parametro_negocio`  <sub>(tabla)</sub>

Filas (aprox.): **4**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `clave` | `text` | no |  |
| `valor` | `numeric(18,6)` | no |  |
| `unidad` | `text` | si |  |
| `descripcion` | `text` | si |  |
| `vigente_desde` | `date` | no | default `'2025-01-01'::date` |
| `vigente_hasta` | `date` | si |  |

---

## Esquema `stream`

*Event stream de e-commerce, append-only y particionado por mes.*

### `stream.evento`  <sub>(tabla particionada)</sub>

Event stream de e-commerce, append-only, particionado mensualmente por event_ts. Catalogos reales (Excel) + temporalidad simulada reproducible.

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `event_id` | `bigint` | no |  |
| `event_uuid` | `uuid` | no | default `gen_random_uuid()` |
| `event_ts` | `timestamp with time zone` | no |  |
| `event_date` | `date` | no |  |
| `event_type` | `text` | no |  |
| `cliente_sk` | `integer` | no |  |
| `session_id` | `uuid` | no |  |
| `canal` | `text` | no |  |
| `dispositivo` | `text` | si |  |
| `producto_sk` | `integer` | si |  |
| `categoria` | `text` | si |  |
| `cantidad` | `numeric(18,4)` | si |  |
| `precio_unitario_pen` | `numeric(18,4)` | si |  |
| `monto_pen` | `numeric(18,4)` | si |  |
| `metodo_pago_sk` | `integer` | si |  |
| `sucursal_sk` | `integer` | si |  |
| `vendedor_sk` | `integer` | si |  |
| `order_id` | `uuid` | si |  |
| `campania` | `text` | si |  |
| `utm_source` | `text` | si |  |
| `descuento_pct` | `numeric(6,4)` | si | default `0` |
| `tiempo_sesion_seg` | `integer` | si |  |
| `source_system` | `text` | no | default `'ecommerce_web'::text` |
| `es_sintetico` | `boolean` | no | default `true`; TRUE: la marca de tiempo/sesion fue generada por el simulador reproducible (ver scripts/20_generar_event_stream.py). |
| `ingestion_ts` | `timestamp with time zone` | no | default `now()` |

### `stream.v_orden`  <sub>(vista)</sub>

Ordenes colocadas derivadas del event stream, con flags de cancelacion y devolucion.

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `order_id` | `uuid` | si |  |
| `cliente_sk` | `integer` | si |  |
| `fecha_orden` | `timestamp with time zone` | si |  |
| `event_date` | `date` | si |  |
| `producto_sk` | `integer` | si |  |
| `categoria` | `text` | si |  |
| `cantidad` | `numeric(18,4)` | si |  |
| `precio_unitario_pen` | `numeric(18,4)` | si |  |
| `monto_pen` | `numeric(18,4)` | si |  |
| `igv_pen` | `numeric` | si |  |
| `total_con_igv_pen` | `numeric` | si |  |
| `total_usd` | `numeric` | si |  |
| `ganancia_pen` | `numeric` | si |  |
| `metodo_pago_sk` | `integer` | si |  |
| `sucursal_sk` | `integer` | si |  |
| `vendedor_sk` | `integer` | si |  |
| `canal` | `text` | si |  |
| `campania` | `text` | si |  |
| `fue_cancelada` | `boolean` | si |  |
| `tuvo_devolucion` | `boolean` | si |  |

---

## Esquema `mart`

*Vistas de negocio: replican los reportes del Excel y extienden el analisis.*

### `mart.v_funnel_eventos`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `event_type` | `text` | si |  |
| `eventos` | `bigint` | si |  |
| `sesiones` | `bigint` | si |  |
| `clientes` | `bigint` | si |  |
| `pct_vs_top_etapa` | `numeric` | si |  |

### `mart.v_kpi_global`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `operaciones` | `bigint` | si |  |
| `operaciones_con_venta` | `bigint` | si |  |
| `operaciones_sin_venta` | `bigint` | si |  |
| `tasa_conversion_pct` | `numeric` | si |  |
| `unidades` | `numeric` | si |  |
| `subtotal_pen` | `numeric` | si |  |
| `igv_pen` | `numeric` | si |  |
| `total_venta_pen` | `numeric` | si |  |
| `total_venta_usd` | `numeric` | si |  |
| `ganancia_pen` | `numeric` | si |  |
| `ticket_promedio_pen` | `numeric` | si |  |

### `mart.v_rep01_conteo_general`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `descripcion` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `resultado` | `numeric` | si |  |

### `mart.v_rep02_subtotal_por_sucursal`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `sucursal` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `total_subtotal_pen` | `numeric` | si |  |
| `total_con_igv_pen` | `numeric` | si |  |
| `ganancia_pen` | `numeric` | si |  |
| `operaciones` | `bigint` | si |  |

### `mart.v_rep03_promedio_por_metodo_pago`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `metodo_pago` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `promedio_subtotal_pen` | `numeric` | si |  |
| `total_subtotal_pen` | `numeric` | si |  |
| `operaciones` | `bigint` | si |  |

### `mart.v_rep04_conteo_por_categoria`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `categoria` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `operaciones` | `bigint` | si |  |
| `unidades` | `numeric` | si |  |
| `subtotal_pen` | `numeric` | si |  |

### `mart.v_rep05_conteo_multicriterio`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `tipo_cliente` | `text` | si |  |
| `sucursal` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `operaciones` | `bigint` | si |  |

### `mart.v_rep06_suma_sucursal_tipo`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `tipo_cliente` | `text` | si |  |
| `sucursal` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `total_subtotal_pen` | `numeric` | si |  |
| `ganancia_pen` | `numeric` | si |  |

### `mart.v_rep07_promedio_categoria_sucursal`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `categoria` | `text` | si |  |
| `sucursal` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `promedio_subtotal_pen` | `numeric` | si |  |
| `operaciones` | `bigint` | si |  |

### `mart.v_rep08_min_max`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `descripcion` | `text` | si |  |
| `funcion_excel` | `text` | si |  |
| `resultado` | `numeric` | si |  |

### `mart.v_rfm_cliente`  <sub>(vista)</sub>

RFM por cliente calculado sobre el event stream; insumo de la capa Gold de churn.

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `cliente_sk` | `integer` | si |  |
| `nombre_cliente` | `text` | si |  |
| `tipo_cliente` | `text` | si |  |
| `ciudad` | `text` | si |  |
| `es_cliente_real` | `boolean` | si |  |
| `primera_compra` | `timestamp with time zone` | si |  |
| `ultima_compra` | `timestamp with time zone` | si |  |
| `recencia_dias` | `integer` | si |  |
| `frecuencia` | `bigint` | si |  |
| `monetario_pen` | `numeric` | si |  |
| `ticket_promedio_pen` | `numeric` | si |  |
| `r_score` | `integer` | si |  |
| `f_score` | `integer` | si |  |
| `m_score` | `integer` | si |  |

### `mart.v_ventas_detalle`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `venta_sk` | `bigint` | si |  |
| `fila_excel` | `integer` | si |  |
| `nombre_cliente` | `text` | si |  |
| `tipo_cliente` | `text` | si |  |
| `pais` | `text` | si |  |
| `ciudad` | `text` | si |  |
| `nombre_producto` | `text` | si |  |
| `categoria` | `text` | si |  |
| `sucursal` | `text` | si |  |
| `vendedor` | `text` | si |  |
| `metodo_pago` | `text` | si |  |
| `cantidad` | `numeric(18,4)` | si |  |
| `precio_unitario_pen` | `numeric(18,4)` | si |  |
| `subtotal_pen` | `numeric(18,4)` | si |  |
| `igv_pen` | `numeric(18,4)` | si |  |
| `utilidad_pen` | `numeric(18,4)` | si |  |
| `costo_unitario_pen` | `numeric(18,4)` | si |  |
| `costo_total_pen` | `numeric(18,4)` | si |  |
| `total_venta_pen` | `numeric(18,4)` | si |  |
| `total_venta_usd` | `numeric(18,6)` | si |  |
| `ganancia_pen` | `numeric(18,4)` | si |  |
| `estado` | `text` | si |  |
| `es_conversion` | `boolean` | si |  |

### `mart.v_ventas_mensuales_stream`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `anio_mes` | `text` | si |  |
| `ordenes` | `bigint` | si |  |
| `clientes_activos` | `bigint` | si |  |
| `venta_pen` | `numeric` | si |  |
| `ganancia_pen` | `numeric` | si |  |
| `ticket_promedio_pen` | `numeric` | si |  |

---

## Esquema `audit`

*Metadatos de ejecucion: cargas, watermarks y checks de calidad.*

### `audit.carga_log`  <sub>(tabla)</sub>

Bitacora de cargas (ETL Excel -> Postgres, generacion de stream, exports).

Filas (aprox.): **6**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `carga_id` | `bigint` | no | default `nextval('audit.carga_log_carga_id_seq'::regclass)` |
| `proceso` | `text` | no |  |
| `objeto` | `text` | no |  |
| `filas_leidas` | `bigint` | si |  |
| `filas_escritas` | `bigint` | si |  |
| `estado` | `text` | no |  |
| `mensaje` | `text` | si |  |
| `inicio_ts` | `timestamp with time zone` | no | default `now()` |
| `fin_ts` | `timestamp with time zone` | si |  |

### `audit.data_quality_result`  <sub>(tabla)</sub>

Resultado de los checks de calidad ejecutados sobre raw/core/stream.

Filas (aprox.): **136**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `check_id` | `bigint` | no | default `nextval('audit.data_quality_result_check_id_seq'::regclass)` |
| `ejecutado_ts` | `timestamp with time zone` | no | default `now()` |
| `capa` | `text` | no |  |
| `objeto` | `text` | no |  |
| `nombre_check` | `text` | no |  |
| `severidad` | `text` | no |  |
| `valor_obs` | `text` | si |  |
| `valor_esp` | `text` | si |  |
| `resultado` | `text` | no |  |

### `audit.watermark_ingesta`  <sub>(tabla)</sub>

Watermark para ingesta incremental hacia Bronze (Databricks).

Filas (aprox.): **1**

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `objeto_origen` | `text` | no |  |
| `ultimo_valor_ts` | `timestamp with time zone` | si |  |
| `ultimo_valor_id` | `bigint` | si |  |
| `ultima_corrida` | `timestamp with time zone` | no | default `now()` |
| `filas_entregadas` | `bigint` | si | default `0` |

### `audit.v_resumen_check`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `ejecutado_ts` | `timestamp with time zone` | si |  |
| `checks` | `bigint` | si |  |
| `pass` | `bigint` | si |  |
| `fail` | `bigint` | si |  |
| `fail_criticos` | `bigint` | si |  |

### `audit.v_ultimo_check`  <sub>(vista)</sub>

| Columna | Tipo | Nulos | Notas |
|---|---|---|---|
| `check_id` | `bigint` | si |  |
| `ejecutado_ts` | `timestamp with time zone` | si |  |
| `capa` | `text` | si |  |
| `objeto` | `text` | si |  |
| `nombre_check` | `text` | si |  |
| `severidad` | `text` | si |  |
| `valor_obs` | `text` | si |  |
| `valor_esp` | `text` | si |  |
| `resultado` | `text` | si |  |
