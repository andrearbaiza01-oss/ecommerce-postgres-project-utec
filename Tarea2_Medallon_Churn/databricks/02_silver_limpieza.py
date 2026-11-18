# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - SILVER: limpieza, conformacion y modelo de negocio
# MAGIC
# MAGIC **Contrato de la capa Silver**
# MAGIC
# MAGIC | Regla | Implementacion |
# MAGIC |---|---|
# MAGIC | Tipado explicito | casts a `decimal(18,4)`, `timestamp`, `date`, `int` |
# MAGIC | Deduplicacion | ventana por clave natural, se conserva el `_ingest_ts` mas reciente |
# MAGIC | Normalizacion de texto | `trim`, colapso de espacios, claves sin tildes en mayusculas |
# MAGIC | Validacion de dominio | enumerados de `event_type`, `canal`, `tipo_cliente`, `metodo_pago` |
# MAGIC | Cuarentena | filas invalidas -> `*_rechazos` con el motivo; **no se descartan en silencio** |
# MAGIC | Dimensiones conformadas | `MERGE` idempotente (SCD tipo 1) |
# MAGIC | Hechos | `silver_fact_venta` (Excel) y `silver_orden` (derivada del event stream) |
# MAGIC | Reglas del Excel | se recalculan y se comparan contra el origen (tolerancia 0.01) |

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import Window, functions as F

NOTEBOOK = "02_silver_limpieza"
checks: list[bool] = []
DEC = "decimal(18,4)"

EVENT_TYPES = ["session_start", "product_view", "search", "add_to_cart", "remove_from_cart",
               "checkout_start", "order_placed", "order_cancelled", "payment_failed",
               "return_requested", "support_ticket", "review_submitted",
               "newsletter_unsubscribe"]
CANALES = ["web", "app_movil", "marketplace", "whatsapp", "tienda"]


def limpiar_texto(col):
    """trim + colapso de espacios internos; cadena vacia -> NULL."""
    c = F.regexp_replace(F.trim(F.col(col) if isinstance(col, str) else col), r"\s+", " ")
    return F.when(c == "", None).otherwise(c)


def clave_sin_tildes(col):
    return F.upper(F.translate(col, "áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN"))


def deduplicar(df, claves: list[str], orden: str = "_ingest_ts"):
    w = Window.partitionBy(*claves).orderBy(F.col(orden).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))


def merge_dim(df, tabla: str, clave: str, comentario: str) -> int:
    """SCD tipo 1 idempotente mediante MERGE."""
    vista = f"stg_{tabla.split('.')[-1]}"
    df.createOrReplaceTempView(vista)
    if not spark.catalog.tableExists(tabla):
        escribir_delta(df, tabla, "overwrite", comentario=comentario)
        return spark.table(tabla).count()
    cols = [c for c in df.columns if c != clave]
    set_clause = ", ".join(f"d.{c} = s.{c}" for c in cols)
    spark.sql(f"""
        MERGE INTO {tabla} d USING {vista} s ON d.{clave} = s.{clave}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT *""")
    n = spark.table(tabla).count()
    print(f"[silver] {tabla} <- MERGE | filas totales: {n:,}")
    return n

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensiones conformadas

# COMMAND ----------

inicio = datetime.now(timezone.utc)

dim_cliente = (spark.table(t_bronze("core_dim_cliente"))
    .select(
        F.col("cliente_sk").cast("int").alias("cliente_sk"),
        clave_sin_tildes(limpiar_texto("cliente_nk")).alias("cliente_nk"),
        limpiar_texto("nombre_cliente").alias("nombre_cliente"),
        limpiar_texto("tipo_cliente").alias("tipo_cliente"),
        limpiar_texto("pais").alias("pais"),
        limpiar_texto("ciudad").alias("ciudad"),
        limpiar_texto("segmento").alias("segmento"),
        limpiar_texto("origen_registro").alias("origen_registro"),
        F.col("es_cliente_real").cast("boolean").alias("es_cliente_real"),
        F.col("fecha_alta").cast("date").alias("fecha_alta"))
    .withColumn("es_b2b", F.col("tipo_cliente") == F.lit("Empresa"))
    .withColumn("_silver_ts", F.current_timestamp()))

cliente_ok = dim_cliente.filter(F.col("tipo_cliente").isin("Empresa", "Persona natural"))
cliente_rechazo = (dim_cliente.join(cliente_ok.select("cliente_sk"), "cliente_sk", "left_anti")
                   .withColumn("_motivo_rechazo", F.lit("tipo_cliente fuera de dominio")))
merge_dim(deduplicar(cliente_ok, ["cliente_sk"], "_silver_ts"),
          t_silver("dim_cliente"), "cliente_sk",
          "Dimension conformada de clientes (SCD1). es_cliente_real marca los 30 del Excel.")
if cliente_rechazo.count():
    escribir_delta(cliente_rechazo, t_silver("dim_cliente_rechazos"), "overwrite")

dim_producto = (spark.table(t_bronze("core_dim_producto"))
    .select(
        F.col("producto_sk").cast("int").alias("producto_sk"),
        limpiar_texto("producto_nk").alias("producto_nk"),
        limpiar_texto("nombre_producto").alias("nombre_producto"),
        limpiar_texto("categoria").alias("categoria"),
        F.col("precio_lista_pen").cast(DEC).alias("precio_lista_pen"),
        F.col("costo_unitario_pen").cast(DEC).alias("costo_unitario_pen"))
    .withColumn("margen_unitario_pen", (F.col("precio_lista_pen") - F.col("costo_unitario_pen")).cast(DEC))
    .withColumn("rango_precio", F.when(F.col("precio_lista_pen") < 100, "bajo")
                                 .when(F.col("precio_lista_pen") < 700, "medio")
                                 .otherwise("alto"))
    .withColumn("_silver_ts", F.current_timestamp()))
merge_dim(deduplicar(dim_producto, ["producto_sk"], "_silver_ts"),
          t_silver("dim_producto"), "producto_sk", "Catalogo de productos conformado.")

dim_sucursal = (spark.table(t_bronze("core_dim_sucursal"))
    .select(F.col("sucursal_sk").cast("int").alias("sucursal_sk"),
            limpiar_texto("sucursal_nk").alias("sucursal_nk"),
            limpiar_texto("nombre").alias("sucursal"),
            limpiar_texto("departamento").alias("departamento"),
            limpiar_texto("macro_region").alias("macro_region"))
    .withColumn("_silver_ts", F.current_timestamp()))
merge_dim(dim_sucursal, t_silver("dim_sucursal"), "sucursal_sk", "Sucursales conformadas.")

dim_vendedor = (spark.table(t_bronze("core_dim_vendedor"))
    .select(F.col("vendedor_sk").cast("int").alias("vendedor_sk"),
            limpiar_texto("vendedor_nk").alias("vendedor_nk"),
            limpiar_texto("nombre").alias("vendedor"),
            F.col("sucursal_sk").cast("int").alias("sucursal_sk"))
    .withColumn("_silver_ts", F.current_timestamp()))
merge_dim(dim_vendedor, t_silver("dim_vendedor"), "vendedor_sk", "Vendedores conformados.")

dim_metodo = (spark.table(t_bronze("core_dim_metodo_pago"))
    .select(F.col("metodo_pago_sk").cast("int").alias("metodo_pago_sk"),
            limpiar_texto("metodo_pago_nk").alias("metodo_pago_nk"),
            limpiar_texto("nombre").alias("metodo_pago"),
            limpiar_texto("instrumento").alias("instrumento"),
            F.col("es_digital").cast("boolean").alias("es_digital"))
    .withColumn("_silver_ts", F.current_timestamp()))
merge_dim(dim_metodo, t_silver("dim_metodo_pago"), "metodo_pago_sk", "Metodos de pago conformados.")

dim_fecha = (spark.table(t_bronze("core_dim_fecha"))
    .select("fecha_sk", "fecha", "anio", "trimestre", "mes", "nombre_mes", "semana_iso",
            "dia", "dia_semana", "nombre_dia", "es_fin_semana", "anio_mes",
            "primer_dia_mes", "ultimo_dia_mes"))
merge_dim(dim_fecha, t_silver("dim_fecha"), "fecha_sk", "Dimension fecha.")

log_paso(NOTEBOOK, "dimensiones", "silver.dim_*", None, None, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hecho de ventas del Excel: recalculo y validacion de las formulas
# MAGIC
# MAGIC Se recalculan las medidas con los parametros de negocio y se comparan contra
# MAGIC los valores que traia el Excel. Cualquier desvio > 0.01 es un FAIL critico.

# COMMAND ----------

inicio = datetime.now(timezone.utc)
igv = PARAMS_NEGOCIO["IGV"]; margen = PARAMS_NEGOCIO["MARGEN_UTILIDAD"]
factor = PARAMS_NEGOCIO["FACTOR_COSTO"]; tc = PARAMS_NEGOCIO["TIPO_CAMBIO_USD"]

fact = (spark.table(t_bronze("core_fact_venta"))
    .select(
        F.col("venta_sk").cast("long").alias("venta_sk"),
        F.col("fila_excel").cast("int").alias("fila_excel"),
        F.col("cliente_sk").cast("int").alias("cliente_sk"),
        F.col("producto_sk").cast("int").alias("producto_sk"),
        F.col("sucursal_sk").cast("int").alias("sucursal_sk"),
        F.col("vendedor_sk").cast("int").alias("vendedor_sk"),
        F.col("metodo_pago_sk").cast("int").alias("metodo_pago_sk"),
        F.col("cantidad").cast(DEC).alias("cantidad"),
        F.col("precio_unitario_pen").cast(DEC).alias("precio_unitario_pen"),
        F.col("subtotal_pen").cast(DEC).alias("subtotal_origen"),
        F.col("ganancia_pen").cast(DEC).alias("ganancia_origen"),
        F.col("total_venta_usd").cast("decimal(18,6)").alias("usd_origen"),
        F.col("estado").alias("estado_origen"))
    .withColumn("subtotal_pen", (F.col("cantidad") * F.col("precio_unitario_pen")).cast(DEC))
    .withColumn("igv_pen", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(igv)).cast(DEC))
    .withColumn("utilidad_pen", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(margen)).cast(DEC))
    .withColumn("costo_unitario_pen", (F.col("precio_unitario_pen") * F.lit(factor)).cast(DEC))
    .withColumn("costo_total_pen", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(factor)).cast(DEC))
    .withColumn("total_venta_pen", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(1 + igv)).cast(DEC))
    .withColumn("total_venta_usd", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(1 + igv) / F.lit(tc)).cast("decimal(18,6)"))
    .withColumn("ganancia_pen", (F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(1 - factor)).cast(DEC))
    .withColumn("estado", F.when(F.col("cantidad") * F.col("precio_unitario_pen") * F.lit(1 - factor) > 0,
                                 F.lit("Ganancia")).otherwise(F.lit("Perdida")))
    .withColumn("es_conversion", F.col("cantidad") > 0)
    .withColumn("_silver_ts", F.current_timestamp()))

desvios = fact.filter(
    (F.abs(F.col("subtotal_pen") - F.col("subtotal_origen")) > 0.01) |
    (F.abs(F.col("ganancia_pen") - F.col("ganancia_origen")) > 0.01) |
    (F.abs(F.col("total_venta_usd") - F.col("usd_origen")) > 0.01) |
    (F.col("estado") != F.col("estado_origen"))).count()
checks.append(dq("silver", "fact_venta", "formulas_excel_reproducidas", "ERROR", desvios, 0, desvios == 0))

escribir_delta(fact.drop("subtotal_origen", "ganancia_origen", "usd_origen", "estado_origen"),
               t_silver("fact_venta"), "overwrite",
               comentario="Hecho de ventas del Excel con medidas recalculadas y validadas.")
checks.append(dq("silver", "fact_venta", "conteo_30", "ERROR",
                 spark.table(t_silver("fact_venta")).count(), 30,
                 spark.table(t_silver("fact_venta")).count() == 30))
log_paso(NOTEBOOK, "fact_venta", t_silver("fact_venta"), 30, 30, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Event stream: limpieza, deduplicacion y cuarentena

# COMMAND ----------

inicio = datetime.now(timezone.utc)
bronce_ev = spark.table(t_bronze("stream_evento"))
n_bronce = bronce_ev.count()

ev = (bronce_ev
    .select(
        F.col("event_uuid").cast("string").alias("event_uuid"),
        F.col("event_ts").cast("timestamp").alias("event_ts"),
        F.col("event_date").cast("date").alias("event_date"),
        limpiar_texto("event_type").alias("event_type"),
        F.col("cliente_sk").cast("int").alias("cliente_sk"),
        F.col("session_id").cast("string").alias("session_id"),
        limpiar_texto("canal").alias("canal"),
        limpiar_texto("dispositivo").alias("dispositivo"),
        F.col("producto_sk").cast("int").alias("producto_sk"),
        limpiar_texto("categoria").alias("categoria"),
        F.col("cantidad").cast(DEC).alias("cantidad"),
        F.col("precio_unitario_pen").cast(DEC).alias("precio_unitario_pen"),
        F.col("monto_pen").cast(DEC).alias("monto_pen"),
        F.col("metodo_pago_sk").cast("int").alias("metodo_pago_sk"),
        F.col("sucursal_sk").cast("int").alias("sucursal_sk"),
        F.col("vendedor_sk").cast("int").alias("vendedor_sk"),
        F.col("order_id").cast("string").alias("order_id"),
        limpiar_texto("campania").alias("campania"),
        limpiar_texto("utm_source").alias("utm_source"),
        F.coalesce(F.col("descuento_pct").cast("decimal(6,4)"), F.lit(0).cast("decimal(6,4)")).alias("descuento_pct"),
        F.col("tiempo_sesion_seg").cast("int").alias("tiempo_sesion_seg"),
        F.col("_ingest_ts"), F.col("_batch_id"))
    .withColumn("anio_mes", F.date_format("event_date", "yyyy-MM"))
    .withColumn("hora", F.hour("event_ts"))
    .withColumn("dia_semana", F.dayofweek("event_ts"))
    .withColumn("es_fin_semana", F.dayofweek("event_ts").isin(1, 7))
    .withColumn("franja_horaria", F.when(F.hour("event_ts") < 12, "manana")
                                   .when(F.hour("event_ts") < 19, "tarde").otherwise("noche"))
    .withColumn("monto_con_igv_pen", (F.col("monto_pen") * F.lit(1 + igv)).cast(DEC))
    .withColumn("ganancia_estimada_pen", (F.col("monto_pen") * F.lit(1 - factor)).cast(DEC)))

# --- Reglas de validacion
regla_invalida = (
      F.col("event_uuid").isNull()
    | F.col("event_ts").isNull()
    | F.col("cliente_sk").isNull()
    | ~F.col("event_type").isin(EVENT_TYPES)
    | ~F.col("canal").isin(CANALES)
    | ((F.col("event_type") == "order_placed") & (F.col("order_id").isNull()))
    | ((F.col("event_type") == "order_placed") & (F.coalesce(F.col("monto_pen"), F.lit(0)) <= 0))
    | (F.coalesce(F.col("cantidad"), F.lit(0)) < 0))

motivo = (F.when(F.col("event_uuid").isNull(), "event_uuid nulo")
           .when(F.col("event_ts").isNull(), "event_ts nulo")
           .when(F.col("cliente_sk").isNull(), "cliente_sk nulo")
           .when(~F.col("event_type").isin(EVENT_TYPES), "event_type fuera de dominio")
           .when(~F.col("canal").isin(CANALES), "canal fuera de dominio")
           .when((F.col("event_type") == "order_placed") & F.col("order_id").isNull(), "orden sin order_id")
           .when((F.col("event_type") == "order_placed") & (F.coalesce(F.col("monto_pen"), F.lit(0)) <= 0), "orden sin monto")
           .otherwise("cantidad negativa"))

rechazos = ev.filter(regla_invalida).withColumn("_motivo_rechazo", motivo)
n_rech = rechazos.count()
escribir_delta(rechazos, t_silver("evento_rechazos"), "overwrite",
               comentario="Cuarentena: eventos que no cumplen las reglas de dominio de Silver.")

# --- Integridad referencial contra las dimensiones
ev_valido = (ev.filter(~regla_invalida)
    .join(spark.table(t_silver("dim_cliente")).select("cliente_sk"), "cliente_sk", "left_semi"))
ev_limpio = deduplicar(ev_valido, ["event_uuid"], "_ingest_ts")

n_limpio = ev_limpio.count()
print(f"[silver] bronce={n_bronce:,} | validos={n_limpio:,} | rechazados={n_rech:,}")

escribir_delta(ev_limpio, t_silver("evento"), "overwrite", particiones=["anio_mes"],
               comentario="Event stream limpio, deduplicado y enriquecido (grano: 1 evento).")

checks.append(dq("silver", "evento", "sin_duplicados", "ERROR",
                 n_limpio - ev_limpio.select("event_uuid").distinct().count(), 0,
                 n_limpio == ev_limpio.select("event_uuid").distinct().count()))
tasa_rech = round(100.0 * n_rech / max(n_bronce, 1), 3)
checks.append(dq("silver", "evento", "tasa_rechazo_menor_1pct", "WARN", f"{tasa_rech}%", "< 1%", tasa_rech < 1.0))
checks.append(dq("silver", "evento", "integridad_cliente", "ERROR",
                 n_bronce - n_rech - n_limpio, 0, (n_bronce - n_rech - n_limpio) == 0))
log_paso(NOTEBOOK, "evento", t_silver("evento"), n_bronce, n_limpio, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Hecho de ordenes (grano = 1 orden) derivado del stream
# MAGIC
# MAGIC Se consolida cada `order_placed` con sus eventos posteriores
# MAGIC (`order_cancelled`, `return_requested`, `support_ticket`, `review_submitted`)
# MAGIC mediante `MERGE`, de modo que la tabla sea reprocesable sin duplicar.

# COMMAND ----------

inicio = datetime.now(timezone.utc)
ev_s = spark.table(t_silver("evento"))

eventos_post = (ev_s.filter(F.col("order_id").isNotNull())
    .groupBy("order_id").agg(
        F.max(F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0)).cast("boolean").alias("fue_cancelada"),
        F.max(F.when(F.col("event_type") == "return_requested", 1).otherwise(0)).cast("boolean").alias("tuvo_devolucion"),
        F.sum(F.when(F.col("event_type") == "support_ticket", 1).otherwise(0)).alias("tickets_soporte"),
        F.max(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0)).cast("boolean").alias("pago_fallido"),
        F.max(F.when(F.col("event_type") == "review_submitted", 1).otherwise(0)).cast("boolean").alias("dejo_review")))

ordenes = (ev_s.filter(F.col("event_type") == "order_placed")
    .select("order_id", "cliente_sk", "producto_sk", "categoria", "sucursal_sk",
            "vendedor_sk", "metodo_pago_sk", "canal", "dispositivo", "campania",
            "utm_source", "descuento_pct", "cantidad", "precio_unitario_pen",
            "monto_pen", "monto_con_igv_pen", "ganancia_estimada_pen",
            F.col("event_ts").alias("fecha_orden"), F.col("event_date").alias("fecha"),
            "anio_mes", "franja_horaria", "es_fin_semana")
    .join(eventos_post, "order_id", "left")
    .fillna({"fue_cancelada": False, "tuvo_devolucion": False, "tickets_soporte": 0,
             "pago_fallido": False, "dejo_review": False})
    .withColumn("es_orden_valida", ~F.col("fue_cancelada"))
    .withColumn("_silver_ts", F.current_timestamp()))

merge_dim(deduplicar(ordenes, ["order_id"], "_silver_ts"), t_silver("orden"), "order_id",
          "Hecho de ordenes derivado del event stream, con flags de cancelacion/devolucion/soporte.")

n_ord = spark.table(t_silver("orden")).count()
checks.append(dq("silver", "orden", "ordenes_presentes", "ERROR", n_ord, "> 0", n_ord > 0))
checks.append(dq("silver", "orden", "montos_positivos", "ERROR",
                 spark.table(t_silver("orden")).filter(F.col("monto_pen") <= 0).count(), 0,
                 spark.table(t_silver("orden")).filter(F.col("monto_pen") <= 0).count() == 0))
log_paso(NOTEBOOK, "orden", t_silver("orden"), n_ord, n_ord, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sesiones (grano = 1 sesion) para analisis de comportamiento

# COMMAND ----------

sesiones = (ev_s.groupBy("session_id", "cliente_sk", "canal", "dispositivo", "anio_mes")
    .agg(F.min("event_ts").alias("inicio_sesion"),
         F.max("event_ts").alias("fin_sesion"),
         F.count("*").alias("eventos"),
         F.countDistinct("producto_sk").alias("productos_vistos"),
         F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("vistas"),
         F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("agregados_carrito"),
         F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias("ordenes"),
         F.sum(F.when(F.col("event_type") == "order_placed", F.col("monto_pen")).otherwise(F.lit(0))).cast(DEC).alias("venta_pen"),
         F.max("campania").alias("campania"), F.max("utm_source").alias("utm_source"))
    .withColumn("duracion_seg", F.unix_timestamp("fin_sesion") - F.unix_timestamp("inicio_sesion"))
    .withColumn("convirtio", F.col("ordenes") > 0)
    .withColumn("fecha", F.to_date("inicio_sesion")))

escribir_delta(sesiones, t_silver("sesion"), "overwrite", particiones=["anio_mes"],
               comentario="Agregado por sesion: eventos, vistas, carrito, conversion y venta.")
log_paso(NOTEBOOK, "sesion", t_silver("sesion"), None, sesiones.count(), inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultado de los checks de Silver

# COMMAND ----------

display(spark.sql(f"""
    SELECT capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado
    FROM {t_ops('dq_result')} WHERE batch_id = '{BATCH_ID}' AND capa = 'silver'
    ORDER BY resultado, severidad, objeto"""))

exigir(checks, "silver")
log_paso(NOTEBOOK, "fin", "silver", None, None, mensaje="capa silver completa")
print("[silver] capa completa y validada")
