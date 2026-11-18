# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - GOLD: marts de negocio
# MAGIC
# MAGIC **Contrato de la capa Gold**
# MAGIC
# MAGIC | Regla | Implementacion |
# MAGIC |---|---|
# MAGIC | Orientada al consumo | tablas desnormalizadas, listas para BI, sin joins del lado del usuario |
# MAGIC | Trazabilidad al origen | los 8 reportes del Excel se reproducen y se validan contra su baseline |
# MAGIC | Grano declarado | cada tabla documenta su grano en el `COMMENT` |
# MAGIC | Reproducibilidad | `overwrite` completo: la Gold siempre se puede reconstruir desde Silver |
# MAGIC
# MAGIC **Contenido**
# MAGIC 1. `gold_ventas_excel` + reportes 1 a 8 (equivalente SQL del Excel, validado).
# MAGIC 2. Marts del e-commerce: KPIs mensuales, sucursal, categoria, vendedor, canal.
# MAGIC 3. Funnel de conversion y cohortes de retencion.
# MAGIC 4. `gold_cliente_360`: vista unica de cliente con RFM (insumo del modelo de churn).

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import Window, functions as F

NOTEBOOK = "03_gold_marts"
checks: list[bool] = []
DEC = "decimal(18,4)"

# Se eliminan las columnas que existirian dos veces tras el join
# (_silver_ts en todas las tablas y costo_unitario_pen en fact + dim_producto):
# de lo contrario cualquier referencia a ellas seria ambigua.
vd = (spark.table(t_silver("fact_venta"))
      .join(spark.table(t_silver("dim_cliente")).drop("_silver_ts"), "cliente_sk")
      .join(spark.table(t_silver("dim_producto")).drop("_silver_ts", "costo_unitario_pen"),
            "producto_sk")
      .join(spark.table(t_silver("dim_sucursal")).drop("_silver_ts"), "sucursal_sk")
      .join(spark.table(t_silver("dim_vendedor")).drop("_silver_ts", "sucursal_sk"), "vendedor_sk")
      .join(spark.table(t_silver("dim_metodo_pago")).drop("_silver_ts"), "metodo_pago_sk"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mart plano de las ventas del Excel

# COMMAND ----------

inicio = datetime.now(timezone.utc)
ventas_excel = vd.select(
    "venta_sk", "fila_excel", "nombre_cliente", "tipo_cliente", "pais", "ciudad",
    "nombre_producto", "categoria", "rango_precio", "sucursal", "departamento",
    "vendedor", "metodo_pago", "instrumento", "es_digital",
    "cantidad", "precio_unitario_pen", "subtotal_pen", "igv_pen", "utilidad_pen",
    "costo_unitario_pen", "costo_total_pen", "total_venta_pen", "total_venta_usd",
    "ganancia_pen", "estado", "es_conversion")
escribir_delta(ventas_excel, t_gold("ventas_excel"), "overwrite",
               comentario="Grano: 1 operacion registrada en el Excel (30 filas). Equivalente a la hoja Base de datos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reportes 1 a 8: equivalencia exacta con el Excel

# COMMAND ----------

# REPORTE 1 - conteos
# Cada rama debe exponer exactamente las columnas (descripcion, resultado):
# unionByName empareja por NOMBRE, y sin alias la columna se llamaria como la
# expresion literal y la union fallaria.
def _linea_rep01(descripcion, df, col_valor="v"):
    return df.select(descripcion.alias("descripcion"),
                     F.col(col_valor).cast("double").alias("resultado"))


rep01 = (
    _linea_rep01(F.lit("Total de registros (CONTAR)"),
                 ventas_excel.agg(F.count("*").alias("v")))
    .unionByName(_linea_rep01(F.lit("Clientes distintos (CONTARA)"),
                 ventas_excel.agg(F.countDistinct("nombre_cliente").alias("v"))))
    .unionByName(_linea_rep01(F.lit("Estado vacio (CONTAR.BLANCO)"),
                 ventas_excel.filter(F.col("estado").isNull()).agg(F.count("*").alias("v"))))
    .unionByName(_linea_rep01(F.lit("Clientes tipo Empresa (CONTAR.SI)"),
                 ventas_excel.filter(F.col("tipo_cliente") == "Empresa").agg(F.count("*").alias("v"))))
    .unionByName(_linea_rep01(F.lit("Clientes tipo Persona natural (CONTAR.SI)"),
                 ventas_excel.filter(F.col("tipo_cliente") == "Persona natural").agg(F.count("*").alias("v"))))
    .unionByName(_linea_rep01(F.concat(F.lit("Ventas en sucursal "), F.col("sucursal")),
                 ventas_excel.groupBy("sucursal").agg(F.count("*").alias("v")))))
escribir_delta(rep01, t_gold("rep01_conteo_general"), "overwrite",
               comentario="Reporte 1 del Excel: CONTAR / CONTARA / CONTAR.BLANCO / CONTAR.SI.")

# REPORTE 2 - subtotal por sucursal
rep02 = (ventas_excel.groupBy("sucursal").agg(
            F.sum("subtotal_pen").cast(DEC).alias("total_subtotal_pen"),
            F.sum("igv_pen").cast(DEC).alias("igv_pen"),
            F.sum("total_venta_pen").cast(DEC).alias("total_con_igv_pen"),
            F.sum("ganancia_pen").cast(DEC).alias("ganancia_pen"),
            F.count("*").alias("operaciones"))
         .withColumn("ticket_promedio_pen", (F.col("total_subtotal_pen") / F.col("operaciones")).cast(DEC))
         .orderBy(F.col("total_subtotal_pen").desc()))
escribir_delta(rep02, t_gold("rep02_ventas_por_sucursal"), "overwrite",
               comentario="Reporte 2: SUMAR.SI por sucursal. Grano: sucursal.")

# REPORTE 3 - promedio por metodo de pago
rep03 = (ventas_excel.groupBy("metodo_pago", "instrumento", "es_digital").agg(
            F.avg("subtotal_pen").cast(DEC).alias("promedio_subtotal_pen"),
            F.sum("subtotal_pen").cast(DEC).alias("total_subtotal_pen"),
            F.count("*").alias("operaciones"))
         .orderBy(F.col("promedio_subtotal_pen").desc()))
escribir_delta(rep03, t_gold("rep03_promedio_por_metodo_pago"), "overwrite",
               comentario="Reporte 3: PROMEDIO.SI por metodo de pago. Grano: metodo de pago.")

# REPORTE 4 - conteo por categoria
rep04 = (ventas_excel.groupBy("categoria").agg(
            F.count("*").alias("operaciones"),
            F.sum("cantidad").cast(DEC).alias("unidades"),
            F.sum("subtotal_pen").cast(DEC).alias("subtotal_pen"),
            F.sum("ganancia_pen").cast(DEC).alias("ganancia_pen"))
         .orderBy(F.col("operaciones").desc(), "categoria"))
escribir_delta(rep04, t_gold("rep04_conteo_por_categoria"), "overwrite",
               comentario="Reporte 4: CONTAR.SI por categoria. Grano: categoria.")

# REPORTE 5 y 6 - multicriterio
rep0506 = (ventas_excel.groupBy("tipo_cliente", "sucursal").agg(
              F.count("*").alias("operaciones"),
              F.sum("subtotal_pen").cast(DEC).alias("total_subtotal_pen"),
              F.sum("ganancia_pen").cast(DEC).alias("ganancia_pen"))
           .orderBy("tipo_cliente", "sucursal"))
escribir_delta(rep0506, t_gold("rep0506_tipo_cliente_sucursal"), "overwrite",
               comentario="Reportes 5 y 6: CONTAR.SI.CONJUNTO / SUMAR.SI.CONJUNTO. Grano: tipo de cliente x sucursal.")

# REPORTE 7 - promedio categoria x sucursal
rep07 = (ventas_excel.groupBy("categoria", "sucursal").agg(
            F.avg("subtotal_pen").cast(DEC).alias("promedio_subtotal_pen"),
            F.count("*").alias("operaciones"))
         .orderBy("categoria", "sucursal"))
escribir_delta(rep07, t_gold("rep07_promedio_categoria_sucursal"), "overwrite",
               comentario="Reporte 7: PROMEDIO.SI.CONJUNTO. Grano: categoria x sucursal.")

# REPORTE 8 - min / max
f = spark.table(t_silver("fact_venta"))
rep08 = spark.createDataFrame([
    ("Precio unitario mas alto", float(f.agg(F.max("precio_unitario_pen")).first()[0])),
    ("Precio unitario mas bajo", float(f.agg(F.min("precio_unitario_pen")).first()[0])),
    ("Mayor cantidad vendida",   float(f.agg(F.max("cantidad")).first()[0])),
    ("Menor cantidad vendida",   float(f.agg(F.min("cantidad")).first()[0])),
    ("Subtotal maximo",          float(f.agg(F.max("subtotal_pen")).first()[0])),
    ("Subtotal minimo",          float(f.agg(F.min("subtotal_pen")).first()[0])),
    ("Ganancia maxima",          float(f.agg(F.max("ganancia_pen")).first()[0])),
    ("Ganancia minima",          float(f.agg(F.min("ganancia_pen")).first()[0])),
], "descripcion string, resultado double")
escribir_delta(rep08, t_gold("rep08_min_max"), "overwrite",
               comentario="Reporte 8: MAX / MIN sobre las medidas clave.")
log_paso(NOTEBOOK, "reportes_excel", "gold.rep01..rep08", None, None, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validacion contra el baseline declarado en el Excel
# MAGIC Si estos checks pasan, la migracion Excel -> Postgres -> Bronze -> Silver -> Gold
# MAGIC no altero ningun resultado de negocio.

# COMMAND ----------

baseline_sucursal = {"Puno": 15336.0, "Arequipa": 8885.0, "Juliaca": 12609.0}
obs = {r["sucursal"]: float(r["total_subtotal_pen"]) for r in rep02.collect()}
for suc, esperado in baseline_sucursal.items():
    checks.append(dq("gold", "rep02", f"subtotal_{suc}", "ERROR",
                     obs.get(suc), esperado, abs(obs.get(suc, -1) - esperado) < 0.01))

baseline_cat = {"Equipos tecnológicos": 7, "Mobiliario de oficina": 8, "Útiles de oficina": 7,
                "Accesorios tecnológicos": 5, "Software y gestión": 3}
obs_cat = {r["categoria"]: r["operaciones"] for r in rep04.collect()}
for cat, esperado in baseline_cat.items():
    checks.append(dq("gold", "rep04", f"conteo_{cat}", "ERROR",
                     obs_cat.get(cat), esperado, obs_cat.get(cat) == esperado))

baseline_minmax = {"Precio unitario mas alto": 2790.0, "Precio unitario mas bajo": 18.0,
                   "Mayor cantidad vendida": 10.0, "Menor cantidad vendida": 0.0,
                   "Subtotal maximo": 7350.0, "Subtotal minimo": 0.0,
                   "Ganancia maxima": 2940.0, "Ganancia minima": 0.0}
obs_mm = {r["descripcion"]: r["resultado"] for r in rep08.collect()}
for k, esperado in baseline_minmax.items():
    checks.append(dq("gold", "rep08", k.replace(" ", "_"), "ERROR",
                     obs_mm.get(k), esperado, abs(obs_mm.get(k, -1) - esperado) < 0.01))

total_excel = float(spark.table(t_gold("ventas_excel")).agg(F.sum("subtotal_pen")).first()[0])
checks.append(dq("gold", "ventas_excel", "subtotal_total_36830", "ERROR",
                 total_excel, 36830.0, abs(total_excel - 36830.0) < 0.01))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Marts del e-commerce (event stream)

# COMMAND ----------

inicio = datetime.now(timezone.utc)
ordenes = (spark.table(t_silver("orden")).filter(F.col("es_orden_valida"))
           .join(spark.table(t_silver("dim_cliente")).select(
                 "cliente_sk", "nombre_cliente", "tipo_cliente", "ciudad", "segmento",
                 "es_cliente_real", "fecha_alta"), "cliente_sk")
           .join(spark.table(t_silver("dim_producto")).select(
                 "producto_sk", "nombre_producto", "rango_precio"), "producto_sk")
           .join(spark.table(t_silver("dim_sucursal")).select("sucursal_sk", "sucursal"), "sucursal_sk")
           .join(spark.table(t_silver("dim_vendedor")).select("vendedor_sk", "vendedor"), "vendedor_sk")
           .join(spark.table(t_silver("dim_metodo_pago")).select("metodo_pago_sk", "metodo_pago"), "metodo_pago_sk"))

escribir_delta(ordenes, t_gold("ordenes_detalle"), "overwrite", particiones=["anio_mes"],
               comentario="Grano: 1 orden valida (no cancelada) con todas sus dimensiones desnormalizadas.")

kpi_mensual = (ordenes.groupBy("anio_mes").agg(
        F.countDistinct("order_id").alias("ordenes"),
        F.countDistinct("cliente_sk").alias("clientes_activos"),
        F.sum("monto_pen").cast(DEC).alias("venta_pen"),
        F.sum("monto_con_igv_pen").cast(DEC).alias("venta_con_igv_pen"),
        F.sum("ganancia_estimada_pen").cast(DEC).alias("ganancia_pen"),
        F.avg("monto_pen").cast(DEC).alias("ticket_promedio_pen"),
        F.sum("cantidad").cast(DEC).alias("unidades"),
        F.avg("descuento_pct").cast("decimal(6,4)").alias("descuento_promedio"))
    .withColumn("venta_usd", (F.col("venta_con_igv_pen") / F.lit(PARAMS_NEGOCIO["TIPO_CAMBIO_USD"])).cast(DEC))
    .withColumn("venta_mes_anterior", F.lag("venta_pen").over(Window.orderBy("anio_mes")))
    .withColumn("var_mom_pct", F.round(100 * (F.col("venta_pen") - F.col("venta_mes_anterior"))
                                       / F.col("venta_mes_anterior"), 2))
    .orderBy("anio_mes"))
escribir_delta(kpi_mensual, t_gold("kpi_mensual"), "overwrite",
               comentario="Grano: mes. KPIs comerciales con variacion mes a mes.")

for dim, nombre in [("sucursal", "ventas_por_sucursal"), ("categoria", "ventas_por_categoria"),
                    ("vendedor", "ventas_por_vendedor"), ("canal", "ventas_por_canal"),
                    ("metodo_pago", "ventas_por_metodo_pago"), ("tipo_cliente", "ventas_por_tipo_cliente")]:
    mart = (ordenes.groupBy(dim).agg(
                F.countDistinct("order_id").alias("ordenes"),
                F.countDistinct("cliente_sk").alias("clientes"),
                F.sum("monto_pen").cast(DEC).alias("venta_pen"),
                F.sum("ganancia_estimada_pen").cast(DEC).alias("ganancia_pen"),
                F.avg("monto_pen").cast(DEC).alias("ticket_promedio_pen"))
            .withColumn("part_venta_pct", F.round(100 * F.col("venta_pen")
                        / F.sum("venta_pen").over(Window.partitionBy()), 2))
            .orderBy(F.col("venta_pen").desc()))
    escribir_delta(mart, t_gold(nombre), "overwrite", comentario=f"Grano: {dim}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funnel de conversion y cohortes de retencion

# COMMAND ----------

ev = spark.table(t_silver("evento"))
etapas = [("1_sesiones", "session_start"), ("2_vista_producto", "product_view"),
          ("3_carrito", "add_to_cart"), ("4_checkout", "checkout_start"),
          ("5_orden", "order_placed")]
funnel_rows = []
base = None
for etiqueta, tipo in etapas:
    n = ev.filter(F.col("event_type") == tipo).select("session_id").distinct().count()
    base = base or n
    funnel_rows.append((etiqueta, tipo, n, round(100.0 * n / max(base, 1), 2)))
funnel = spark.createDataFrame(funnel_rows, "etapa string, event_type string, sesiones long, pct_vs_sesiones double")
# tasa de paso entre etapas consecutivas
funnel = funnel.withColumn("pct_vs_etapa_anterior", F.round(
    100.0 * F.col("sesiones") / F.lag("sesiones").over(Window.orderBy("etapa")), 2))
escribir_delta(funnel, t_gold("funnel_conversion"), "overwrite",
               comentario="Grano: etapa del funnel. Sesiones unicas que alcanzaron cada etapa.")

cohortes = (spark.table(t_silver("orden")).filter(F.col("es_orden_valida"))
    .withColumn("cohorte", F.date_format(F.min("fecha").over(Window.partitionBy("cliente_sk")), "yyyy-MM"))
    .withColumn("mes_actividad", F.date_format("fecha", "yyyy-MM"))
    .withColumn("meses_desde_cohorte",
                F.months_between(F.trunc("fecha", "month"),
                                 F.trunc(F.min("fecha").over(Window.partitionBy("cliente_sk")), "month")).cast("int"))
    .groupBy("cohorte", "meses_desde_cohorte")
    .agg(F.countDistinct("cliente_sk").alias("clientes"),
         F.sum("monto_pen").cast(DEC).alias("venta_pen")))
tam_cohorte = cohortes.filter(F.col("meses_desde_cohorte") == 0).select(
    "cohorte", F.col("clientes").alias("clientes_cohorte"))
cohortes = (cohortes.join(tam_cohorte, "cohorte")
            .withColumn("retencion_pct", F.round(100.0 * F.col("clientes") / F.col("clientes_cohorte"), 2))
            .orderBy("cohorte", "meses_desde_cohorte"))
escribir_delta(cohortes, t_gold("cohortes_retencion"), "overwrite",
               comentario="Grano: cohorte de primera compra x meses transcurridos. Retencion de clientes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `gold_cliente_360`: vista unica de cliente con RFM
# MAGIC Grano: 1 cliente. Es el insumo directo de la ingenieria de features de churn.

# COMMAND ----------

inicio = datetime.now(timezone.utc)
fecha_corte = spark.table(t_silver("evento")).agg(F.max("event_date")).first()[0]
print(f"[gold] fecha de corte del stream: {fecha_corte}")

agg_ord = (spark.table(t_silver("orden")).groupBy("cliente_sk").agg(
    F.countDistinct(F.when(F.col("es_orden_valida"), F.col("order_id"))).alias("ordenes"),
    F.sum(F.when(F.col("es_orden_valida"), F.col("monto_pen")).otherwise(F.lit(0))).cast(DEC).alias("monetario_pen"),
    F.sum(F.when(F.col("es_orden_valida"), F.col("ganancia_estimada_pen")).otherwise(F.lit(0))).cast(DEC).alias("margen_pen"),
    F.avg(F.when(F.col("es_orden_valida"), F.col("monto_pen"))).cast(DEC).alias("ticket_promedio_pen"),
    F.min(F.when(F.col("es_orden_valida"), F.col("fecha"))).alias("primera_compra"),
    F.max(F.when(F.col("es_orden_valida"), F.col("fecha"))).alias("ultima_compra"),
    F.sum(F.when(F.col("fue_cancelada"), 1).otherwise(0)).alias("ordenes_canceladas"),
    F.sum(F.when(F.col("tuvo_devolucion"), 1).otherwise(0)).alias("devoluciones"),
    F.sum("tickets_soporte").alias("tickets_soporte"),
    F.avg("descuento_pct").cast("decimal(6,4)").alias("descuento_promedio"),
    F.countDistinct("categoria").alias("categorias_distintas")))

agg_ev = (spark.table(t_silver("evento")).groupBy("cliente_sk").agg(
    F.count("*").alias("eventos_totales"),
    F.countDistinct("session_id").alias("sesiones"),
    F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("vistas_producto"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("agregados_carrito"),
    F.sum(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0)).alias("pagos_fallidos"),
    F.sum(F.when(F.col("event_type") == "newsletter_unsubscribe", 1).otherwise(0)).alias("desuscripciones"),
    F.max("event_date").alias("ultima_actividad"),
    F.min("event_date").alias("primera_actividad")))

canal_top = (spark.table(t_silver("evento")).groupBy("cliente_sk", "canal").count()
             .withColumn("rn", F.row_number().over(
                 Window.partitionBy("cliente_sk").orderBy(F.col("count").desc(), "canal")))
             .filter(F.col("rn") == 1).select("cliente_sk", F.col("canal").alias("canal_preferido")))

cliente_360 = (spark.table(t_silver("dim_cliente"))
    .join(agg_ord, "cliente_sk", "left").join(agg_ev, "cliente_sk", "left")
    .join(canal_top, "cliente_sk", "left")
    .fillna(0, subset=["ordenes", "ordenes_canceladas", "devoluciones", "tickets_soporte",
                       "eventos_totales", "sesiones", "vistas_producto", "agregados_carrito",
                       "pagos_fallidos", "desuscripciones", "categorias_distintas"])
    .withColumn("recencia_dias", F.datediff(F.lit(fecha_corte), F.col("ultima_compra")))
    .withColumn("dias_sin_actividad", F.datediff(F.lit(fecha_corte), F.col("ultima_actividad")))
    .withColumn("antiguedad_dias", F.datediff(F.lit(fecha_corte), F.col("primera_actividad")))
    .withColumn("tasa_conversion_sesion", F.round(F.col("ordenes") / F.greatest(F.col("sesiones"), F.lit(1)), 4))
    .withColumn("tasa_cancelacion", F.round(F.col("ordenes_canceladas") / F.greatest(F.col("ordenes"), F.lit(1)), 4))
    .withColumn("tasa_devolucion", F.round(F.col("devoluciones") / F.greatest(F.col("ordenes"), F.lit(1)), 4))
    .withColumn("es_cliente_activo", F.col("ordenes") > 0))

# Los quintiles RFM se calculan SOLO sobre clientes con compras (si no, los
# clientes sin ordenes distorsionan los cortes) y luego se reincorporan.
rfm = (cliente_360.filter(F.col("ordenes") > 0).select("cliente_sk", "recencia_dias", "ordenes", "monetario_pen")
       .withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recencia_dias").desc())))
       .withColumn("f_score", F.ntile(5).over(Window.orderBy("ordenes")))
       .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetario_pen")))
       .select("cliente_sk", "r_score", "f_score", "m_score"))

cliente_360 = (cliente_360.join(rfm, "cliente_sk", "left")
    .withColumn("rfm", F.concat_ws("", "r_score", "f_score", "m_score"))
    .withColumn("segmento_rfm",
        F.when(F.col("ordenes") == 0, "sin_compras")
         .when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "campeon")
         .when((F.col("r_score") >= 4) & (F.col("f_score") < 4), "nuevo_o_prometedor")
         .when((F.col("r_score") == 3), "necesita_atencion")
         .when((F.col("r_score") <= 2) & (F.col("m_score") >= 4), "en_riesgo_alto_valor")
         .otherwise("hibernando"))
    .withColumn("_gold_ts", F.current_timestamp()))

escribir_delta(cliente_360, t_gold("cliente_360"), "overwrite",
               comentario="Grano: 1 cliente. RFM, agregados de comportamiento y segmento. Insumo del modelo de churn.")

n360 = spark.table(t_gold("cliente_360")).count()
checks.append(dq("gold", "cliente_360", "clientes_completos", "ERROR", n360,
                 spark.table(t_silver("dim_cliente")).count(),
                 n360 == spark.table(t_silver("dim_cliente")).count()))
log_paso(NOTEBOOK, "cliente_360", t_gold("cliente_360"), None, n360, inicio=inicio)

# COMMAND ----------

display(spark.table(t_gold("kpi_mensual")).orderBy("anio_mes"))
display(spark.table(t_gold("funnel_conversion")).orderBy("etapa"))
display(spark.table(t_gold("cliente_360")).groupBy("segmento_rfm")
        .agg(F.count("*").alias("clientes"),
             F.sum("monetario_pen").cast(DEC).alias("venta_pen"),
             F.avg("recencia_dias").cast("int").alias("recencia_promedio"))
        .orderBy(F.col("venta_pen").desc()))

# COMMAND ----------

display(spark.sql(f"""
    SELECT objeto, nombre_check, valor_obs, valor_esp, resultado
    FROM {t_ops('dq_result')} WHERE batch_id = '{BATCH_ID}' AND capa = 'gold'
    ORDER BY resultado, objeto, nombre_check"""))

exigir(checks, "gold")
log_paso(NOTEBOOK, "fin", "gold", None, None, mensaje="marts de gold completos y validados")
print("[gold] marts completos. Los reportes del Excel coinciden con el baseline.")
