# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Calidad de datos y monitoreo del pipeline
# MAGIC
# MAGIC Notebook de cierre: consolida la evidencia de que la plataforma funciona.
# MAGIC
# MAGIC 1. **Reconciliacion end-to-end**: Excel -> Postgres -> Bronze -> Silver -> Gold.
# MAGIC 2. **Resultado de todos los checks** del batch, por capa y severidad.
# MAGIC 3. **Bitacora** de ejecucion y watermarks.
# MAGIC 4. **Salud del modelo** de churn entre corridas (deteccion de drift).
# MAGIC 5. **Historial Delta** (time travel) de las tablas criticas.

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from pyspark.sql import functions as F

NOTEBOOK = "06_calidad_y_monitoreo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reconciliacion de conteos y montos entre capas

# COMMAND ----------

def conteo(tabla):
    try:
        return spark.table(tabla).count()
    except Exception:
        return None


def suma(tabla, col):
    try:
        v = spark.table(tabla).agg(F.sum(col)).first()[0]
        return None if v is None else round(float(v), 2)
    except Exception:
        return None


filas = [
    ("Excel (declarado)",        "30 filas / 36830.00", 30, 36830.00),
    ("bronze.raw_ventas_excel",  t_bronze("raw_ventas_excel"),
     conteo(t_bronze("raw_ventas_excel")), suma(t_bronze("raw_ventas_excel"), "subtotal")),
    ("bronze.core_fact_venta",   t_bronze("core_fact_venta"),
     conteo(t_bronze("core_fact_venta")), suma(t_bronze("core_fact_venta"), "subtotal_pen")),
    ("silver.fact_venta",        t_silver("fact_venta"),
     conteo(t_silver("fact_venta")), suma(t_silver("fact_venta"), "subtotal_pen")),
    ("gold.ventas_excel",        t_gold("ventas_excel"),
     conteo(t_gold("ventas_excel")), suma(t_gold("ventas_excel"), "subtotal_pen")),
]
recon = spark.createDataFrame(
    [(a, str(b), c, d) for a, b, c, d in filas],
    "capa string, objeto string, filas int, subtotal_pen double")
recon = recon.withColumn("coincide_filas", F.col("filas") == F.lit(30)) \
             .withColumn("coincide_monto", F.abs(F.col("subtotal_pen") - F.lit(36830.00)) < 0.01)
display(recon)

ok_recon = recon.filter(~(F.col("coincide_filas") & F.col("coincide_monto"))).count() == 0
dq("monitoreo", "reconciliacion", "excel_a_gold_sin_perdida", "ERROR",
   "todas las capas coinciden" if ok_recon else "hay desvios", "coincidencia total", ok_recon)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliacion del event stream

# COMMAND ----------

recon_ev = spark.createDataFrame([
    ("bronze.stream_evento", conteo(t_bronze("stream_evento"))),
    ("silver.evento",        conteo(t_silver("evento"))),
    ("silver.evento_rechazos", conteo(t_silver("evento_rechazos"))),
    ("silver.orden",         conteo(t_silver("orden"))),
    ("gold.ordenes_detalle", conteo(t_gold("ordenes_detalle"))),
    ("gold.cliente_360",     conteo(t_gold("cliente_360"))),
    ("gold.churn_features",  conteo(t_gold("churn_features"))),
    ("gold.churn_scores",    conteo(t_gold("churn_scores"))),
], "objeto string, filas long")
display(recon_ev)

nb = conteo(t_bronze("stream_evento")) or 0
ns = conteo(t_silver("evento")) or 0
nr = conteo(t_silver("evento_rechazos")) or 0
dq("monitoreo", "stream", "bronze_igual_silver_mas_rechazos", "ERROR",
   f"{nb} vs {ns}+{nr}", "igualdad", nb == ns + nr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resultado de los checks de calidad del batch actual

# COMMAND ----------

display(spark.sql(f"""
    SELECT capa,
           COUNT(*) AS checks,
           SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) AS pass,
           SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) AS fail,
           SUM(CASE WHEN resultado = 'FAIL' AND severidad = 'ERROR' THEN 1 ELSE 0 END) AS fail_criticos
    FROM {t_ops('dq_result')}
    WHERE batch_id = '{BATCH_ID}'
    GROUP BY capa ORDER BY capa"""))

display(spark.sql(f"""
    SELECT capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado
    FROM {t_ops('dq_result')}
    WHERE resultado = 'FAIL'
    ORDER BY ejecutado_ts DESC, severidad LIMIT 100"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bitacora del pipeline y watermarks

# COMMAND ----------

display(spark.sql(f"""
    SELECT notebook, paso, objeto, filas_leidas, filas_escritas, estado,
           ROUND((UNIX_TIMESTAMP(fin_ts) - UNIX_TIMESTAMP(inicio_ts)), 1) AS segundos, mensaje
    FROM {t_ops('pipeline_log')}
    ORDER BY inicio_ts DESC LIMIT 60"""))

display(spark.table(t_ops("watermark")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salud del modelo de churn

# COMMAND ----------

try:
    display(spark.table(t_gold("churn_monitoreo")).orderBy(F.col("ejecutado_ts").desc()))
    display(spark.table(t_gold("churn_modelo_metricas"))
            .filter(F.col("metrica").isin("auc_roc", "auc_pr", "ks", "recall", "precision"))
            .groupBy("modelo").pivot("metrica").agg(F.round(F.max("valor"), 4)))
    display(spark.table(t_gold("churn_lift_deciles")).orderBy("decil"))
    display(spark.table(t_gold("churn_importancia_variables"))
            .orderBy(F.col("valor_abs").desc()).limit(20))
    display(spark.table(t_gold("churn_scores")).groupBy("prioridad").agg(
        F.count("*").alias("clientes"),
        F.round(F.avg("prob_churn"), 4).alias("prob_promedio"),
        F.round(F.sum("ingreso_en_riesgo_pen"), 2).alias("ingreso_en_riesgo_pen")).orderBy("prioridad"))
except Exception as exc:
    print(f"[monitoreo] el modelo aun no se ha entrenado ({type(exc).__name__}). "
          "Ejecuta 05_gold_churn_model.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historial Delta (time travel) de las tablas criticas

# COMMAND ----------

for tabla in [t_bronze("stream_evento"), t_silver("evento"), t_silver("orden"),
              t_gold("cliente_360"), t_gold("churn_features"), t_gold("churn_scores")]:
    try:
        print(f"\n=== {tabla} ===")
        display(spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 5")
                .select("version", "timestamp", "operation", "operationMetrics"))
    except Exception as exc:
        print(f"  (sin historial: {type(exc).__name__})")

# COMMAND ----------

log_paso(NOTEBOOK, "fin", "monitoreo", None, None, mensaje="reporte de calidad generado")
print("[monitoreo] reporte completo. Revisa las tablas de ops para el detalle.")
