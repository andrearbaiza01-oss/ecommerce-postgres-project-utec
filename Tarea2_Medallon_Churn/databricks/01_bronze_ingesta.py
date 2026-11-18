# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - BRONZE: ingesta desde Postgres
# MAGIC
# MAGIC **Contrato de la capa Bronze**
# MAGIC
# MAGIC | Regla | Implementacion |
# MAGIC |---|---|
# MAGIC | Fidelidad al origen | sin casts de negocio, sin filtros, sin renombres |
# MAGIC | Linaje obligatorio | `_ingest_ts`, `_batch_id`, `_source_system`, `_source_object`, `_ingest_mode` |
# MAGIC | Dimensiones y tablas pequenas | snapshot completo (`overwrite`) |
# MAGIC | Event stream | **append incremental** con watermark sobre `ingestion_ts` |
# MAGIC | Idempotencia | el watermark evita reprocesar; `recarga_completa=true` fuerza full refresh |
# MAGIC | Particionamiento | `stream_evento` por `anio_mes` (18 particiones, evita small files) |
# MAGIC
# MAGIC El notebook valida los conteos contra `_manifiesto.json` cuando se usa
# MAGIC `modo_ingesta=volume_files` y corta la ejecucion si un check critico falla.

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

NOTEBOOK = "01_bronze_ingesta"
checks: list[bool] = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificacion del landing / conectividad

# COMMAND ----------

manifiesto = None
if MODO_INGESTA == "volume_files":
    try:
        archivos = [f.name for f in dbutils.fs.ls(LANDING)]
    except Exception:
        archivos = []
    faltantes = [f"{o}.parquet" for o in ORIGENES if f"{o}.parquet" not in archivos]
    print(f"[bronze] archivos en landing: {len(archivos)}")
    if faltantes:
        raise FileNotFoundError(
            "Faltan archivos en el Volume de landing: " + ", ".join(faltantes) +
            f"\n\nPasos:\n 1) python scripts/30_exportar_para_databricks.py\n"
            f" 2) Sube data/export/*.parquet y _manifiesto.json a {LANDING}\n"
            "    (Catalog Explorer > Volumes > landing > Upload, o databricks fs cp)")
    try:
        import json
        manifiesto = json.loads(
            "".join(dbutils.fs.head(f"{LANDING}/_manifiesto.json", 1024 * 1024)))
        print(f"[bronze] manifiesto generado: {manifiesto['generado_utc']}")
    except Exception as exc:
        print(f"[bronze] sin manifiesto para validar conteos ({type(exc).__name__})")
else:
    prueba = leer_origen("core_dim_sucursal")
    print(f"[bronze] JDBC OK, dim_sucursal = {prueba.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Snapshot completo de tablas RAW y dimensiones CORE

# COMMAND ----------

resumen = []
for nombre, meta in ORIGENES.items():
    if meta["modo"] != "overwrite":
        continue
    inicio = datetime.now(timezone.utc)
    df = columnas_linaje(leer_origen(nombre), meta["origen"])
    leidas = df.count()
    escritas = escribir_delta(
        df, t_bronze(nombre), modo="overwrite",
        comentario=f"Bronze snapshot de {meta['origen']} (ingesta {MODO_INGESTA})")
    log_paso(NOTEBOOK, "snapshot", t_bronze(nombre), leidas, escritas, inicio=inicio)
    resumen.append((nombre, leidas, escritas))

    if manifiesto and nombre in manifiesto["objetos"]:
        esperado = manifiesto["objetos"][nombre]["filas"]
        checks.append(dq("bronze", nombre, "conteo_vs_manifiesto", "ERROR",
                         escritas, esperado, escritas == esperado))

display(spark.createDataFrame(resumen, "objeto string, leidas long, escritas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Event stream: ingesta incremental con watermark
# MAGIC
# MAGIC Se leen solo los eventos cuyo `ingestion_ts` supera el watermark guardado.
# MAGIC En la primera corrida (o con `recarga_completa=true`) se trae todo el historico.

# COMMAND ----------

inicio = datetime.now(timezone.utc)
objeto = "stream_evento"
tabla_bronze = t_bronze(objeto)

wm_anterior = None if RECARGA_COMPLETA else leer_watermark(objeto)
if RECARGA_COMPLETA and spark.catalog.tableExists(tabla_bronze):
    spark.sql(f"DROP TABLE {tabla_bronze}")
    print("[bronze] recarga completa: tabla de eventos eliminada")

print(f"[bronze] watermark anterior: {wm_anterior}")
df_ev = leer_origen(objeto, filtro_watermark=None if wm_anterior is None else str(wm_anterior))
df_ev = (columnas_linaje(df_ev, ORIGENES[objeto]["origen"])
         .withColumn("anio_mes", F.date_format(F.col("event_date"), "yyyy-MM")))

nuevos = df_ev.count()
print(f"[bronze] eventos nuevos a ingestar: {nuevos:,}")

if nuevos == 0:
    print("[bronze] nada por hacer: Bronze ya esta al dia")
    log_paso(NOTEBOOK, "incremental", tabla_bronze, 0, 0, estado="SKIP",
             mensaje="sin eventos nuevos", inicio=inicio)
else:
    existe = spark.catalog.tableExists(tabla_bronze)
    total = escribir_delta(df_ev, tabla_bronze, modo="append" if existe else "overwrite",
                           particiones=["anio_mes"],
                           comentario="Bronze append-only del event stream de e-commerce")
    nuevo_wm = df_ev.agg(F.max("ingestion_ts")).collect()[0][0]
    guardar_watermark(objeto, nuevo_wm, total)
    print(f"[bronze] nuevo watermark: {nuevo_wm}")
    log_paso(NOTEBOOK, "incremental", tabla_bronze, nuevos, total, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checks de calidad de la capa Bronze

# COMMAND ----------

ev = spark.table(tabla_bronze)
n_ev = ev.count()

checks.append(dq("bronze", "stream_evento", "volumen_minimo", "ERROR", n_ev, ">= 10000", n_ev >= 10_000))
checks.append(dq("bronze", "stream_evento", "event_uuid_sin_nulos", "ERROR",
                 ev.filter(F.col("event_uuid").isNull()).count(), 0,
                 ev.filter(F.col("event_uuid").isNull()).count() == 0))
dups = n_ev - ev.select("event_uuid").distinct().count()
checks.append(dq("bronze", "stream_evento", "duplicados_event_uuid", "WARN", dups, 0, dups == 0))
checks.append(dq("bronze", "raw_ventas_excel", "conteo_30_filas", "ERROR",
                 spark.table(t_bronze("raw_ventas_excel")).count(), 30,
                 spark.table(t_bronze("raw_ventas_excel")).count() == 30))
n_cli = spark.table(t_bronze("core_dim_cliente")).count()
checks.append(dq("bronze", "core_dim_cliente", "clientes_presentes", "ERROR", n_cli, "> 0", n_cli > 0))

exigir(checks, "bronze")   # corta la ejecucion si algun check ERROR quedo en FAIL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optimizacion fisica y resumen

# COMMAND ----------

for tabla in [t_bronze(o) for o in ORIGENES]:
    try:
        spark.sql(f"OPTIMIZE {tabla}")
    except Exception as exc:
        print(f"[bronze] OPTIMIZE no aplicado en {tabla}: {type(exc).__name__}")

print("\n=== BRONZE ===")
display(spark.sql(f"""
    SELECT '{ESQ_BRONZE}' AS esquema, table_name, 'delta' AS formato
    FROM {CATALOGO}.information_schema.tables
    WHERE table_schema = '{ESQ_BRONZE}' ORDER BY table_name
""") if CTX["usa_uc"] else spark.sql(f"SHOW TABLES IN {ESQ_BRONZE}"))

display(spark.sql(f"""
    SELECT anio_mes, COUNT(*) AS eventos, COUNT(DISTINCT cliente_sk) AS clientes
    FROM {tabla_bronze} GROUP BY anio_mes ORDER BY anio_mes"""))

log_paso(NOTEBOOK, "fin", "bronze", None, n_ev, mensaje="capa bronze completa")
