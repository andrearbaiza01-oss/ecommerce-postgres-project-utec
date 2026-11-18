# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuracion y bootstrap de la plataforma
# MAGIC
# MAGIC **Proyecto:** arquitectura medallon para el e-commerce de CORPORACION PERUVIANS SUR S.A.C.
# MAGIC
# MAGIC Este notebook **no se ejecuta solo**: los demas lo invocan con
# MAGIC `%run ./00_config_setup`. Centraliza:
# MAGIC
# MAGIC 1. Parametros (widgets) de catalogo, esquemas y modo de ingesta.
# MAGIC 2. Creacion de catalogo / esquemas `bronze`, `silver`, `gold`, `ops` y del Volume de landing.
# MAGIC 3. Helpers reutilizables: lectura del origen (JDBC o archivos), escritura Delta,
# MAGIC    bitacora de ejecucion y registro de checks de calidad.
# MAGIC
# MAGIC ## Arquitectura
# MAGIC
# MAGIC ```
# MAGIC  Excel (origen)
# MAGIC      |  scripts/10_cargar_excel.py
# MAGIC      v
# MAGIC  POSTGRES (Docker)            raw.* | core.* (estrella) | stream.evento (event stream particionado)
# MAGIC      |  JDBC  o  export Parquet -> Volume
# MAGIC      v
# MAGIC  BRONZE   ingesta fiel + metadatos de linaje, append incremental con watermark
# MAGIC      v
# MAGIC  SILVER   tipado, deduplicacion, normalizacion, cuarentena, dimensiones conformadas, ordenes
# MAGIC      v
# MAGIC  GOLD     marts de negocio + cliente_360 + features de churn + modelo + scores
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "01 Catalogo")
dbutils.widgets.text("prefijo_esquema", "tarea2", "02 Prefijo de esquemas")
dbutils.widgets.dropdown("modo_ingesta", "volume_files", ["volume_files", "jdbc"], "03 Modo de ingesta")
dbutils.widgets.text("pg_host", "host.docker.internal", "04 Postgres host (solo jdbc)")
dbutils.widgets.text("pg_port", "5435", "05 Postgres puerto (solo jdbc)")
dbutils.widgets.text("pg_db", "ecommerce_dw", "06 Postgres base (solo jdbc)")
dbutils.widgets.text("pg_user", "ecom", "07 Postgres usuario (solo jdbc)")
dbutils.widgets.text("secret_scope", "", "08 Secret scope del password (solo jdbc)")
dbutils.widgets.dropdown("recarga_completa", "false", ["true", "false"], "09 Recarga completa")

# COMMAND ----------

from datetime import datetime, timezone
import uuid

CATALOGO = dbutils.widgets.get("catalogo").strip()
PREFIJO = dbutils.widgets.get("prefijo_esquema").strip()
MODO_INGESTA = dbutils.widgets.get("modo_ingesta").strip()
RECARGA_COMPLETA = dbutils.widgets.get("recarga_completa").strip().lower() == "true"

ESQ_BRONZE = f"{PREFIJO}_bronze"
ESQ_SILVER = f"{PREFIJO}_silver"
ESQ_GOLD = f"{PREFIJO}_gold"
ESQ_OPS = f"{PREFIJO}_ops"

BATCH_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)

# Objetos que viajan desde Postgres. "modo" indica la estrategia hacia Bronze.
ORIGENES = {
    "raw_ventas_excel":       {"origen": "raw.ventas_excel",      "modo": "overwrite", "clave": "raw_id"},
    "raw_parametros_excel":   {"origen": "raw.parametros_excel",  "modo": "overwrite", "clave": "clave"},
    "raw_reportes_excel":     {"origen": "raw.reportes_excel",    "modo": "overwrite", "clave": "raw_id"},
    "core_dim_cliente":       {"origen": "core.dim_cliente",      "modo": "overwrite", "clave": "cliente_sk"},
    "core_dim_producto":      {"origen": "core.dim_producto",     "modo": "overwrite", "clave": "producto_sk"},
    "core_dim_sucursal":      {"origen": "core.dim_sucursal",     "modo": "overwrite", "clave": "sucursal_sk"},
    "core_dim_vendedor":      {"origen": "core.dim_vendedor",     "modo": "overwrite", "clave": "vendedor_sk"},
    "core_dim_metodo_pago":   {"origen": "core.dim_metodo_pago",  "modo": "overwrite", "clave": "metodo_pago_sk"},
    "core_dim_fecha":         {"origen": "core.dim_fecha",        "modo": "overwrite", "clave": "fecha_sk"},
    "core_fact_venta":        {"origen": "core.fact_venta",       "modo": "overwrite", "clave": "venta_sk"},
    "core_parametro_negocio": {"origen": "core.parametro_negocio","modo": "overwrite", "clave": "clave"},
    # Unica tabla con ingesta incremental (append + watermark sobre ingestion_ts)
    "stream_evento":          {"origen": "stream.evento",         "modo": "append",    "clave": "event_uuid",
                               "watermark": "ingestion_ts"},
}

# Reglas de negocio heredadas del Excel (fuente: core.parametro_negocio)
PARAMS_NEGOCIO = {"IGV": 0.18, "MARGEN_UTILIDAD": 0.25, "FACTOR_COSTO": 0.60, "TIPO_CAMBIO_USD": 3.75}

# Ventanas del modelo de churn
CHURN_DIAS_INACTIVIDAD = 90   # sin ordenes en N dias -> churn
CHURN_N_SNAPSHOTS = 6         # cortes mensuales para entrenar con validacion temporal

# COMMAND ----------

def _sql(stmt: str):
    return spark.sql(stmt)


def preparar_entorno() -> dict:
    """Crea catalogo (si se puede), esquemas y volume de landing. Devuelve el contexto efectivo."""
    usa_uc = True
    try:
        _sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
    except Exception as exc:  # catalogo existente sin permisos de CREATE, o metastore Hive
        print(f"[setup] No se creo el catalogo '{CATALOGO}' ({type(exc).__name__}). "
              f"Se asume que ya existe o que el workspace usa hive_metastore.")
    try:
        _sql(f"USE CATALOG {CATALOGO}")
    except Exception:
        usa_uc = False
        print("[setup] Unity Catalog no disponible: se usara hive_metastore sin prefijo de catalogo.")

    prefijo = f"{CATALOGO}." if usa_uc else ""
    for esq in (ESQ_BRONZE, ESQ_SILVER, ESQ_GOLD, ESQ_OPS):
        _sql(f"CREATE SCHEMA IF NOT EXISTS {prefijo}{esq}")

    ruta_landing = None
    if usa_uc:
        try:
            _sql(f"CREATE VOLUME IF NOT EXISTS {prefijo}{ESQ_OPS}.landing")
            ruta_landing = f"/Volumes/{CATALOGO}/{ESQ_OPS}/landing"
        except Exception as exc:
            print(f"[setup] Volume no disponible ({type(exc).__name__}); usar DBFS /FileStore.")
    if ruta_landing is None:
        ruta_landing = "dbfs:/FileStore/tarea2/landing"
        dbutils.fs.mkdirs(ruta_landing)

    ctx = dict(usa_uc=usa_uc, prefijo=prefijo, landing=ruta_landing)
    print(f"[setup] catalogo={CATALOGO if usa_uc else 'hive_metastore'} | "
          f"esquemas={ESQ_BRONZE},{ESQ_SILVER},{ESQ_GOLD},{ESQ_OPS} | landing={ruta_landing}")
    return ctx


CTX = preparar_entorno()
P = CTX["prefijo"]          # prefijo de catalogo listo para interpolar
LANDING = CTX["landing"]


def preparar_sparkml_tmp() -> str | None:
    """Habilita Spark ML en clusters serverless / de acceso compartido.

    En esos entornos la libreria exige la variable SPARKML_TEMP_DFS_PATH
    apuntando a un Volume de Unity Catalog para cachear DataFrames; sin ella
    CrossValidator aborta con PySparkException. Es idempotente.
    """
    import os
    if os.environ.get("SPARKML_TEMP_DFS_PATH"):
        return os.environ["SPARKML_TEMP_DFS_PATH"]
    if not CTX["usa_uc"]:
        return None
    ruta = f"/Volumes/{CATALOGO}/{ESQ_OPS}/tmp"
    try:
        _sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQ_OPS}.tmp")
        dbutils.fs.mkdirs(ruta)
        os.environ["SPARKML_TEMP_DFS_PATH"] = ruta   # cacheo de DataFrames
        os.environ["MLFLOW_DFS_TMP"] = ruta            # guardado de modelos SparkML
        print(f"[setup] SPARKML_TEMP_DFS_PATH / MLFLOW_DFS_TMP = {ruta}")
        return ruta
    except Exception as exc:
        print(f"[setup] no se pudo preparar el volumen temporal de Spark ML "
              f"({type(exc).__name__}): el entrenamiento puede fallar en serverless.")
        return None


preparar_sparkml_tmp()


def t_bronze(nombre: str) -> str: return f"{P}{ESQ_BRONZE}.{nombre}"
def t_silver(nombre: str) -> str: return f"{P}{ESQ_SILVER}.{nombre}"
def t_gold(nombre: str) -> str:   return f"{P}{ESQ_GOLD}.{nombre}"
def t_ops(nombre: str) -> str:    return f"{P}{ESQ_OPS}.{nombre}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas de control (bitacora, watermarks y calidad de datos)

# COMMAND ----------

_sql(f"""CREATE TABLE IF NOT EXISTS {t_ops('pipeline_log')} (
    batch_id STRING, notebook STRING, paso STRING, objeto STRING,
    filas_leidas BIGINT, filas_escritas BIGINT, estado STRING, mensaje STRING,
    inicio_ts TIMESTAMP, fin_ts TIMESTAMP) USING DELTA""")

_sql(f"""CREATE TABLE IF NOT EXISTS {t_ops('watermark')} (
    objeto STRING, ultimo_valor TIMESTAMP, filas_acumuladas BIGINT,
    actualizado_ts TIMESTAMP) USING DELTA""")

_sql(f"""CREATE TABLE IF NOT EXISTS {t_ops('dq_result')} (
    batch_id STRING, ejecutado_ts TIMESTAMP, capa STRING, objeto STRING,
    nombre_check STRING, severidad STRING, valor_obs STRING, valor_esp STRING,
    resultado STRING) USING DELTA""")

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F, types as T


def log_paso(notebook: str, paso: str, objeto: str, leidas=None, escritas=None,
             estado: str = "OK", mensaje: str = None, inicio=None) -> None:
    fila = [(BATCH_ID, notebook, paso, objeto,
             None if leidas is None else int(leidas),
             None if escritas is None else int(escritas),
             estado, mensaje, inicio or datetime.now(timezone.utc),
             datetime.now(timezone.utc))]
    esquema = T.StructType([
        T.StructField("batch_id", T.StringType()), T.StructField("notebook", T.StringType()),
        T.StructField("paso", T.StringType()), T.StructField("objeto", T.StringType()),
        T.StructField("filas_leidas", T.LongType()), T.StructField("filas_escritas", T.LongType()),
        T.StructField("estado", T.StringType()), T.StructField("mensaje", T.StringType()),
        T.StructField("inicio_ts", T.TimestampType()), T.StructField("fin_ts", T.TimestampType())])
    spark.createDataFrame(fila, esquema).write.mode("append").saveAsTable(t_ops("pipeline_log"))


def dq(capa: str, objeto: str, nombre: str, severidad: str, obs, esp, ok: bool) -> bool:
    """Registra un check de calidad en ops.dq_result.

    Devuelve el valor *bloqueante* del check: False solo cuando un check de
    severidad ERROR falla. Asi `exigir([...])` corta el pipeline unicamente ante
    fallas criticas, mientras que los WARN quedan registrados sin detenerlo.
    """
    esquema = T.StructType([
        T.StructField("batch_id", T.StringType()), T.StructField("ejecutado_ts", T.TimestampType()),
        T.StructField("capa", T.StringType()), T.StructField("objeto", T.StringType()),
        T.StructField("nombre_check", T.StringType()), T.StructField("severidad", T.StringType()),
        T.StructField("valor_obs", T.StringType()), T.StructField("valor_esp", T.StringType()),
        T.StructField("resultado", T.StringType())])
    fila = [(BATCH_ID, datetime.now(timezone.utc), capa, objeto, nombre, severidad,
             str(obs), str(esp), "PASS" if ok else "FAIL")]
    spark.createDataFrame(fila, esquema).write.mode("append").saveAsTable(t_ops("dq_result"))
    print(f"   [{'PASS' if ok else 'FAIL'}] {severidad:<5} {capa}.{objeto} :: {nombre} "
          f"(obs={obs}, esp={esp})")
    return ok or severidad != "ERROR"


def exigir(checks: list[bool], etapa: str) -> None:
    """Corta la ejecucion si algun check critico fallo (fail-fast del pipeline)."""
    if not all(checks):
        raise AssertionError(f"[{etapa}] Checks criticos de calidad en FAIL. "
                             f"Revisa {t_ops('dq_result')} para el batch {BATCH_ID}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura del origen: JDBC contra Postgres **o** archivos Parquet en el Volume
# MAGIC
# MAGIC - `modo_ingesta = jdbc`: requiere que el cluster alcance el Postgres por red
# MAGIC   (tunel/VPN/host publico). El password se lee de un **secret scope**.
# MAGIC - `modo_ingesta = volume_files`: se suben los Parquet generados por
# MAGIC   `scripts/30_exportar_para_databricks.py` al Volume de landing. Es la opcion
# MAGIC   recomendada cuando el Postgres corre en Docker local (Databricks SaaS no lo alcanza).

# COMMAND ----------

def _password_pg() -> str:
    scope = dbutils.widgets.get("secret_scope").strip()
    if scope:
        return dbutils.secrets.get(scope=scope, key="pg_password")
    raise ValueError(
        "Para modo jdbc define un secret scope con la clave 'pg_password'.\n"
        "  databricks secrets create-scope tarea2\n"
        "  databricks secrets put-secret tarea2 pg_password")


def leer_origen(nombre_objeto: str, filtro_watermark: str | None = None) -> DataFrame:
    """Devuelve el DataFrame del objeto de origen segun el modo de ingesta configurado."""
    meta = ORIGENES[nombre_objeto]
    if MODO_INGESTA == "jdbc":
        host = dbutils.widgets.get("pg_host").strip()
        port = dbutils.widgets.get("pg_port").strip()
        base = dbutils.widgets.get("pg_db").strip()
        usuario = dbutils.widgets.get("pg_user").strip()
        consulta = f"SELECT * FROM {meta['origen']}"
        if filtro_watermark and meta.get("watermark"):
            consulta += f" WHERE {meta['watermark']} > TIMESTAMP '{filtro_watermark}'"
        return (spark.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{host}:{port}/{base}")
                .option("query", consulta)
                .option("user", usuario)
                .option("password", _password_pg())
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", "10000")
                .load())
    ruta = f"{LANDING}/{nombre_objeto}.parquet"
    df = spark.read.parquet(ruta)
    if filtro_watermark and meta.get("watermark"):
        df = df.filter(F.col(meta["watermark"]) > F.lit(filtro_watermark).cast("timestamp"))
    return df


def leer_watermark(objeto: str):
    fila = (spark.table(t_ops("watermark")).filter(F.col("objeto") == objeto)
            .orderBy(F.col("actualizado_ts").desc()).limit(1).collect())
    return fila[0]["ultimo_valor"] if fila else None


def guardar_watermark(objeto: str, valor, filas: int) -> None:
    spark.sql(f"DELETE FROM {t_ops('watermark')} WHERE objeto = '{objeto}'")
    esquema = T.StructType([
        T.StructField("objeto", T.StringType()), T.StructField("ultimo_valor", T.TimestampType()),
        T.StructField("filas_acumuladas", T.LongType()), T.StructField("actualizado_ts", T.TimestampType())])
    (spark.createDataFrame([(objeto, valor, int(filas), datetime.now(timezone.utc))], esquema)
          .write.mode("append").saveAsTable(t_ops("watermark")))


def escribir_delta(df: DataFrame, tabla: str, modo: str = "overwrite",
                   particiones: list[str] | None = None, comentario: str | None = None) -> int:
    w = df.write.format("delta").mode(modo).option("mergeSchema", "true")
    if modo == "overwrite":
        w = w.option("overwriteSchema", "true")
    if particiones:
        w = w.partitionBy(*particiones)
    w.saveAsTable(tabla)
    if comentario:
        spark.sql(f"COMMENT ON TABLE {tabla} IS '{comentario}'")
    n = spark.table(tabla).count()
    print(f"[delta] {tabla} <- {modo} | filas totales: {n:,}")
    return n


def columnas_linaje(df: DataFrame, origen: str) -> DataFrame:
    """Agrega metadatos de linaje exigidos en Bronze."""
    return (df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_batch_id", F.lit(BATCH_ID))
              .withColumn("_source_system", F.lit("postgres:ecommerce_dw"))
              .withColumn("_source_object", F.lit(origen))
              .withColumn("_ingest_mode", F.lit(MODO_INGESTA)))


print(f"[setup] listo. batch_id={BATCH_ID} | modo_ingesta={MODO_INGESTA} | "
      f"recarga_completa={RECARGA_COMPLETA}")
