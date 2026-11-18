# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - GOLD: modelo de churn (entrenamiento, evaluacion y scoring)
# MAGIC
# MAGIC ## Metodologia
# MAGIC
# MAGIC | Aspecto | Decision |
# MAGIC |---|---|
# MAGIC | Validacion | **out-of-time**: se entrena con los cortes antiguos y se evalua con los 2 mas recientes. Un split aleatorio inflaria las metricas porque el mismo cliente aparece en varios cortes |
# MAGIC | Agrupamiento | ningun cliente comparte corte entre train y test (split por `fecha_snapshot`) |
# MAGIC | Modelos | `LogisticRegression` (linea base interpretable) y `GBTClassifier` (no lineal). Se promueve el de mejor AUC en test |
# MAGIC | Desbalance | ponderacion de clases via `weightCol` |
# MAGIC | Seleccion de hiperparametros | `CrossValidator` 3-fold **solo sobre train** |
# MAGIC | Metricas | ROC-AUC, PR-AUC, KS, precision/recall/F1, matriz de confusion y tabla de lift por decil |
# MAGIC | Trazabilidad | todo queda en **MLflow** (params, metricas, artefactos, modelo) |
# MAGIC | Salida de negocio | `gold_churn_scores`: probabilidad, decil, segmento de riesgo, accion sugerida e ingreso en riesgo (S/.) |

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from datetime import datetime, timezone

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Window, functions as F

NOTEBOOK = "05_gold_churn_model"
checks: list[bool] = []
DEC = "decimal(18,4)"
SEMILLA = 42

ft = spark.table(t_gold("churn_features"))
snapshots = sorted([r[0] for r in ft.select("fecha_snapshot").distinct().collect()])
print("[modelo] cortes disponibles:", [str(s) for s in snapshots])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definicion de variables

# COMMAND ----------

CATEGORICAS = ["tipo_cliente", "ciudad", "segmento", "canal_preferido",
               "categoria_preferida", "dispositivo_preferido",
               "metodo_pago_preferido", "sucursal_preferida"]

NUMERICAS = [
    # recencia / ritmo
    "recencia_dias", "dias_sin_actividad", "antiguedad_dias", "intervalo_medio_dias",
    "intervalo_desv_dias", "intervalo_max_dias", "ratio_recencia_intervalo",
    # frecuencia
    "ordenes_hist", "ordenes_30d", "ordenes_90d", "ordenes_90d_prev", "ordenes_365d",
    "ordenes_por_mes_hist", "sesiones_hist", "sesiones_30d", "sesiones_90d",
    "sesiones_90d_prev", "eventos_30d", "eventos_90d",
    # monetario
    "monto_hist_pen", "monto_30d_pen", "monto_90d_pen", "monto_90d_prev_pen",
    "monto_365d_pen", "ticket_promedio_pen", "ticket_desv_pen", "margen_hist_pen",
    "monto_por_mes_hist", "descuento_promedio", "unidades_promedio",
    # tendencia
    "tendencia_ordenes_90d", "tendencia_monto_90d", "tendencia_sesiones_90d",
    # friccion / servicio
    "cancelaciones_hist", "devoluciones_hist", "tickets_hist", "pagos_fallidos_hist",
    "tickets_90d", "devoluciones_90d", "desuscripciones", "reviews",
    "tasa_cancelacion", "tasa_devolucion", "tasa_pago_fallido", "tickets_por_orden",
    # engagement
    "vistas_hist", "carrito_hist", "checkouts_hist", "vistas_por_sesion",
    "tasa_conversion_sesion", "tasa_abandono_carrito",
    "categorias_distintas", "productos_distintos",
]
BOOLEANAS = ["es_b2b", "usa_descuento", "tuvo_friccion"]

NUMERICAS = [c for c in NUMERICAS if c in ft.columns]
CATEGORICAS = [c for c in CATEGORICAS if c in ft.columns]
print(f"[modelo] {len(NUMERICAS)} numericas + {len(CATEGORICAS)} categoricas + {len(BOOLEANAS)} booleanas")

# COMMAND ----------

def preparar(df):
    """Casts, imputacion de categoricas y peso de clase."""
    out = df
    for c in NUMERICAS:
        out = out.withColumn(c, F.col(c).cast("double"))
    for c in BOOLEANAS:
        out = out.withColumn(c, F.col(c).cast("int").cast("double"))
    for c in CATEGORICAS:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit("DESCONOCIDO")))
    return out


datos = preparar(ft).withColumn("label", F.col("churn").cast("double"))

# Split out-of-time: ultimos 2 cortes como test
cortes_test = snapshots[-2:]
train = datos.filter(~F.col("fecha_snapshot").isin(cortes_test)).cache()
test = datos.filter(F.col("fecha_snapshot").isin(cortes_test)).cache()

n_train, n_test = train.count(), test.count()
tasa_tr = train.agg(F.avg("label")).first()[0]
tasa_te = test.agg(F.avg("label")).first()[0]
print(f"[modelo] train: {n_train:,} filas (cortes {[str(s) for s in snapshots[:-2]]}) | churn {tasa_tr:.1%}")
print(f"[modelo] test : {n_test:,} filas (cortes {[str(s) for s in cortes_test]}) | churn {tasa_te:.1%}")

# Peso de clase para compensar el desbalance
peso_pos = (1 - tasa_tr) / max(tasa_tr, 1e-6)
train = train.withColumn("peso", F.when(F.col("label") == 1, F.lit(float(peso_pos))).otherwise(F.lit(1.0)))

checks.append(dq("gold", "churn_model", "train_y_test_no_vacios", "ERROR",
                 f"{n_train}/{n_test}", "> 0", n_train > 0 and n_test > 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline de features

# COMMAND ----------

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in CATEGORICAS]
encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAS],
                        outputCols=[f"{c}_ohe" for c in CATEGORICAS], handleInvalid="keep")
imputer = Imputer(inputCols=NUMERICAS, outputCols=[f"{c}_imp" for c in NUMERICAS],
                  strategy="median")
assembler = VectorAssembler(
    inputCols=[f"{c}_imp" for c in NUMERICAS] + BOOLEANAS + [f"{c}_ohe" for c in CATEGORICAS],
    outputCol="features", handleInvalid="keep")
etapas_features = indexers + [encoder, imputer, assembler]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Entrenamiento con MLflow + validacion cruzada sobre train

# COMMAND ----------

# --- Requisito de Spark ML en serverless / acceso compartido -------------
# La validacion cruzada cachea DataFrames y exige SPARKML_TEMP_DFS_PATH
# apuntando a un Volume de Unity Catalog. Se prepara aqui tambien (ademas de
# en 00_config_setup) para que la celda sea autosuficiente.
import os

RUTA_TMP = f"/Volumes/{CATALOGO}/{ESQ_OPS}/tmp"
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQ_OPS}.tmp")
    dbutils.fs.mkdirs(RUTA_TMP)
    os.environ["SPARKML_TEMP_DFS_PATH"] = RUTA_TMP   # cacheo durante el entrenamiento
    os.environ["MLFLOW_DFS_TMP"] = RUTA_TMP          # guardado del modelo en MLflow
    print(f"[spark-ml] SPARKML_TEMP_DFS_PATH / MLFLOW_DFS_TMP = {RUTA_TMP}")
except Exception as exc:
    RUTA_TMP = None
    print(f"[spark-ml] no se pudo preparar el volumen temporal ({type(exc).__name__}).")

ev_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
ev_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

try:
    mlflow.set_registry_uri("databricks-uc")
    NOMBRE_MODELO = f"{CATALOGO}.{ESQ_GOLD}.modelo_churn_ecommerce"
except Exception:
    NOMBRE_MODELO = "modelo_churn_ecommerce"

try:
    mlflow.set_experiment("/Shared/tarea2_churn_ecommerce")
except Exception as exc:
    print(f"[mlflow] no se pudo usar /Shared ({type(exc).__name__}); "
          "se usara el experimento propio del notebook.")


def metricas(pred, etiqueta: str) -> dict:
    p = pred.withColumn("prob", vector_to_array("probability")[1])
    auc = ev_auc.evaluate(pred)
    pr = ev_pr.evaluate(pred)
    f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(pred)
    acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy").evaluate(pred)
    tp = p.filter((F.col("label") == 1) & (F.col("prediction") == 1)).count()
    fp = p.filter((F.col("label") == 0) & (F.col("prediction") == 1)).count()
    fn = p.filter((F.col("label") == 1) & (F.col("prediction") == 0)).count()
    tn = p.filter((F.col("label") == 0) & (F.col("prediction") == 0)).count()
    prec = tp / max(tp + fp, 1)
    rec = tp / max(tp + fn, 1)
    # KS: maxima separacion entre las acumuladas de churners y no churners
    w = Window.orderBy(F.col("prob").desc())
    ks_df = (p.withColumn("rk", F.row_number().over(w))
              .withColumn("cum_pos", F.sum("label").over(w) / F.lit(max(tp + fn, 1)))
              .withColumn("cum_neg", F.sum(1 - F.col("label")).over(w) / F.lit(max(tn + fp, 1))))
    ks = ks_df.agg(F.max(F.abs(F.col("cum_pos") - F.col("cum_neg")))).first()[0] or 0.0
    m = dict(auc_roc=auc, auc_pr=pr, f1=f1, accuracy=acc, precision=prec, recall=rec,
             ks=float(ks), tp=tp, fp=fp, fn=fn, tn=tn)
    print(f"[{etiqueta}] AUC={auc:.4f} PR-AUC={pr:.4f} KS={ks:.4f} "
          f"F1={f1:.4f} precision={prec:.4f} recall={rec:.4f}")
    return m


resultados = {}

with mlflow.start_run(run_name=f"churn_{BATCH_ID[:8]}") as run_padre:
    mlflow.log_params({
        "dias_inactividad_churn": CHURN_DIAS_INACTIVIDAD,
        "n_snapshots": len(snapshots),
        "cortes_train": ",".join(str(s) for s in snapshots[:-2]),
        "cortes_test": ",".join(str(s) for s in cortes_test),
        "n_train": n_train, "n_test": n_test,
        "tasa_churn_train": round(float(tasa_tr), 4),
        "n_features_numericas": len(NUMERICAS),
        "n_features_categoricas": len(CATEGORICAS),
        "peso_clase_positiva": round(float(peso_pos), 3),
    })

    # ---------- Modelo 1: Regresion Logistica (linea base interpretable)
    with mlflow.start_run(run_name="logistic_regression", nested=True):
        lr = LogisticRegression(labelCol="label", featuresCol="features",
                                weightCol="peso", maxIter=60)
        pipe_lr = Pipeline(stages=etapas_features + [lr])
        grid_lr = (ParamGridBuilder()
                   .addGrid(lr.regParam, [0.001, 0.01, 0.1])
                   .addGrid(lr.elasticNetParam, [0.0, 0.5])
                   .build())
        cv_lr = CrossValidator(estimator=pipe_lr, estimatorParamMaps=grid_lr,
                               evaluator=ev_auc, numFolds=3, seed=SEMILLA, parallelism=2)
        modelo_lr = cv_lr.fit(train)
        mejor_lr = modelo_lr.bestModel
        m_lr = metricas(mejor_lr.transform(test), "LR  test")
        mlflow.log_metrics({f"test_{k}": v for k, v in m_lr.items()})
        mlflow.log_metric("cv_auc_promedio", float(max(modelo_lr.avgMetrics)))
        etapa_lr = mejor_lr.stages[-1]
        mlflow.log_params({"regParam": etapa_lr.getRegParam(),
                           "elasticNetParam": etapa_lr.getElasticNetParam()})
        resultados["logistic_regression"] = (mejor_lr, m_lr)

    # ---------- Modelo 2: Gradient Boosted Trees
    with mlflow.start_run(run_name="gbt", nested=True):
        gbt = GBTClassifier(labelCol="label", featuresCol="features",
                            weightCol="peso", seed=SEMILLA, maxIter=60)
        pipe_gbt = Pipeline(stages=etapas_features + [gbt])
        grid_gbt = (ParamGridBuilder()
                    .addGrid(gbt.maxDepth, [3, 5])
                    .addGrid(gbt.stepSize, [0.05, 0.1])
                    .build())
        cv_gbt = CrossValidator(estimator=pipe_gbt, estimatorParamMaps=grid_gbt,
                                evaluator=ev_auc, numFolds=3, seed=SEMILLA, parallelism=2)
        modelo_gbt = cv_gbt.fit(train)
        mejor_gbt = modelo_gbt.bestModel
        m_gbt = metricas(mejor_gbt.transform(test), "GBT test")
        mlflow.log_metrics({f"test_{k}": v for k, v in m_gbt.items()})
        mlflow.log_metric("cv_auc_promedio", float(max(modelo_gbt.avgMetrics)))
        etapa_gbt = mejor_gbt.stages[-1]
        mlflow.log_params({"maxDepth": etapa_gbt.getMaxDepth(),
                           "stepSize": etapa_gbt.getStepSize()})
        resultados["gbt"] = (mejor_gbt, m_gbt)

    # ---------- Promocion del ganador
    ganador = max(resultados.items(), key=lambda kv: kv[1][1]["auc_roc"])
    nombre_ganador, (modelo_final, m_final) = ganador
    print(f"\n[modelo] ganador: {nombre_ganador} (AUC test = {m_final['auc_roc']:.4f})")
    mlflow.log_param("modelo_ganador", nombre_ganador)
    mlflow.log_metrics({f"ganador_{k}": v for k, v in m_final.items()})

    # En serverless, log_model exige dfs_tmpdir en un Volume de UC.
    # El guardado del modelo NUNCA debe tumbar la corrida: el objeto ya esta
    # entrenado en memoria y el scoring puede continuar aunque falle el registro.
    _kw = {"dfs_tmpdir": RUTA_TMP} if RUTA_TMP else {}
    try:
        mlflow.spark.log_model(modelo_final, "modelo_churn",
                               registered_model_name=NOMBRE_MODELO, **_kw)
        print(f"[modelo] registrado como: {NOMBRE_MODELO}")
    except Exception as exc:
        print(f"[modelo] registro en catalogo no disponible ({type(exc).__name__}: {exc})")
        try:
            mlflow.spark.log_model(modelo_final, "modelo_churn", **_kw)
            print(f"[modelo] logueado sin registrar en el run {run_padre.info.run_id}")
        except Exception as exc2:
            print(f"[modelo] no se pudo loguear el artefacto ({type(exc2).__name__}: {exc2}). "
                  "Las metricas y el scoring continuan normalmente.")
    RUN_ID = run_padre.info.run_id

checks.append(dq("gold", "churn_model", "auc_test_mayor_0_70", "ERROR",
                 round(m_final["auc_roc"], 4), ">= 0.70", m_final["auc_roc"] >= 0.70))
checks.append(dq("gold", "churn_model", "recall_test_mayor_0_60", "WARN",
                 round(m_final["recall"], 4), ">= 0.60", m_final["recall"] >= 0.60))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metricas, importancia de variables y tabla de lift

# COMMAND ----------

filas_met = []
for nombre, (_, m) in resultados.items():
    for k, v in m.items():
        filas_met.append((BATCH_ID, RUN_ID, nombre, k, float(v),
                          nombre == nombre_ganador))
met = spark.createDataFrame(
    filas_met, "batch_id string, run_id string, modelo string, metrica string, valor double, es_ganador boolean")
escribir_delta(met, t_gold("churn_modelo_metricas"), "append" if spark.catalog.tableExists(t_gold("churn_modelo_metricas")) else "overwrite",
               comentario="Grano: batch x modelo x metrica. Historial de evaluacion de los modelos de churn.")

# --- importancia / coeficientes
etapa = modelo_final.stages[-1]

# Reconstruccion de los nombres del vector de features, en el mismo orden que
# VectorAssembler: numericas imputadas -> booleanas -> one-hot por categoria.
# StringIndexer(handleInvalid="keep") produce len(labels)+1 categorias y
# OneHotEncoder(dropLast=True) emite len(labels) columnas por variable.
etiquetas_indexer = {}
for st in modelo_final.stages:
    if hasattr(st, "labels") and hasattr(st, "getOutputCol"):
        etiquetas_indexer[st.getOutputCol()] = list(st.labels)

nombres_features = list(NUMERICAS) + list(BOOLEANAS)
for c in CATEGORICAS:
    etiquetas = etiquetas_indexer.get(f"{c}_idx", [])
    nombres_features += [f"{c}={v}" for v in etiquetas] if etiquetas else [f"{c}_ohe"]

if hasattr(etapa, "featureImportances"):
    pesos = etapa.featureImportances.toArray()
    tipo_peso = "importancia_gini"
else:
    pesos = etapa.coefficients.toArray()
    tipo_peso = "coeficiente"

n = min(len(pesos), len(nombres_features))
imp = (spark.createDataFrame(
            [(BATCH_ID, nombre_ganador, tipo_peso, nombres_features[i], float(pesos[i]),
              float(abs(pesos[i]))) for i in range(n)],
            "batch_id string, modelo string, tipo string, variable string, valor double, valor_abs double")
       .orderBy(F.col("valor_abs").desc()))
escribir_delta(imp, t_gold("churn_importancia_variables"), "overwrite",
               comentario="Grano: variable. Importancia (GBT) o coeficiente (LR) del modelo promovido.")
display(imp.limit(25))

# --- tabla de lift por decil sobre test
pred_test = (modelo_final.transform(test)
             .withColumn("prob", vector_to_array("probability")[1]))
tasa_base = pred_test.agg(F.avg("label")).first()[0]
lift = (pred_test
    .withColumn("decil", F.ntile(10).over(Window.orderBy(F.col("prob").desc())))
    .groupBy("decil").agg(
        F.count("*").alias("clientes"),
        F.sum("label").alias("churners"),
        F.round(F.avg("label"), 4).alias("tasa_churn"),
        F.round(F.min("prob"), 4).alias("prob_min"),
        F.round(F.max("prob"), 4).alias("prob_max"),
        F.round(F.sum("monto_por_mes_hist"), 2).alias("venta_mensual_pen"))
    .withColumn("lift", F.round(F.col("tasa_churn") / F.lit(tasa_base), 3))
    .withColumn("churners_acum", F.sum("churners").over(Window.orderBy("decil")))
    .withColumn("captura_acum_pct", F.round(
        100.0 * F.col("churners_acum") / F.sum("churners").over(Window.partitionBy()), 2))
    .orderBy("decil"))
escribir_delta(lift, t_gold("churn_lift_deciles"), "overwrite",
               comentario="Grano: decil de riesgo en test. Lift y captura acumulada de churners.")
display(lift)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Scoring de la base activa y accionabilidad comercial

# COMMAND ----------

inicio = datetime.now(timezone.utc)
scoring_input = preparar(spark.table(t_gold("churn_scoring_input")))
scores = (modelo_final.transform(scoring_input)
    .withColumn("prob_churn", F.round(vector_to_array("probability")[1], 6))
    .withColumn("decil_riesgo", F.ntile(10).over(Window.orderBy(F.col("prob_churn").desc())))
    .withColumn("segmento_riesgo",
        F.when(F.col("prob_churn") >= 0.70, "critico")
         .when(F.col("prob_churn") >= 0.50, "alto")
         .when(F.col("prob_churn") >= 0.30, "medio").otherwise("bajo"))
    .withColumn("valor_mensual_pen", F.round(F.coalesce(F.col("monto_por_mes_hist"), F.lit(0.0)), 2))
    # ingreso esperado en riesgo: probabilidad x venta mensual x horizonte (3 meses)
    .withColumn("ingreso_en_riesgo_pen",
                F.round(F.col("prob_churn") * F.col("valor_mensual_pen") * 3, 2))
    .withColumn("prioridad",
        F.when((F.col("segmento_riesgo").isin("critico", "alto")) & (F.col("valor_mensual_pen") >= 500), "P1")
         .when(F.col("segmento_riesgo").isin("critico", "alto"), "P2")
         .when(F.col("segmento_riesgo") == "medio", "P3").otherwise("P4"))
    .withColumn("accion_sugerida",
        F.when((F.col("prioridad") == "P1") & (F.col("es_b2b").cast("boolean")),
               "Contacto del ejecutivo de cuenta + propuesta de condiciones corporativas")
         .when(F.col("prioridad") == "P1",
               "Llamada de retencion + cupon personalizado de alto valor")
         .when((F.col("prioridad") == "P2") & (F.col("tickets_por_orden") > 0.5),
               "Resolver incidencias de soporte abiertas antes de cualquier oferta")
         .when(F.col("prioridad") == "P2", "Campania de reactivacion con descuento dirigido")
         .when(F.col("prioridad") == "P3", "Email de recomendaciones segun categoria preferida")
         .otherwise("Mantener en comunicacion regular (newsletter)"))
    .withColumn("motivo_principal",
        F.when(F.col("ratio_recencia_intervalo") >= 2, "Lleva mas del doble de su ciclo habitual sin comprar")
         .when(F.col("tendencia_ordenes_90d") < -0.3, "Caida sostenida de ordenes en los ultimos 90 dias")
         .when(F.col("tasa_devolucion") > 0.2, "Alta tasa de devoluciones")
         .when(F.col("tickets_por_orden") > 0.5, "Historial de incidencias de soporte")
         .when(F.col("tasa_pago_fallido") > 0.2, "Problemas recurrentes de pago")
         .when(F.col("sesiones_90d") == 0, "Sin ninguna visita en los ultimos 90 dias")
         .otherwise("Patron general de bajo engagement"))
    .withColumn("modelo", F.lit(nombre_ganador))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("fecha_scoring", F.current_date())
    .select("cliente_sk", "nombre_cliente", "tipo_cliente", "ciudad", "segmento",
            "es_cliente_real", "sucursal_preferida", "canal_preferido",
            "categoria_preferida", "metodo_pago_preferido",
            "recencia_dias", "ordenes_hist", "ordenes_90d", "monto_hist_pen",
            "ticket_promedio_pen", "ratio_recencia_intervalo", "tendencia_ordenes_90d",
            "tickets_por_orden", "tasa_devolucion",
            "prob_churn", "decil_riesgo", "segmento_riesgo", "prioridad",
            "valor_mensual_pen", "ingreso_en_riesgo_pen", "motivo_principal",
            "accion_sugerida", "modelo", "run_id", "fecha_scoring"))

n_scores = escribir_delta(scores, t_gold("churn_scores"), "overwrite",
                          comentario="Grano: 1 cliente activo puntuado. Probabilidad de churn, prioridad comercial e ingreso en riesgo.")
log_paso(NOTEBOOK, "scoring", t_gold("churn_scores"), None, n_scores, inicio=inicio)

checks.append(dq("gold", "churn_scores", "scores_en_rango", "ERROR",
                 scores.filter((F.col("prob_churn") < 0) | (F.col("prob_churn") > 1)).count(), 0,
                 scores.filter((F.col("prob_churn") < 0) | (F.col("prob_churn") > 1)).count() == 0))
checks.append(dq("gold", "churn_scores", "poblacion_puntuada", "ERROR", n_scores, "> 0", n_scores > 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumen ejecutivo y monitoreo

# COMMAND ----------

resumen = (spark.table(t_gold("churn_scores")).groupBy("segmento_riesgo", "prioridad")
    .agg(F.count("*").alias("clientes"),
         F.round(F.avg("prob_churn"), 4).alias("prob_promedio"),
         F.round(F.sum("valor_mensual_pen"), 2).alias("venta_mensual_pen"),
         F.round(F.sum("ingreso_en_riesgo_pen"), 2).alias("ingreso_en_riesgo_pen"))
    .orderBy("prioridad", F.col("ingreso_en_riesgo_pen").desc()))
escribir_delta(resumen, t_gold("churn_resumen_ejecutivo"), "overwrite",
               comentario="Grano: segmento de riesgo x prioridad. Resumen para la gerencia comercial.")
display(resumen)

monitoreo = (spark.table(t_gold("churn_scores")).agg(
        F.lit(BATCH_ID).alias("batch_id"),
        F.current_timestamp().alias("ejecutado_ts"),
        F.lit(nombre_ganador).alias("modelo"),
        F.lit(RUN_ID).alias("run_id"),
        F.count("*").alias("clientes_puntuados"),
        F.round(F.avg("prob_churn"), 6).alias("prob_media"),
        F.round(F.stddev("prob_churn"), 6).alias("prob_desv"),
        F.round(F.expr("percentile_approx(prob_churn, 0.5)"), 6).alias("prob_p50"),
        F.round(F.expr("percentile_approx(prob_churn, 0.9)"), 6).alias("prob_p90"),
        F.sum(F.when(F.col("segmento_riesgo").isin("critico", "alto"), 1).otherwise(0)).alias("clientes_riesgo_alto"),
        F.round(F.sum("ingreso_en_riesgo_pen"), 2).alias("ingreso_total_en_riesgo_pen"))
    .withColumn("auc_test", F.lit(float(m_final["auc_roc"])))
    .withColumn("ks_test", F.lit(float(m_final["ks"]))))
escribir_delta(monitoreo, t_gold("churn_monitoreo"),
               "append" if spark.catalog.tableExists(t_gold("churn_monitoreo")) else "overwrite",
               comentario="Grano: corrida. Distribucion de scores y metricas para detectar drift entre ejecuciones.")
display(spark.table(t_gold("churn_monitoreo")).orderBy(F.col("ejecutado_ts").desc()))

# COMMAND ----------

display(spark.table(t_gold("churn_scores"))
        .filter(F.col("prioridad") == "P1")
        .orderBy(F.col("ingreso_en_riesgo_pen").desc())
        .select("nombre_cliente", "tipo_cliente", "sucursal_preferida", "recencia_dias",
                "ordenes_hist", "monto_hist_pen", "prob_churn", "ingreso_en_riesgo_pen",
                "motivo_principal", "accion_sugerida")
        .limit(40))

# COMMAND ----------

exigir(checks, "churn_model")
log_paso(NOTEBOOK, "fin", "churn_model", n_train + n_test, n_scores,
         mensaje=f"ganador={nombre_ganador} auc={m_final['auc_roc']:.4f} run={RUN_ID}")
print(f"""
=========================================================
 MODELO DE CHURN - RESUMEN
=========================================================
 Modelo promovido : {nombre_ganador}
 AUC ROC (test)   : {m_final['auc_roc']:.4f}
 PR-AUC (test)    : {m_final['auc_pr']:.4f}
 KS (test)        : {m_final['ks']:.4f}
 Recall / Precision: {m_final['recall']:.3f} / {m_final['precision']:.3f}
 Clientes puntuados: {n_scores:,}
 MLflow run        : {RUN_ID}
 Tablas Gold       : churn_scores, churn_lift_deciles,
                     churn_importancia_variables, churn_modelo_metricas,
                     churn_resumen_ejecutivo, churn_monitoreo
=========================================================""")
