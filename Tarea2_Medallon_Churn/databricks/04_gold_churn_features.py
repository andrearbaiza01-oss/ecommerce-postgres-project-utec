# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - GOLD: ingenieria de features de churn (point-in-time correcta)
# MAGIC
# MAGIC ## Definicion del problema
# MAGIC
# MAGIC El e-commerce es un negocio **no contractual**: el cliente nunca "cancela",
# MAGIC simplemente deja de comprar. Por eso el churn se define por inactividad:
# MAGIC
# MAGIC > Un cliente esta en **churn** si, habiendo estado activo antes de la fecha de
# MAGIC > corte `T0`, **no coloca ninguna orden valida** en los `90` dias siguientes.
# MAGIC
# MAGIC ```
# MAGIC   ventana de observacion (features)        ventana de etiqueta (label)
# MAGIC  |-------------------------------|T0|------------------------------|T0+90
# MAGIC   solo datos con event_date <= T0        churn = 0 ordenes en (T0, T0+90]
# MAGIC ```
# MAGIC
# MAGIC ## Decisiones de diseno
# MAGIC
# MAGIC | Decision | Motivo |
# MAGIC |---|---|
# MAGIC | Features **solo** con `event_date <= T0` | evita *data leakage*: ninguna variable mira el futuro |
# MAGIC | Poblacion elegible: con >= 1 orden valida en `[T0-180, T0]` | modelar retencion de la base activa, no reactivacion de inactivos |
# MAGIC | **6 cortes mensuales** (snapshots) | multiplica las filas de entrenamiento y permite validacion temporal real |
# MAGIC | Snapshot de scoring en `T0 = fecha maxima` | produce la poblacion a puntuar hoy (label desconocida) |
# MAGIC | Etiqueta derivada de los datos | no existe ninguna columna de churn en el origen |

# COMMAND ----------

# MAGIC %run ./00_config_setup

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from pyspark.sql import Window, functions as F

NOTEBOOK = "04_gold_churn_features"
checks: list[bool] = []
DEC = "decimal(18,4)"

DIAS_LABEL = CHURN_DIAS_INACTIVIDAD      # 90
DIAS_ELEGIBILIDAD = 180
N_SNAPSHOTS = CHURN_N_SNAPSHOTS          # 6

eventos = spark.table(t_silver("evento"))
ordenes = spark.table(t_silver("orden")).filter(F.col("es_orden_valida"))
clientes = spark.table(t_silver("dim_cliente"))

FECHA_MAX = eventos.agg(F.max("event_date")).first()[0]
print(f"[churn] fecha maxima observada en el stream: {FECHA_MAX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Calendario de cortes (snapshots)

# COMMAND ----------

def primer_dia_mes(d):
    return d.replace(day=1)


def restar_meses(d, n: int):
    y, m = d.year, d.month - n
    while m <= 0:
        m += 12
        y -= 1
    return d.replace(year=y, month=m, day=1)


# El ultimo corte entrenable necesita 90 dias completos de ventana de etiqueta
t0_max_entrenable = primer_dia_mes(FECHA_MAX - timedelta(days=DIAS_LABEL))
SNAPSHOTS = [restar_meses(t0_max_entrenable, i) for i in range(N_SNAPSHOTS - 1, -1, -1)]
SNAPSHOT_SCORING = FECHA_MAX

print("[churn] cortes de entrenamiento:")
for s in SNAPSHOTS:
    print(f"   T0 = {s}  ->  ventana de etiqueta ({s}, {s + timedelta(days=DIAS_LABEL)}]")
print(f"[churn] corte de scoring (sin etiqueta): T0 = {SNAPSHOT_SCORING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Constructor de features para un corte `T0`

# COMMAND ----------

def ventana_ordenes(t0, dias_ini: int, dias_fin: int = 0):
    """Ordenes en [T0 - dias_ini, T0 - dias_fin]."""
    desde = t0 - timedelta(days=dias_ini)
    hasta = t0 - timedelta(days=dias_fin)
    return ordenes.filter((F.col("fecha") > F.lit(desde)) & (F.col("fecha") <= F.lit(hasta)))


def ventana_eventos(t0, dias_ini: int, dias_fin: int = 0):
    desde = t0 - timedelta(days=dias_ini)
    hasta = t0 - timedelta(days=dias_fin)
    return eventos.filter((F.col("event_date") > F.lit(desde)) & (F.col("event_date") <= F.lit(hasta)))


def construir_features(t0, con_etiqueta: bool = True):
    """Devuelve el dataset de features para el corte t0 (y su etiqueta si aplica)."""
    ord_hist = ordenes.filter(F.col("fecha") <= F.lit(t0))
    ev_hist = eventos.filter(F.col("event_date") <= F.lit(t0))

    # ---- poblacion elegible: base activa en los ultimos 180 dias
    elegibles = (ventana_ordenes(t0, DIAS_ELEGIBILIDAD)
                 .select("cliente_sk").distinct())

    # ---- historico de ordenes
    hist = ord_hist.groupBy("cliente_sk").agg(
        F.count("*").alias("ordenes_hist"),
        F.sum("monto_pen").cast(DEC).alias("monto_hist_pen"),
        F.avg("monto_pen").cast(DEC).alias("ticket_promedio_pen"),
        F.stddev("monto_pen").cast(DEC).alias("ticket_desv_pen"),
        F.sum("ganancia_estimada_pen").cast(DEC).alias("margen_hist_pen"),
        F.max("fecha").alias("ultima_orden"),
        F.min("fecha").alias("primera_orden"),
        F.countDistinct("categoria").alias("categorias_distintas"),
        F.countDistinct("producto_sk").alias("productos_distintos"),
        F.avg("descuento_pct").cast("decimal(6,4)").alias("descuento_promedio"),
        F.avg(F.col("cantidad").cast("double")).alias("unidades_promedio"))

    # ---- intervalo medio entre compras y su dispersion
    w_cli = Window.partitionBy("cliente_sk").orderBy("fecha")
    intervalos = (ord_hist.withColumn("fecha_prev", F.lag("fecha").over(w_cli))
                  .filter(F.col("fecha_prev").isNotNull())
                  .withColumn("dias_entre", F.datediff("fecha", "fecha_prev"))
                  .groupBy("cliente_sk").agg(
                      F.avg("dias_entre").alias("intervalo_medio_dias"),
                      F.stddev("dias_entre").alias("intervalo_desv_dias"),
                      F.max("dias_entre").alias("intervalo_max_dias")))

    # ---- ventanas moviles de ordenes
    def agg_ventana(dias_ini, dias_fin, sufijo):
        return (ventana_ordenes(t0, dias_ini, dias_fin).groupBy("cliente_sk").agg(
            F.count("*").alias(f"ordenes_{sufijo}"),
            F.sum("monto_pen").cast(DEC).alias(f"monto_{sufijo}_pen"),
            F.sum(F.when(F.col("tuvo_devolucion"), 1).otherwise(0)).alias(f"devoluciones_{sufijo}"),
            F.sum("tickets_soporte").alias(f"tickets_{sufijo}")))

    o30 = agg_ventana(30, 0, "30d")
    o90 = agg_ventana(90, 0, "90d")
    o90_prev = agg_ventana(180, 90, "90d_prev")
    o365 = agg_ventana(365, 0, "365d")

    # ---- comportamiento en el stream (todo el historico y ventanas recientes)
    ev_agg = ev_hist.groupBy("cliente_sk").agg(
        F.countDistinct("session_id").alias("sesiones_hist"),
        F.count("*").alias("eventos_hist"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("vistas_hist"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carrito_hist"),
        F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias("checkouts_hist"),
        F.sum(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0)).alias("pagos_fallidos_hist"),
        F.sum(F.when(F.col("event_type") == "support_ticket", 1).otherwise(0)).alias("tickets_hist"),
        F.sum(F.when(F.col("event_type") == "return_requested", 1).otherwise(0)).alias("devoluciones_hist"),
        F.sum(F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0)).alias("cancelaciones_hist"),
        F.sum(F.when(F.col("event_type") == "newsletter_unsubscribe", 1).otherwise(0)).alias("desuscripciones"),
        F.sum(F.when(F.col("event_type") == "review_submitted", 1).otherwise(0)).alias("reviews"),
        F.max("event_date").alias("ultima_actividad"),
        F.min("event_date").alias("primera_actividad"))

    ev30 = ventana_eventos(t0, 30).groupBy("cliente_sk").agg(
        F.countDistinct("session_id").alias("sesiones_30d"),
        F.count("*").alias("eventos_30d"))
    ev90 = ventana_eventos(t0, 90).groupBy("cliente_sk").agg(
        F.countDistinct("session_id").alias("sesiones_90d"),
        F.count("*").alias("eventos_90d"))
    ev90p = ventana_eventos(t0, 180, 90).groupBy("cliente_sk").agg(
        F.countDistinct("session_id").alias("sesiones_90d_prev"))

    # ---- preferencias (modo) calculadas con datos <= T0
    def preferencia(col, alias):
        """Moda de una columna dimensional, ignorando los eventos sin valor."""
        return (ev_hist.filter(F.col(col).isNotNull()).groupBy("cliente_sk", col).count()
                .withColumn("rn", F.row_number().over(
                    Window.partitionBy("cliente_sk").orderBy(F.col("count").desc(), F.col(col))))
                .filter(F.col("rn") == 1).select("cliente_sk", F.col(col).alias(alias)))

    pref_canal = preferencia("canal", "canal_preferido")
    pref_cat = preferencia("categoria", "categoria_preferida")
    pref_disp = preferencia("dispositivo", "dispositivo_preferido")
    pref_metodo = (ord_hist.groupBy("cliente_sk", "metodo_pago_sk").count()
                   .withColumn("rn", F.row_number().over(Window.partitionBy("cliente_sk")
                               .orderBy(F.col("count").desc(), "metodo_pago_sk")))
                   .filter(F.col("rn") == 1)
                   .join(spark.table(t_silver("dim_metodo_pago")).select("metodo_pago_sk", "metodo_pago"),
                         "metodo_pago_sk")
                   .select("cliente_sk", F.col("metodo_pago").alias("metodo_pago_preferido")))
    pref_suc = (ord_hist.groupBy("cliente_sk", "sucursal_sk").count()
                .withColumn("rn", F.row_number().over(Window.partitionBy("cliente_sk")
                            .orderBy(F.col("count").desc(), "sucursal_sk")))
                .filter(F.col("rn") == 1)
                .join(spark.table(t_silver("dim_sucursal")).select("sucursal_sk", "sucursal"), "sucursal_sk")
                .select("cliente_sk", F.col("sucursal").alias("sucursal_preferida")))

    df = (elegibles
          .join(clientes.select("cliente_sk", "nombre_cliente", "tipo_cliente", "ciudad",
                                "segmento", "es_cliente_real", "es_b2b"), "cliente_sk")
          .join(hist, "cliente_sk", "left")
          .join(intervalos, "cliente_sk", "left")
          .join(o30, "cliente_sk", "left").join(o90, "cliente_sk", "left")
          .join(o90_prev, "cliente_sk", "left").join(o365, "cliente_sk", "left")
          .join(ev_agg, "cliente_sk", "left")
          .join(ev30, "cliente_sk", "left").join(ev90, "cliente_sk", "left")
          .join(ev90p, "cliente_sk", "left")
          .join(pref_canal, "cliente_sk", "left").join(pref_cat, "cliente_sk", "left")
          .join(pref_disp, "cliente_sk", "left").join(pref_metodo, "cliente_sk", "left")
          .join(pref_suc, "cliente_sk", "left"))

    ceros = ["ordenes_30d", "ordenes_90d", "ordenes_90d_prev", "ordenes_365d",
             "monto_30d_pen", "monto_90d_pen", "monto_90d_prev_pen", "monto_365d_pen",
             "devoluciones_30d", "devoluciones_90d", "devoluciones_90d_prev", "devoluciones_365d",
             "tickets_30d", "tickets_90d", "tickets_90d_prev", "tickets_365d",
             "sesiones_30d", "sesiones_90d", "sesiones_90d_prev", "eventos_30d", "eventos_90d",
             "pagos_fallidos_hist", "tickets_hist", "devoluciones_hist", "cancelaciones_hist",
             "desuscripciones", "reviews", "carrito_hist", "checkouts_hist", "vistas_hist"]
    df = df.fillna(0, subset=[c for c in ceros if c in df.columns])

    # ---- features derivadas (ratios, tendencias y senales de riesgo)
    df = (df
        .withColumn("fecha_snapshot", F.lit(t0).cast("date"))
        .withColumn("recencia_dias", F.datediff(F.lit(t0), F.col("ultima_orden")))
        .withColumn("dias_sin_actividad", F.datediff(F.lit(t0), F.col("ultima_actividad")))
        .withColumn("antiguedad_dias", F.greatest(F.datediff(F.lit(t0), F.col("primera_actividad")), F.lit(1)))
        .withColumn("intervalo_medio_dias", F.coalesce(F.col("intervalo_medio_dias"),
                                                       F.col("antiguedad_dias").cast("double")))
        .withColumn("intervalo_desv_dias", F.coalesce(F.col("intervalo_desv_dias"), F.lit(0.0)))
        # senal clasica: recencia relativa al ritmo historico del cliente
        .withColumn("ratio_recencia_intervalo",
                    F.round(F.col("recencia_dias") / F.greatest(F.col("intervalo_medio_dias"), F.lit(1.0)), 4))
        .withColumn("ordenes_por_mes_hist",
                    F.round(F.col("ordenes_hist") * 30.0 / F.col("antiguedad_dias"), 4))
        .withColumn("tendencia_ordenes_90d",
                    F.round((F.col("ordenes_90d") - F.col("ordenes_90d_prev"))
                            / F.greatest(F.col("ordenes_90d_prev").cast("double"), F.lit(1.0)), 4))
        .withColumn("tendencia_monto_90d",
                    F.round((F.col("monto_90d_pen") - F.col("monto_90d_prev_pen"))
                            / F.greatest(F.col("monto_90d_prev_pen").cast("double"), F.lit(1.0)), 4))
        .withColumn("tendencia_sesiones_90d",
                    F.round((F.col("sesiones_90d") - F.col("sesiones_90d_prev"))
                            / F.greatest(F.col("sesiones_90d_prev").cast("double"), F.lit(1.0)), 4))
        .withColumn("tasa_conversion_sesion",
                    F.round(F.col("ordenes_hist") / F.greatest(F.col("sesiones_hist"), F.lit(1)), 4))
        .withColumn("tasa_abandono_carrito",
                    F.round(1 - F.col("ordenes_hist") / F.greatest(F.col("carrito_hist"), F.lit(1)), 4))
        .withColumn("tasa_cancelacion",
                    F.round(F.col("cancelaciones_hist") / F.greatest(F.col("ordenes_hist"), F.lit(1)), 4))
        .withColumn("tasa_devolucion",
                    F.round(F.col("devoluciones_hist") / F.greatest(F.col("ordenes_hist"), F.lit(1)), 4))
        .withColumn("tasa_pago_fallido",
                    F.round(F.col("pagos_fallidos_hist") / F.greatest(F.col("checkouts_hist"), F.lit(1)), 4))
        .withColumn("tickets_por_orden",
                    F.round(F.col("tickets_hist") / F.greatest(F.col("ordenes_hist"), F.lit(1)), 4))
        .withColumn("vistas_por_sesion",
                    F.round(F.col("vistas_hist") / F.greatest(F.col("sesiones_hist"), F.lit(1)), 4))
        .withColumn("monto_por_mes_hist",
                    F.round(F.col("monto_hist_pen") / (F.col("antiguedad_dias") / 30.0), 2))
        .withColumn("usa_descuento", (F.coalesce(F.col("descuento_promedio"), F.lit(0)) > 0))
        .withColumn("tuvo_friccion", (F.col("tickets_hist") + F.col("devoluciones_hist")
                                      + F.col("cancelaciones_hist") + F.col("pagos_fallidos_hist")) > 0))

    if con_etiqueta:
        label_ini = t0
        label_fin = t0 + timedelta(days=DIAS_LABEL)
        compras_futuras = (ordenes
            .filter((F.col("fecha") > F.lit(label_ini)) & (F.col("fecha") <= F.lit(label_fin)))
            .groupBy("cliente_sk").agg(F.count("*").alias("ordenes_label"),
                                       F.sum("monto_pen").cast(DEC).alias("monto_label_pen")))
        df = (df.join(compras_futuras, "cliente_sk", "left")
                .fillna(0, subset=["ordenes_label", "monto_label_pen"])
                .withColumn("churn", F.when(F.col("ordenes_label") == 0, 1).otherwise(0))
                .withColumn("fecha_fin_label", F.lit(label_fin).cast("date")))
    else:
        df = (df.withColumn("ordenes_label", F.lit(None).cast("long"))
                .withColumn("monto_label_pen", F.lit(None).cast(DEC))
                .withColumn("churn", F.lit(None).cast("int"))
                .withColumn("fecha_fin_label", F.lit(None).cast("date")))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabla de entrenamiento: union de los cortes mensuales

# COMMAND ----------

inicio = datetime.now(timezone.utc)
frames = []
for t0 in SNAPSHOTS:
    d = construir_features(t0, con_etiqueta=True)
    n = d.count()
    tasa = d.agg(F.avg("churn")).first()[0]
    print(f"   T0={t0} | elegibles={n:,} | tasa de churn={tasa:.1%}")
    frames.append(d)

features = frames[0]
for d in frames[1:]:
    features = features.unionByName(d)

features = features.withColumn("_gold_ts", F.current_timestamp())
n_total = escribir_delta(features, t_gold("churn_features"), "overwrite",
                         particiones=["fecha_snapshot"],
                         comentario=("Grano: cliente x fecha_snapshot. Features point-in-time "
                                     f"(solo datos <= T0) y etiqueta de churn a {DIAS_LABEL} dias."))
log_paso(NOTEBOOK, "features_entrenamiento", t_gold("churn_features"), None, n_total, inicio=inicio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Poblacion a puntuar hoy (sin etiqueta)

# COMMAND ----------

scoring = (construir_features(SNAPSHOT_SCORING, con_etiqueta=False)
           .withColumn("_gold_ts", F.current_timestamp()))
n_scoring = escribir_delta(scoring, t_gold("churn_scoring_input"), "overwrite",
                           comentario=f"Grano: 1 cliente elegible al corte {SNAPSHOT_SCORING}. Sin etiqueta: es la poblacion a puntuar.")
log_paso(NOTEBOOK, "features_scoring", t_gold("churn_scoring_input"), None, n_scoring)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checks de calidad del dataset de modelado

# COMMAND ----------

ft = spark.table(t_gold("churn_features"))
tasa_global = ft.agg(F.avg("churn")).first()[0]

checks.append(dq("gold", "churn_features", "filas_suficientes", "ERROR", n_total, ">= 500", n_total >= 500))
checks.append(dq("gold", "churn_features", "etiqueta_sin_nulos", "ERROR",
                 ft.filter(F.col("churn").isNull()).count(), 0,
                 ft.filter(F.col("churn").isNull()).count() == 0))
checks.append(dq("gold", "churn_features", "balance_entre_5_y_95pct", "ERROR",
                 f"{tasa_global:.3f}", "0.05 - 0.95", 0.05 < tasa_global < 0.95))
checks.append(dq("gold", "churn_features", "snapshots_completos", "ERROR",
                 ft.select("fecha_snapshot").distinct().count(), N_SNAPSHOTS,
                 ft.select("fecha_snapshot").distinct().count() == N_SNAPSHOTS))
checks.append(dq("gold", "churn_features", "clave_unica_cliente_snapshot", "ERROR",
                 n_total - ft.select("cliente_sk", "fecha_snapshot").distinct().count(), 0,
                 n_total == ft.select("cliente_sk", "fecha_snapshot").distinct().count()))
# Anti-leakage: ninguna feature puede provenir de la ventana de etiqueta
checks.append(dq("gold", "churn_features", "sin_leakage_recencia", "ERROR",
                 ft.filter(F.col("recencia_dias") < 0).count(), 0,
                 ft.filter(F.col("recencia_dias") < 0).count() == 0))
checks.append(dq("gold", "churn_scoring_input", "poblacion_a_puntuar", "ERROR",
                 n_scoring, "> 0", n_scoring > 0))

exigir(checks, "churn_features")

# COMMAND ----------

display(ft.groupBy("fecha_snapshot").agg(
    F.count("*").alias("clientes"),
    F.sum("churn").alias("churn"),
    F.round(F.avg("churn"), 4).alias("tasa_churn"),
    F.round(F.avg("recencia_dias"), 1).alias("recencia_promedio"),
    F.round(F.avg("ordenes_90d"), 2).alias("ordenes_90d_promedio")).orderBy("fecha_snapshot"))

display(ft.groupBy("churn").agg(
    F.count("*").alias("clientes"),
    F.round(F.avg("recencia_dias"), 1).alias("recencia"),
    F.round(F.avg("ratio_recencia_intervalo"), 2).alias("ratio_recencia_intervalo"),
    F.round(F.avg("ordenes_90d"), 2).alias("ordenes_90d"),
    F.round(F.avg("tendencia_ordenes_90d"), 3).alias("tendencia_ordenes"),
    F.round(F.avg("tickets_por_orden"), 3).alias("tickets_por_orden"),
    F.round(F.avg("tasa_devolucion"), 3).alias("tasa_devolucion"),
    F.round(F.avg("monto_hist_pen"), 2).alias("monto_historico")))

log_paso(NOTEBOOK, "fin", "churn_features", None, n_total, mensaje="features de churn listas")
print(f"[churn] dataset de modelado: {n_total:,} filas | tasa de churn global: {tasa_global:.1%}")
