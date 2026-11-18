-- ===========================================================================
-- 04 - MART: vistas de negocio.
--  (a) Replican en SQL los 8 reportes + BUSCARV del Excel  -> permiten
--      validar que la migracion a Postgres no altero ningun resultado.
--  (b) Extienden el analisis sobre el event stream (funnel, RFM, cohortes).
-- ===========================================================================
SET client_encoding = 'UTF8';

-- ---------------------------------------------------------------------------
-- Vista plana equivalente a la hoja "Base de datos"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ventas_detalle AS
SELECT  f.venta_sk,
        f.fila_excel,
        c.nombre_cliente,
        c.tipo_cliente,
        c.pais,
        c.ciudad,
        p.nombre_producto,
        p.categoria,
        s.nombre                AS sucursal,
        v.nombre                AS vendedor,
        mp.nombre               AS metodo_pago,
        f.cantidad,
        f.precio_unitario_pen,
        f.subtotal_pen,
        f.igv_pen,
        f.utilidad_pen,
        f.costo_unitario_pen,
        f.costo_total_pen,
        f.total_venta_pen,
        f.total_venta_usd,
        f.ganancia_pen,
        f.estado,
        f.es_conversion
FROM    core.fact_venta f
JOIN    core.dim_cliente     c  ON c.cliente_sk     = f.cliente_sk
JOIN    core.dim_producto    p  ON p.producto_sk    = f.producto_sk
JOIN    core.dim_sucursal    s  ON s.sucursal_sk    = f.sucursal_sk
JOIN    core.dim_vendedor    v  ON v.vendedor_sk    = f.vendedor_sk
JOIN    core.dim_metodo_pago mp ON mp.metodo_pago_sk = f.metodo_pago_sk;

-- ---------------------------------------------------------------------------
-- REPORTE 1: conteo general de registros  (CONTAR / CONTARA / CONTAR.SI)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep01_conteo_general AS
SELECT 'Total de registros numericos (Cantidad)'::text AS descripcion, 'CONTAR'::text AS funcion_excel,
       COUNT(*)::numeric AS resultado FROM core.fact_venta
UNION ALL
SELECT 'Total de celdas con datos (Nombres)', 'CONTARA',
       COUNT(DISTINCT cliente_sk)::numeric FROM core.fact_venta
UNION ALL
SELECT 'Celdas vacias en columna Estado', 'CONTAR.BLANCO',
       COUNT(*) FILTER (WHERE estado IS NULL)::numeric FROM core.fact_venta
UNION ALL
SELECT 'Clientes tipo Empresa', 'CONTAR.SI', COUNT(*)::numeric
  FROM mart.v_ventas_detalle WHERE tipo_cliente = 'Empresa'
UNION ALL
SELECT 'Clientes tipo Persona natural', 'CONTAR.SI', COUNT(*)::numeric
  FROM mart.v_ventas_detalle WHERE tipo_cliente = 'Persona natural'
UNION ALL
SELECT 'Ventas en sucursal ' || sucursal, 'CONTAR.SI', COUNT(*)::numeric
  FROM mart.v_ventas_detalle GROUP BY sucursal;

-- ---------------------------------------------------------------------------
-- REPORTE 2: sumas condicionales por sucursal (SUMAR.SI)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep02_subtotal_por_sucursal AS
SELECT  sucursal,
        'SUMAR.SI'::text          AS funcion_excel,
        SUM(subtotal_pen)         AS total_subtotal_pen,
        SUM(total_venta_pen)      AS total_con_igv_pen,
        SUM(ganancia_pen)         AS ganancia_pen,
        COUNT(*)                  AS operaciones
FROM    mart.v_ventas_detalle
GROUP BY sucursal
ORDER BY total_subtotal_pen DESC;

-- ---------------------------------------------------------------------------
-- REPORTE 3: promedio por metodo de pago (PROMEDIO.SI)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep03_promedio_por_metodo_pago AS
SELECT  metodo_pago,
        'PROMEDIO.SI'::text        AS funcion_excel,
        AVG(subtotal_pen)          AS promedio_subtotal_pen,
        SUM(subtotal_pen)          AS total_subtotal_pen,
        COUNT(*)                   AS operaciones
FROM    mart.v_ventas_detalle
GROUP BY metodo_pago
ORDER BY promedio_subtotal_pen DESC;

-- ---------------------------------------------------------------------------
-- REPORTE 4: conteo por categoria (CONTAR.SI)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep04_conteo_por_categoria AS
SELECT  categoria,
        'CONTAR.SI'::text AS funcion_excel,
        COUNT(*)          AS operaciones,
        SUM(cantidad)     AS unidades,
        SUM(subtotal_pen) AS subtotal_pen
FROM    mart.v_ventas_detalle
GROUP BY categoria
ORDER BY operaciones DESC, categoria;

-- ---------------------------------------------------------------------------
-- REPORTE 5: conteo con multiples criterios (CONTAR.SI.CONJUNTO)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep05_conteo_multicriterio AS
SELECT  tipo_cliente,
        sucursal,
        'CONTAR.SI.CONJUNTO'::text AS funcion_excel,
        COUNT(*)                   AS operaciones
FROM    mart.v_ventas_detalle
GROUP BY tipo_cliente, sucursal
ORDER BY tipo_cliente, sucursal;

-- ---------------------------------------------------------------------------
-- REPORTE 6: suma por sucursal y tipo de cliente (SUMAR.SI.CONJUNTO)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep06_suma_sucursal_tipo AS
SELECT  tipo_cliente,
        sucursal,
        'SUMAR.SI.CONJUNTO'::text AS funcion_excel,
        SUM(subtotal_pen)         AS total_subtotal_pen,
        SUM(ganancia_pen)         AS ganancia_pen
FROM    mart.v_ventas_detalle
GROUP BY tipo_cliente, sucursal
ORDER BY tipo_cliente, sucursal;

-- ---------------------------------------------------------------------------
-- REPORTE 7: promedio por categoria y sucursal (PROMEDIO.SI.CONJUNTO)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep07_promedio_categoria_sucursal AS
SELECT  categoria,
        sucursal,
        'PROMEDIO.SI.CONJUNTO'::text AS funcion_excel,
        AVG(subtotal_pen)            AS promedio_subtotal_pen,
        COUNT(*)                     AS operaciones
FROM    mart.v_ventas_detalle
GROUP BY categoria, sucursal
ORDER BY categoria, sucursal;

-- ---------------------------------------------------------------------------
-- REPORTE 8: minimos y maximos (MAX / MIN)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_rep08_min_max AS
SELECT 'Precio unitario mas alto'::text AS descripcion, 'MAX'::text AS funcion_excel, MAX(precio_unitario_pen) AS resultado FROM core.fact_venta
UNION ALL SELECT 'Precio unitario mas bajo', 'MIN', MIN(precio_unitario_pen) FROM core.fact_venta
UNION ALL SELECT 'Mayor cantidad vendida',   'MAX', MAX(cantidad)            FROM core.fact_venta
UNION ALL SELECT 'Menor cantidad vendida',   'MIN', MIN(cantidad)            FROM core.fact_venta
UNION ALL SELECT 'Subtotal maximo',          'MAX', MAX(subtotal_pen)        FROM core.fact_venta
UNION ALL SELECT 'Subtotal minimo',          'MIN', MIN(subtotal_pen)        FROM core.fact_venta
UNION ALL SELECT 'Ganancia maxima',          'MAX', MAX(ganancia_pen)        FROM core.fact_venta
UNION ALL SELECT 'Ganancia minima',          'MIN', MIN(ganancia_pen)        FROM core.fact_venta;

-- ---------------------------------------------------------------------------
-- BUSCARV: funcion equivalente al buscador del Excel
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mart.fn_buscar_cliente(p_nombre text)
RETURNS TABLE (campo text, resultado text)
LANGUAGE sql STABLE AS $$
    SELECT t.campo, t.resultado FROM (
        SELECT 1 AS orden, 'Tipo de cliente'::text,  d.tipo_cliente                       FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 2, 'Pais',             d.pais                                    FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 3, 'Ciudad',           d.ciudad                                  FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 4, 'Producto vendido', d.nombre_producto                         FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 5, 'Categoria',        d.categoria                               FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 6, 'Sucursal',         d.sucursal                                FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 7, 'Vendedor',         d.vendedor                                FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 8, 'Metodo de pago',   d.metodo_pago                             FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 9, 'Cantidad',         d.cantidad::text                          FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 10,'Precio unitario',  to_char(d.precio_unitario_pen,'FM999999.00') FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 11,'Subtotal',         to_char(d.subtotal_pen,'FM999999.00')     FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 12,'Total de venta',   to_char(d.total_venta_pen,'FM999999.00')  FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 13,'Ganancia',         to_char(d.ganancia_pen,'FM999999.00')     FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
        UNION ALL SELECT 14,'Estado',           d.estado                                  FROM mart.v_ventas_detalle d WHERE d.nombre_cliente ILIKE p_nombre
    ) t(orden, campo, resultado)
    ORDER BY t.orden;
$$;
COMMENT ON FUNCTION mart.fn_buscar_cliente IS 'Equivalente SQL del buscador BUSCARV del Excel.';

-- ---------------------------------------------------------------------------
-- KPIs globales
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_kpi_global AS
SELECT  COUNT(*)                                            AS operaciones,
        COUNT(*) FILTER (WHERE es_conversion)               AS operaciones_con_venta,
        COUNT(*) FILTER (WHERE NOT es_conversion)           AS operaciones_sin_venta,
        ROUND(100.0 * COUNT(*) FILTER (WHERE es_conversion) / NULLIF(COUNT(*),0), 2) AS tasa_conversion_pct,
        SUM(cantidad)                                       AS unidades,
        SUM(subtotal_pen)                                   AS subtotal_pen,
        SUM(igv_pen)                                        AS igv_pen,
        SUM(total_venta_pen)                                AS total_venta_pen,
        SUM(total_venta_usd)                                AS total_venta_usd,
        SUM(ganancia_pen)                                   AS ganancia_pen,
        ROUND(AVG(subtotal_pen), 2)                         AS ticket_promedio_pen
FROM    core.fact_venta;

-- ===========================================================================
-- VISTAS SOBRE EL EVENT STREAM
-- ===========================================================================

CREATE OR REPLACE VIEW mart.v_funnel_eventos AS
WITH base AS (
    SELECT event_type, COUNT(*) AS eventos, COUNT(DISTINCT session_id) AS sesiones,
           COUNT(DISTINCT cliente_sk) AS clientes
    FROM stream.evento GROUP BY event_type
)
SELECT  event_type,
        eventos, sesiones, clientes,
        ROUND(100.0 * sesiones / NULLIF(MAX(sesiones) OVER (), 0), 2) AS pct_vs_top_etapa
FROM    base
ORDER BY eventos DESC;

CREATE OR REPLACE VIEW mart.v_ventas_mensuales_stream AS
SELECT  to_char(date_trunc('month', fecha_orden), 'YYYY-MM') AS anio_mes,
        COUNT(*)                        AS ordenes,
        COUNT(DISTINCT cliente_sk)      AS clientes_activos,
        SUM(monto_pen)                  AS venta_pen,
        SUM(ganancia_pen)               AS ganancia_pen,
        ROUND(AVG(monto_pen), 2)        AS ticket_promedio_pen
FROM    stream.v_orden
WHERE   NOT fue_cancelada
GROUP BY 1
ORDER BY 1;

-- RFM por cliente (base del modelo de churn)
CREATE OR REPLACE VIEW mart.v_rfm_cliente AS
WITH ref AS (SELECT MAX(fecha_orden) AS fecha_corte FROM stream.v_orden),
ord AS (
    SELECT o.cliente_sk,
           MAX(o.fecha_orden)  AS ultima_compra,
           MIN(o.fecha_orden)  AS primera_compra,
           COUNT(*)            AS frecuencia,
           SUM(o.monto_pen)    AS monetario_pen,
           AVG(o.monto_pen)    AS ticket_promedio_pen
    FROM   stream.v_orden o
    WHERE  NOT o.fue_cancelada
    GROUP  BY o.cliente_sk)
SELECT  c.cliente_sk,
        c.nombre_cliente,
        c.tipo_cliente,
        c.ciudad,
        c.es_cliente_real,
        o.primera_compra,
        o.ultima_compra,
        EXTRACT(DAY FROM (SELECT fecha_corte FROM ref) - o.ultima_compra)::int AS recencia_dias,
        o.frecuencia,
        ROUND(o.monetario_pen, 2)      AS monetario_pen,
        ROUND(o.ticket_promedio_pen,2) AS ticket_promedio_pen,
        NTILE(5) OVER (ORDER BY EXTRACT(DAY FROM (SELECT fecha_corte FROM ref) - o.ultima_compra) DESC) AS r_score,
        NTILE(5) OVER (ORDER BY o.frecuencia)                                   AS f_score,
        NTILE(5) OVER (ORDER BY o.monetario_pen)                                AS m_score
FROM    ord o
JOIN    core.dim_cliente c ON c.cliente_sk = o.cliente_sk;

COMMENT ON VIEW mart.v_rfm_cliente IS 'RFM por cliente calculado sobre el event stream; insumo de la capa Gold de churn.';
