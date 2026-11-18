-- ===========================================================================
-- 05 - Calidad de datos: suite de checks ejecutable y auditable.
-- Compara Postgres contra el BASELINE declarado en las hojas de reportes del
-- Excel. Resultado -> audit.data_quality_result
-- Uso:  CALL audit.sp_ejecutar_checks();   SELECT * FROM audit.v_ultimo_check;
-- ===========================================================================
SET client_encoding = 'UTF8';

CREATE OR REPLACE PROCEDURE audit.sp_ejecutar_checks()
LANGUAGE plpgsql AS $$
DECLARE
    v_run timestamptz := now();
BEGIN
    DELETE FROM audit.data_quality_result WHERE ejecutado_ts < now() - INTERVAL '30 days';

    -- ---------------- Completitud ----------------
    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'raw','raw.ventas_excel','conteo_filas_30','ERROR',
           COUNT(*)::text,'30', CASE WHEN COUNT(*) = 30 THEN 'PASS' ELSE 'FAIL' END
    FROM raw.ventas_excel;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.fact_venta','conteo_filas_30','ERROR',
           COUNT(*)::text,'30', CASE WHEN COUNT(*) = 30 THEN 'PASS' ELSE 'FAIL' END
    FROM core.fact_venta;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.dim_cliente','clientes_reales_30','ERROR',
           COUNT(*)::text,'30', CASE WHEN COUNT(*) = 30 THEN 'PASS' ELSE 'FAIL' END
    FROM core.dim_cliente WHERE es_cliente_real;

    -- ---------------- Consistencia raw vs core ----------------
    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.fact_venta','subtotal_core_igual_raw','ERROR',
           ROUND(c.s,2)::text, ROUND(r.s,2)::text,
           CASE WHEN ABS(c.s - r.s) < 0.01 THEN 'PASS' ELSE 'FAIL' END
    FROM (SELECT COALESCE(SUM(subtotal_pen),0) s FROM core.fact_venta) c,
         (SELECT COALESCE(SUM(subtotal),0)     s FROM raw.ventas_excel)  r;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.fact_venta','ganancia_core_igual_raw','ERROR',
           ROUND(c.s,2)::text, ROUND(r.s,2)::text,
           CASE WHEN ABS(c.s - r.s) < 0.01 THEN 'PASS' ELSE 'FAIL' END
    FROM (SELECT COALESCE(SUM(ganancia_pen),0) s FROM core.fact_venta) c,
         (SELECT COALESCE(SUM(ganancia),0)     s FROM raw.ventas_excel)  r;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.fact_venta','usd_core_igual_raw','WARN',
           ROUND(c.s,2)::text, ROUND(r.s,2)::text,
           CASE WHEN ABS(c.s - r.s) < 0.05 THEN 'PASS' ELSE 'FAIL' END
    FROM (SELECT COALESCE(SUM(total_venta_usd),0) s FROM core.fact_venta) c,
         (SELECT COALESCE(SUM(venta_total_usd),0) s FROM raw.ventas_excel)  r;

    -- ---------------- Baseline declarado en el Excel ----------------
    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'mart','mart.v_rep02_subtotal_por_sucursal',
           'rep02_subtotal_'||b.sucursal,'ERROR',
           ROUND(COALESCE(v.total_subtotal_pen,0),2)::text, b.esperado::text,
           CASE WHEN ABS(COALESCE(v.total_subtotal_pen,0) - b.esperado) < 0.01 THEN 'PASS' ELSE 'FAIL' END
    FROM (VALUES ('Puno',15336.0),('Arequipa',8885.0),('Juliaca',12609.0)) AS b(sucursal, esperado)
    LEFT JOIN mart.v_rep02_subtotal_por_sucursal v ON v.sucursal = b.sucursal;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'mart','mart.v_rep04_conteo_por_categoria',
           'rep04_conteo_'||b.categoria,'ERROR',
           COALESCE(v.operaciones,0)::text, b.esperado::text,
           CASE WHEN COALESCE(v.operaciones,0) = b.esperado THEN 'PASS' ELSE 'FAIL' END
    FROM (VALUES ('Equipos tecnologicos',7),('Mobiliario de oficina',8),('Utiles de oficina',7),
                 ('Accesorios tecnologicos',5),('Software y gestion',3)) AS b(categoria, esperado)
    LEFT JOIN mart.v_rep04_conteo_por_categoria v
           ON lower(translate(v.categoria,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN')) = lower(b.categoria);

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'mart','mart.v_rep05_conteo_multicriterio',
           'rep05_'||b.tipo||'_'||b.sucursal,'ERROR',
           COALESCE(v.operaciones,0)::text, b.esperado::text,
           CASE WHEN COALESCE(v.operaciones,0) = b.esperado THEN 'PASS' ELSE 'FAIL' END
    FROM (VALUES ('Empresa','Puno',7),('Empresa','Arequipa',4),('Empresa','Juliaca',5),
                 ('Persona natural','Puno',3),('Persona natural','Arequipa',6),('Persona natural','Juliaca',5))
           AS b(tipo, sucursal, esperado)
    LEFT JOIN mart.v_rep05_conteo_multicriterio v
           ON v.tipo_cliente = b.tipo AND v.sucursal = b.sucursal;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'mart','mart.v_rep08_min_max','rep08_'||b.k,'ERROR',
           ROUND(COALESCE(v.resultado,0),2)::text, b.esperado::text,
           CASE WHEN ABS(COALESCE(v.resultado,0) - b.esperado) < 0.01 THEN 'PASS' ELSE 'FAIL' END
    FROM (VALUES ('precio_max',2790.0),('precio_min',18.0),('cantidad_max',10.0),
                 ('cantidad_min',0.0),('subtotal_max',7350.0),('subtotal_min',0.0),
                 ('ganancia_max',2940.0),('ganancia_min',0.0)) AS b(k, esperado)
    LEFT JOIN (SELECT CASE descripcion
                        WHEN 'Precio unitario mas alto' THEN 'precio_max'
                        WHEN 'Precio unitario mas bajo' THEN 'precio_min'
                        WHEN 'Mayor cantidad vendida'   THEN 'cantidad_max'
                        WHEN 'Menor cantidad vendida'   THEN 'cantidad_min'
                        WHEN 'Subtotal maximo'          THEN 'subtotal_max'
                        WHEN 'Subtotal minimo'          THEN 'subtotal_min'
                        WHEN 'Ganancia maxima'          THEN 'ganancia_max'
                        ELSE 'ganancia_min' END AS k, resultado
               FROM mart.v_rep08_min_max) v ON v.k = b.k;

    -- ---------------- Integridad / reglas de negocio ----------------
    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.fact_venta','estado_coherente_con_ganancia','ERROR',
           COUNT(*)::text,'0', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM core.fact_venta
    WHERE (ganancia_pen > 0 AND estado <> 'Ganancia') OR (ganancia_pen <= 0 AND estado <> 'Perdida');

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'core','core.dim_producto','precio_positivo','ERROR',
           COUNT(*)::text,'0', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM core.dim_producto WHERE precio_lista_pen <= 0;

    -- ---------------- Event stream ----------------
    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'stream','stream.evento','ordenes_con_order_id','ERROR',
           COUNT(*)::text,'0', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM stream.evento WHERE event_type = 'order_placed' AND order_id IS NULL;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'stream','stream.evento','ordenes_con_monto_positivo','ERROR',
           COUNT(*)::text,'0', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM stream.evento WHERE event_type = 'order_placed' AND COALESCE(monto_pen,0) <= 0;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'stream','stream.evento','sin_eventos_en_particion_default','WARN',
           COUNT(*)::text,'0', CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM stream.evento_pdefault;

    INSERT INTO audit.data_quality_result (ejecutado_ts, capa, objeto, nombre_check, severidad, valor_obs, valor_esp, resultado)
    SELECT v_run,'stream','stream.evento','volumen_minimo','WARN',
           COUNT(*)::text,'>= 10000', CASE WHEN COUNT(*) >= 10000 THEN 'PASS' ELSE 'FAIL' END
    FROM stream.evento;

    RAISE NOTICE 'Checks de calidad ejecutados: %', v_run;
END $$;

CREATE OR REPLACE VIEW audit.v_ultimo_check AS
SELECT *
FROM   audit.data_quality_result
WHERE  ejecutado_ts = (SELECT MAX(ejecutado_ts) FROM audit.data_quality_result)
ORDER  BY resultado DESC, severidad, objeto, nombre_check;

CREATE OR REPLACE VIEW audit.v_resumen_check AS
SELECT ejecutado_ts,
       COUNT(*)                                   AS checks,
       COUNT(*) FILTER (WHERE resultado='PASS')   AS pass,
       COUNT(*) FILTER (WHERE resultado='FAIL')   AS fail,
       COUNT(*) FILTER (WHERE resultado='FAIL' AND severidad='ERROR') AS fail_criticos
FROM   audit.data_quality_result
GROUP  BY ejecutado_ts
ORDER  BY ejecutado_ts DESC;
