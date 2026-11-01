
-- ============================================================
-- TRABAJO FINAL - BASE DE DATOS
-- PUNTO 4: OPTIMIZACION DE CONSULTAS E INDICES
-- ARCHIVO: 03_COMPARACION_Y_PLANES.sql
-- ============================================================
-- R1: Numero de mediciones
-- R2: Comparacion detallada de tiempos
-- R3: Costos, filas y tiempos
-- R4: Tipos de Scan e indices
-- R5: Operaciones mas costosas
-- R6: Validacion de datos y consultas
-- R7: Verificacion de indices
-- R8: Planes completos
-- R9: Tabla ejecutiva ANTES vs DESPUES
--
-- IMPORTANTE:
-- Ejecutar previamente los scripts 00, 01 y 02.
-- Este archivo solo consulta resultados; no modifica datos.
-- ============================================================

SET search_path TO tf_p4_original, public;


-- ============================================================
-- R1. NUMERO DE MEDICIONES
-- Deben existir 15 mediciones por consulta y etapa.
-- Total esperado: 60 mediciones.
-- ============================================================

SELECT
    consulta,
    etapa,
    COUNT(*) AS mediciones
FROM tf_p4_original.mediciones
GROUP BY consulta, etapa
ORDER BY consulta, etapa;


-- ============================================================
-- R2. COMPARACION DETALLADA DE TIEMPOS
-- Mediana y promedio de ejecucion y planificacion.
-- Una mejora positiva indica reduccion del tiempo.
-- ============================================================

WITH resumen AS (
    SELECT
        consulta,
        etapa,

        ROUND(
            (
                percentile_cont(0.5)
                WITHIN GROUP (
                    ORDER BY (
                        plan_real #>> '{0,Execution Time}'
                    )::numeric
                )
            )::numeric, 4
        ) AS ejec_mediana_ms,

        ROUND(
            AVG(
                (plan_real #>>
                 '{0,Execution Time}')::numeric
            ), 4
        ) AS ejec_promedio_ms,

        ROUND(
            (
                percentile_cont(0.5)
                WITHIN GROUP (
                    ORDER BY (
                        plan_real #>> '{0,Planning Time}'
                    )::numeric
                )
            )::numeric, 4
        ) AS plan_mediana_ms,

        ROUND(
            AVG(
                (plan_real #>>
                 '{0,Planning Time}')::numeric
            ), 4
        ) AS plan_promedio_ms,

        COUNT(*) AS n

    FROM tf_p4_original.mediciones
    GROUP BY consulta, etapa
)

SELECT
    a.consulta,
    a.n AS repeticiones_antes,
    d.n AS repeticiones_despues,

    a.ejec_mediana_ms AS ejec_antes_ms,
    d.ejec_mediana_ms AS ejec_despues_ms,

    ROUND(
        a.ejec_mediana_ms - d.ejec_mediana_ms, 4
    ) AS ahorro_ms,

    ROUND(
        100 * (
            a.ejec_mediana_ms - d.ejec_mediana_ms
        ) / NULLIF(a.ejec_mediana_ms, 0),
        2
    ) AS mejora_pct,

    a.plan_mediana_ms AS planning_antes_ms,
    d.plan_mediana_ms AS planning_despues_ms,

    a.ejec_promedio_ms AS ejec_promedio_antes_ms,
    d.ejec_promedio_ms AS ejec_promedio_despues_ms

FROM resumen a
JOIN resumen d
    ON a.consulta = d.consulta

WHERE a.etapa = 'ANTES'
  AND d.etapa = 'DESPUES'

ORDER BY a.consulta;


-- ============================================================
-- R3. COSTOS, FILAS Y TIEMPOS
-- Se usa la repeticion 8 como muestra central de 15.
-- ============================================================

SELECT
    consulta,
    etapa,

    plan_estimado #>>
        '{0,Plan,Node Type}' AS nodo_raiz,

    (
        plan_estimado #>>
        '{0,Plan,Total Cost}'
    )::numeric AS costo_estimado,

    (
        plan_estimado #>>
        '{0,Plan,Plan Rows}'
    )::bigint AS filas_estimadas,

    (
        plan_real #>>
        '{0,Plan,Actual Rows}'
    )::bigint AS filas_reales,

    (
        plan_real #>>
        '{0,Planning Time}'
    )::numeric AS planificacion_ms,

    (
        plan_real #>>
        '{0,Execution Time}'
    )::numeric AS ejecucion_ms

FROM tf_p4_original.mediciones

WHERE repeticion = 8

ORDER BY consulta, etapa;


-- ============================================================
-- R4. TIPOS DE SCAN E INDICES UTILIZADOS
-- Lee todos los nodos del plan de ejecucion.
-- ============================================================

WITH RECURSIVE nodos AS (

    SELECT
        m.consulta,
        m.etapa,
        m.plan_real #> '{0,Plan}' AS nodo,
        0 AS nivel

    FROM tf_p4_original.mediciones m
    WHERE m.repeticion = 8

    UNION ALL

    SELECT
        n.consulta,
        n.etapa,
        hijo.value,
        n.nivel + 1

    FROM nodos n

    CROSS JOIN LATERAL
        jsonb_array_elements(
            COALESCE(
                n.nodo->'Plans',
                '[]'::jsonb
            )
        ) AS hijo(value)
)

SELECT
    consulta,
    etapa,
    nivel,

    nodo->>'Node Type' AS tipo_scan,
    nodo->>'Relation Name' AS tabla,

    COALESCE(
        nodo->>'Index Name',
        'NINGUNO'
    ) AS indice,

    (
        nodo->>'Total Cost'
    )::numeric AS costo_estimado,

    (
        nodo->>'Plan Rows'
    )::bigint AS filas_estimadas,

    (
        nodo->>'Actual Rows'
    )::bigint AS filas_reales_por_loop,

    (
        nodo->>'Actual Loops'
    )::bigint AS loops,

    COALESCE(
        (
            nodo->>'Rows Removed by Filter'
        )::bigint,
        0
    ) AS filas_descartadas_por_loop,

    (
        nodo->>'Actual Total Time'
    )::numeric AS tiempo_real_ms_por_loop

FROM nodos

WHERE nodo->>'Node Type' LIKE '%Scan%'

ORDER BY consulta, etapa, nivel;


-- ============================================================
-- R5. OPERACIONES MAS COSTOSAS
-- Muestra las 8 operaciones de mayor tiempo acumulado
-- por consulta y etapa.
--
-- IMPORTANTE:
-- Actual Total Time incluye el trabajo de nodos hijos.
-- Los tiempos de los nodos no deben sumarse.
-- ============================================================

WITH RECURSIVE nodos AS (

    SELECT
        m.consulta,
        m.etapa,
        m.plan_real #> '{0,Plan}' AS nodo,
        0 AS nivel

    FROM tf_p4_original.mediciones m
    WHERE m.repeticion = 8

    UNION ALL

    SELECT
        n.consulta,
        n.etapa,
        hijo.value,
        n.nivel + 1

    FROM nodos n

    CROSS JOIN LATERAL
        jsonb_array_elements(
            COALESCE(
                n.nodo->'Plans',
                '[]'::jsonb
            )
        ) AS hijo(value)
),

ordenados AS (

    SELECT
        consulta,
        etapa,
        nivel,

        nodo->>'Node Type' AS operacion,

        COALESCE(
            nodo->>'Index Name',
            ''
        ) AS indice,

        (
            nodo->>'Total Cost'
        )::numeric AS costo_estimado,

        (
            nodo->>'Actual Total Time'
        )::numeric AS tiempo_por_loop_ms,

        (
            nodo->>'Actual Rows'
        )::bigint AS filas_reales_por_loop,

        (
            nodo->>'Actual Loops'
        )::bigint AS loops,

        ROW_NUMBER() OVER (
            PARTITION BY consulta, etapa

            ORDER BY (
                nodo->>'Actual Total Time'
            )::numeric DESC NULLS LAST
        ) AS prioridad

    FROM nodos
)

SELECT
    consulta,
    etapa,
    nivel,
    operacion,
    indice,
    costo_estimado,
    tiempo_por_loop_ms,
    filas_reales_por_loop,
    loops

FROM ordenados

WHERE prioridad <= 8

ORDER BY consulta, etapa, prioridad;


-- ============================================================
-- R6. VALIDACION DE LA BASE DE DATOS
-- Verifica cantidad de eventos, compras y sesiones
-- compradoras.
-- ============================================================

SELECT
    COUNT(*) AS eventos,

    COUNT(*) FILTER (
        WHERE event_type = 'purchase'
    ) AS compras,

    COUNT(DISTINCT user_session) FILTER (
        WHERE event_type = 'purchase'
    ) AS sesiones_compradoras

FROM tf_p4_original.eventos;


-- Validacion de las consultas originales.
-- Se utiliza MD5 para verificar su contenido.

SELECT
    codigo,
    md5(sql_original) AS huella_consulta

FROM tf_p4_original.consultas

ORDER BY codigo;


-- ============================================================
-- R7. INDICES EXISTENTES DESPUES DE LA OPTIMIZACION
-- ============================================================

SELECT
    indexname,
    indexdef

FROM pg_indexes

WHERE schemaname = 'tf_p4_original'
  AND tablename = 'eventos'

ORDER BY indexname;


-- ============================================================
-- R8. PLANES COMPLETOS EXPLAIN Y EXPLAIN ANALYZE
-- Se utiliza la repeticion 8.
-- ============================================================

SELECT
    consulta,
    etapa,
    plan_estimado,
    plan_real

FROM tf_p4_original.mediciones

WHERE repeticion = 8

ORDER BY consulta, etapa;


-- ============================================================
-- R9. TABLA EJECUTIVA DE COMPARACION DE TIEMPOS
-- CONSULTAS Q2 Y Q10
-- ANTES VS DESPUES
--
-- ESTA DEBE SER LA ULTIMA CONSULTA DEL ARCHIVO
--
-- Presenta:
-- 1. Numero de pruebas
-- 2. Tiempo ANTES
-- 3. Tiempo DESPUES
-- 4. Ahorro en milisegundos
-- 5. Porcentaje de mejora
-- 6. Tiempos de planificacion
-- 7. Resultado de la optimizacion
--
-- Mejora positiva: DESPUES tarda menos.
-- Mejora negativa: DESPUES tarda mas.
-- ============================================================

WITH resumen AS (

    SELECT
        consulta,

        COUNT(*) FILTER (
            WHERE etapa = 'ANTES'
        ) AS repeticiones_antes,

        COUNT(*) FILTER (
            WHERE etapa = 'DESPUES'
        ) AS repeticiones_despues,

        AVG(
            (
                plan_real #>>
                '{0,Execution Time}'
            )::numeric
        ) FILTER (
            WHERE etapa = 'ANTES'
        ) AS ejecucion_antes_ms,

        AVG(
            (
                plan_real #>>
                '{0,Execution Time}'
            )::numeric
        ) FILTER (
            WHERE etapa = 'DESPUES'
        ) AS ejecucion_despues_ms,

        AVG(
            (
                plan_real #>>
                '{0,Planning Time}'
            )::numeric
        ) FILTER (
            WHERE etapa = 'ANTES'
        ) AS planning_antes_ms,

        AVG(
            (
                plan_real #>>
                '{0,Planning Time}'
            )::numeric
        ) FILTER (
            WHERE etapa = 'DESPUES'
        ) AS planning_despues_ms

    FROM tf_p4_original.mediciones

    GROUP BY consulta
)

SELECT
    c.consulta,

    COALESCE(
        r.repeticiones_antes, 0
    ) AS pruebas_antes,

    COALESCE(
        r.repeticiones_despues, 0
    ) AS pruebas_despues,

    ROUND(
        r.ejecucion_antes_ms, 4
    ) AS tiempo_antes_ms,

    ROUND(
        r.ejecucion_despues_ms, 4
    ) AS tiempo_despues_ms,

    ROUND(
        r.ejecucion_antes_ms -
        r.ejecucion_despues_ms, 4
    ) AS ahorro_ms,

    ROUND(
        100 * (
            r.ejecucion_antes_ms -
            r.ejecucion_despues_ms
        ) / NULLIF(
            r.ejecucion_antes_ms, 0
        ),
        2
    ) AS mejora_pct,

    ROUND(
        r.planning_antes_ms, 4
    ) AS planificacion_antes_ms,

    ROUND(
        r.planning_despues_ms, 4
    ) AS planificacion_despues_ms,

    CASE

        WHEN COALESCE(
            r.repeticiones_antes, 0
        ) = 0

        OR COALESCE(
            r.repeticiones_despues, 0
        ) = 0

        THEN 'FALTAN MEDICIONES'

        WHEN r.ejecucion_despues_ms
             < r.ejecucion_antes_ms

        THEN 'MEJORA'

        WHEN r.ejecucion_despues_ms
             > r.ejecucion_antes_ms

        THEN 'NO MEJORA'

        ELSE 'SIN CAMBIO'

    END AS resultado

FROM (
    VALUES ('Q2'), ('Q10')
) AS c(consulta)

LEFT JOIN resumen AS r
    ON r.consulta = c.consulta

ORDER BY
    CASE c.consulta
        WHEN 'Q2' THEN 1
        WHEN 'Q10' THEN 2
        ELSE 3
    END;
