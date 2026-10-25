-- Trabajo Final - Base de Datos (UTEC)
-- Parte 1, seccion 2: consultas SQL orientadas al negocio
--
-- Esquema: analisis_ecommerce_produccion
-- Datos:   eventos de un e-commerce, 2019-11-01 entre 00:00 y 00:31 (4,083 eventos)
--
-- Q1-Q5  consultas basicas e intermedias
-- Q6-Q10 consultas avanzadas
--
-- Nota sobre los datos: el volcado cubre una sola ventana de 32 minutos,
-- por eso el analisis temporal (Q8) se hace a nivel de minuto. Ademas la
-- tabla dispositivos esta vacia y usuarios no trae nombre ni fecha de
-- registro, asi que esas columnas no se usan.

SET search_path TO analisis_ecommerce_produccion;


-- =====================================================================
-- Q1. Que productos atraen mas atencion y de que marca son?
--
-- Resultado: los 15 productos mas vistos. Arriba queda el 1004856 de
-- samsung con 45 vistas, seguido de dos modelos de apple.
--
-- Lectura: la atencion se concentra en pocos smartphones de samsung,
-- apple y xiaomi. Son los que deberian ocupar el home y los primeros
-- resultados del buscador, porque ya tienen la demanda.
-- =====================================================================
SELECT
    e.product_id,
    m.brand_name AS marca,
    COALESCE(NULLIF(e.category_code, ''), 'sin categoria') AS categoria,
    COUNT(*) AS vistas,
    COUNT(DISTINCT e.user_id) AS usuarios,
    ROUND(AVG(e.price), 2) AS precio_promedio
FROM eventos e
INNER JOIN productos p ON p.product_id = e.product_id
INNER JOIN marcas m ON m.brand_id = p.brand_id
WHERE e.event_type = 'view'
GROUP BY e.product_id, m.brand_name, e.category_code
ORDER BY vistas DESC
LIMIT 15;


-- =====================================================================
-- Q2. Que categorias generan ingresos y cual es su ticket promedio?
--
-- Resultado: 6 categorias (el HAVING deja fuera las que vendieron una
-- sola vez). electronics.smartphone factura unos S/ 12,100, mas del
-- doble que la siguiente.
--
-- Lectura: el negocio depende casi por completo de una categoria. El
-- ticket promedio ayuda a decidir donde empujar volumen y donde margen.
-- =====================================================================
SELECT
    COALESCE(NULLIF(category_code, ''), 'sin categoria') AS categoria,
    COUNT(*) AS compras,
    COUNT(DISTINCT user_id) AS compradores,
    ROUND(SUM(price), 2) AS ingreso_total,
    ROUND(AVG(price), 2) AS ticket_promedio,
    MIN(price) AS ticket_minimo,
    MAX(price) AS ticket_maximo
FROM eventos
WHERE event_type = 'purchase'
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY ingreso_total DESC;


-- =====================================================================
-- Q3. Que marcas convierten mejor las vistas que reciben?
--
-- Resultado: 28 marcas con 20 vistas o mas. Samsung convierte 3.8% y
-- apple 3.0%; veinte marcas se quedan en cero pese a tener trafico
-- (huawei con 89 vistas es el caso mas llamativo).
--
-- Lectura: una marca muy vista que no vende no tiene problema de
-- trafico sino de precio, stock o ficha de producto. Es la señal mas
-- accionable para el area comercial.
-- =====================================================================
SELECT
    m.brand_name AS marca,
    COUNT(*) FILTER (WHERE e.event_type = 'view') AS vistas,
    COUNT(*) FILTER (WHERE e.event_type = 'cart') AS carritos,
    COUNT(*) FILTER (WHERE e.event_type = 'purchase') AS compras,
    ROUND(100.0 * COUNT(*) FILTER (WHERE e.event_type = 'purchase')
                / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'view'), 0), 2) AS conversion_pct,
    ROUND(COALESCE(SUM(e.price) FILTER (WHERE e.event_type = 'purchase'), 0), 2) AS ingreso
FROM eventos e
INNER JOIN productos p ON p.product_id = e.product_id
INNER JOIN marcas m ON m.brand_id = p.brand_id
GROUP BY m.brand_name
HAVING COUNT(*) FILTER (WHERE e.event_type = 'view') >= 20
ORDER BY conversion_pct DESC NULLS LAST, vistas DESC;


-- =====================================================================
-- Q4. Cuantos usuarios registrados no hicieron absolutamente nada?
--
-- Resultado: los primeros 20 usuarios ordenados por actividad. Salen
-- todos con cero eventos; en total son 101 de 1,113 usuarios.
--
-- Lectura: aqui el LEFT JOIN es obligatorio. Con INNER JOIN esos 101
-- usuarios desaparecerian y pareceria que toda la base esta activa.
-- Ese 9% inactivo es el publico de una campaña de reactivacion.
-- =====================================================================
SELECT
    u.user_id,
    COUNT(e.event_id) AS eventos,
    COUNT(DISTINCT e.user_session) AS sesiones,
    COUNT(*) FILTER (WHERE e.event_type = 'purchase') AS compras,
    MIN(e.event_time) AS primer_evento,
    MAX(e.event_time) AS ultimo_evento
FROM usuarios u
LEFT JOIN eventos e ON e.user_id = u.user_id
GROUP BY u.user_id
ORDER BY eventos ASC, u.user_id
LIMIT 20;


-- =====================================================================
-- Q5. Que cobertura tiene el registro de dispositivos?
--
-- Resultado: ninguna sesion tiene dispositivo asociado, porque la tabla
-- esta vacia. Tambien aparecen sesiones que no generaron ni un evento
-- (son 131 en total).
--
-- Lectura: hallazgo de calidad de datos. Hoy no se puede segmentar por
-- mobile / desktop / tablet, que es una de las dimensiones mas usadas
-- en e-commerce. Las sesiones sin eventos apuntan a rebote o a un error
-- de tracking.
--
-- Se cuenta con DISTINCT porque dispositivos.user_session no es unica:
-- si una sesion tuviera dos dispositivos, el join duplicaria eventos.
-- =====================================================================
SELECT
    s.user_session,
    s.user_id,
    s.session_start,
    COALESCE(d.device_type, 'no registrado') AS dispositivo,
    COUNT(DISTINCT e.event_id) AS eventos
FROM sesiones s
LEFT JOIN dispositivos d ON d.user_session = s.user_session
LEFT JOIN eventos e ON e.user_session = s.user_session
GROUP BY s.user_session, s.user_id, s.session_start, d.device_type
ORDER BY eventos ASC, s.session_start
LIMIT 20;


-- =====================================================================
-- Q6. Como se reparten las sesiones en el embudo de conversion?
--
-- Resultado: de 1,130 sesiones con actividad, 95% solo navega, 1% deja
-- el carrito abandonado y 3.5% compra. Esas 39 sesiones que compran
-- explican los S/ 20,800 de ingreso.
--
-- Lectura: el embudo es muy angosto. El grupo del medio es el de mayor
-- retorno: ya mostro intencion de compra y solo le falta un empujon
-- (recordatorio, envio gratis, descuento de cierre). Las sesiones que
-- compran ven 4.7 productos en promedio contra 3.5 de las que no.
-- =====================================================================
WITH por_sesion AS (
    SELECT
        user_session,
        COUNT(*) FILTER (WHERE event_type = 'view') AS vistas,
        COUNT(*) FILTER (WHERE event_type = 'cart') AS carritos,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS compras,
        COALESCE(SUM(price) FILTER (WHERE event_type = 'purchase'), 0) AS ingreso
    FROM eventos
    GROUP BY user_session
),
clasificadas AS (
    SELECT
        por_sesion.*,
        CASE
            WHEN compras > 0 THEN '3. compro'
            WHEN carritos > 0 THEN '2. abandono carrito'
            ELSE '1. solo navego'
        END AS etapa
    FROM por_sesion
)
SELECT
    etapa,
    COUNT(*) AS sesiones,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
    ROUND(AVG(vistas), 2) AS vistas_promedio,
    ROUND(SUM(ingreso), 2) AS ingreso
FROM clasificadas
GROUP BY etapa
ORDER BY etapa;


-- =====================================================================
-- Q7. Cuales son los 3 productos top de cada categoria?
--
-- Resultado: 20 filas sobre las 10 categorias con ventas. El contraste
-- es fuerte: electronics.video.tv depende en 92% de un solo televisor
-- samsung, mientras que en smartphones el primer puesto pesa solo 27%.
--
-- Lectura: una categoria que vive de un SKU es fragil, un quiebre de
-- stock la tumba entera. Se usa DENSE_RANK y no ROW_NUMBER para que dos
-- productos empatados compartan puesto.
-- =====================================================================
WITH ventas AS (
    SELECT
        COALESCE(NULLIF(e.category_code, ''), 'sin categoria') AS categoria,
        e.product_id,
        m.brand_name AS marca,
        COUNT(*) AS unidades,
        ROUND(SUM(e.price), 2) AS ingreso
    FROM eventos e
    INNER JOIN productos p ON p.product_id = e.product_id
    INNER JOIN marcas m ON m.brand_id = p.brand_id
    WHERE e.event_type = 'purchase'
    GROUP BY 1, e.product_id, m.brand_name
),
ranking AS (
    SELECT
        ventas.*,
        DENSE_RANK() OVER (PARTITION BY categoria ORDER BY ingreso DESC) AS puesto,
        ROUND(100.0 * ingreso / SUM(ingreso) OVER (PARTITION BY categoria), 2) AS pct_categoria
    FROM ventas
)
SELECT categoria, puesto, product_id, marca, unidades, ingreso, pct_categoria
FROM ranking
WHERE puesto <= 3
ORDER BY categoria, puesto;


-- =====================================================================
-- Q8. Como evoluciona la actividad minuto a minuto?
--
-- Resultado: 32 filas, una por minuto, con el ingreso del minuto, el
-- acumulado, la variacion contra el minuto anterior y una media movil
-- de vistas de 3 minutos. El acumulado cierra en S/ 20,800 y hay 6
-- minutos sin ninguna compra.
--
-- Lectura: el acumulado muestra el ritmo al que entra el dinero y la
-- media movil limpia el ruido para ver si el trafico sube o baja. Los
-- minutos con caida fuerte son los que vale la pena revisar.
--
-- Si mas adelante se cargan varios dias, basta cambiar 'minute' por
-- 'day' en el DATE_TRUNC y la consulta sirve igual.
-- =====================================================================
WITH por_minuto AS (
    SELECT
        DATE_TRUNC('minute', event_time) AS minuto,
        COUNT(*) AS eventos,
        COUNT(*) FILTER (WHERE event_type = 'view') AS vistas,
        COUNT(*) FILTER (WHERE event_type = 'cart') AS carritos,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS compras,
        COALESCE(SUM(price) FILTER (WHERE event_type = 'purchase'), 0) AS ingreso
    FROM eventos
    GROUP BY 1
)
SELECT
    minuto,
    eventos,
    vistas,
    compras,
    ingreso,
    SUM(ingreso) OVER (ORDER BY minuto) AS ingreso_acumulado,
    LAG(ingreso) OVER (ORDER BY minuto) AS ingreso_anterior,
    ingreso - LAG(ingreso) OVER (ORDER BY minuto) AS variacion,
    ROUND(AVG(vistas) OVER (ORDER BY minuto ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS media_movil_vistas
FROM por_minuto
ORDER BY minuto;


-- =====================================================================
-- Q9. Que marcas venden por encima del ticket promedio del negocio?
--
-- Resultado: 5 marcas sobre el promedio general de S/ 408. Apple va
-- primera con un ticket de S/ 924, mas del doble.
--
-- Lectura: estas marcas levantan el ticket promedio del sitio.
-- Empujarlas en recomendaciones sube el ingreso por transaccion sin
-- tener que traer mas trafico, que es la palanca mas cara. El promedio
-- general se calcula una sola vez en el CROSS JOIN y no dentro del
-- SELECT, para no repetir la subconsulta en cada columna.
-- =====================================================================
SELECT
    m.brand_name AS marca,
    COUNT(*) AS compras,
    ROUND(AVG(e.price), 2) AS ticket_promedio,
    ROUND(g.ticket_general, 2) AS ticket_general,
    ROUND(AVG(e.price) - g.ticket_general, 2) AS diferencia,
    CASE
        WHEN AVG(e.price) >= 2.0 * g.ticket_general THEN 'premium'
        WHEN AVG(e.price) >= 1.5 * g.ticket_general THEN 'medio alto'
        ELSE 'sobre el promedio'
    END AS segmento,
    ROUND(SUM(e.price), 2) AS ingreso
FROM eventos e
INNER JOIN productos p ON p.product_id = e.product_id
INNER JOIN marcas m ON m.brand_id = p.brand_id
CROSS JOIN (
    SELECT AVG(price) AS ticket_general
    FROM eventos
    WHERE event_type = 'purchase'
) g
WHERE e.event_type = 'purchase'
GROUP BY m.brand_name, g.ticket_general
HAVING AVG(e.price) > g.ticket_general
ORDER BY ticket_promedio DESC;


-- =====================================================================
-- Q10. Que camino recorre el usuario antes de comprar?
--
-- Resultado: el paso a paso de las sesiones que terminaron en compra,
-- con los segundos que pasaron entre un evento y el siguiente. Se ven
-- dos patrones claros: compras casi inmediatas tras una o dos vistas
-- del mismo producto, y compras pensadas donde el usuario compara
-- varios modelos y pasa por el carrito antes de cerrar.
--
-- Lectura: sirve para saber cuantos productos ve alguien antes de
-- decidirse y si la compra es impulsiva o meditada. Es la base para
-- construir features de comportamiento y tambien para detectar sesiones
-- raras (compras instantaneas sin navegacion previa, posibles bots).
-- =====================================================================
WITH secuencia AS (
    SELECT
        user_session,
        event_time,
        event_type,
        product_id,
        price,
        ROW_NUMBER() OVER (PARTITION BY user_session ORDER BY event_time, event_id) AS paso,
        LAG(event_time) OVER (PARTITION BY user_session ORDER BY event_time, event_id) AS anterior,
        LEAD(event_type) OVER (PARTITION BY user_session ORDER BY event_time, event_id) AS siguiente
    FROM eventos
    WHERE user_session IN (
        SELECT user_session FROM eventos WHERE event_type = 'purchase'
    )
)
SELECT
    user_session,
    paso,
    event_type AS accion,
    product_id,
    price,
    ROUND(EXTRACT(EPOCH FROM (event_time - anterior)))::int AS segundos,
    COALESCE(siguiente, 'fin de sesion') AS siguiente_accion,
    CASE
        WHEN anterior IS NULL THEN 'inicio'
        WHEN EXTRACT(EPOCH FROM (event_time - anterior)) <= 10 THEN 'impulsivo'
        WHEN EXTRACT(EPOCH FROM (event_time - anterior)) <= 60 THEN 'normal'
        ELSE 'pensado'
    END AS ritmo
FROM secuencia
ORDER BY user_session, paso
LIMIT 40;
