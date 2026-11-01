-- Trabajo Final - Base de Datos (UTEC)
-- Parte 1, seccion 3: features para Machine Learning
--
-- Esquema: analisis_ecommerce_produccion
-- Datos:   eventos de un e-commerce (dataset REES46 de Kaggle),
--          2019-11-01 entre 00:00 y 00:31 (4,083 eventos, 1,012 usuarios)
--
-- Las seis features se calculan por usuario (una fila por user_id).
-- Juntas forman un dataset para un modelo de propension de compra:
-- predecir si un usuario va a comprar a partir de como navega.
--
-- F1 vistas_usuario              intensidad de navegacion
-- F2 productos_distintos_vistos  exploracion frente a foco
-- F3 precio_promedio_visto       segmento de precio que busca
-- F4 ratio_carrito_vistas        intencion de compra
-- F5 minutos_activo              duracion de la actividad
-- F6 concentracion_marca         lealtad a una marca
--
-- Al final, D1 une las seis features con la variable objetivo (compro)
-- en una sola tabla, y D2 compara compradores con no compradores.
--
-- Nota sobre los datos: la ventana es de 32 minutos, por eso las
-- features no usan periodos como "ultimos 30 dias" sino la actividad
-- dentro de la ventana. Las tablas dispositivos y usuarios no traen
-- datos utiles (dispositivos esta vacia y usuarios no tiene nombre ni
-- fecha de registro), asi que todas las features salen de eventos.

SET search_path TO analisis_ecommerce_produccion;


-- =====================================================================
-- F1. vistas_usuario
--
-- Definicion: numero de productos vistos (eventos 'view') por el
-- usuario durante la ventana.
--
-- Justificacion: mide cuanto navega el usuario. Quien ve mas productos
-- esta mas comprometido con la tienda y tiene mas oportunidades de
-- encontrar algo que comprar.
--
-- Uso en IA: variable de entrada de un modelo de propension de compra
-- y de segmentacion (clustering) de usuarios por nivel de actividad.
--
-- Resultado: el usuario mas activo vio 44 productos en 32 minutos. En
-- promedio, los compradores ven 5.7 productos y los no compradores 3.9.
-- =====================================================================
SELECT
    user_id,
    COUNT(*) FILTER (WHERE event_type = 'view') AS vistas_usuario
FROM eventos
GROUP BY user_id
ORDER BY vistas_usuario DESC
LIMIT 10;


-- =====================================================================
-- F2. productos_distintos_vistos
--
-- Definicion: cantidad de productos diferentes que vio el usuario.
--
-- Justificacion: distingue dos comportamientos con las mismas vistas.
-- Quien ve 10 veces el mismo producto esta decidiendo una compra; quien
-- ve 10 productos distintos esta explorando. Comparada con F1 indica
-- si el usuario ya tiene un producto en mente.
--
-- Uso en IA: propension de compra y sistemas de recomendacion (a un
-- usuario explorador conviene mostrarle variedad; a uno enfocado,
-- alternativas del mismo producto).
--
-- Resultado: hay usuarios con 25 vistas y 25 productos distintos (solo
-- exploran) y compradores con 11 vistas de un solo producto.
-- =====================================================================
SELECT
    user_id,
    COUNT(DISTINCT product_id) FILTER (WHERE event_type = 'view') AS productos_distintos_vistos
FROM eventos
GROUP BY user_id
ORDER BY productos_distintos_vistos DESC
LIMIT 10;


-- =====================================================================
-- F3. precio_promedio_visto
--
-- Definicion: precio promedio de los productos que vio el usuario.
--
-- Justificacion: aproxima el segmento de precio y el poder adquisitivo
-- del usuario. Quien mira productos de S/ 1,000 no responde igual a
-- una oferta que quien mira productos de S/ 50.
--
-- Uso en IA: segmentacion de clientes, recomendaciones dentro del
-- rango de precio del usuario y personalizacion de descuentos.
--
-- Resultado: los usuarios que miran productos mas caros llegan a un
-- promedio de S/ 2,011. Los compradores miran en promedio productos de
-- S/ 376 y los no compradores de S/ 309.
-- =====================================================================
SELECT
    user_id,
    ROUND(AVG(price) FILTER (WHERE event_type = 'view'), 2) AS precio_promedio_visto
FROM eventos
GROUP BY user_id
HAVING COUNT(*) FILTER (WHERE event_type = 'view') > 0
ORDER BY precio_promedio_visto DESC
LIMIT 10;


-- =====================================================================
-- F4. ratio_carrito_vistas
--
-- Definicion: agregados al carrito divididos entre productos vistos.
--
-- Justificacion: es la señal de intencion mas fuerte antes de la
-- compra. Un ratio alto indica que el usuario no solo mira sino que
-- selecciona. NULLIF evita dividir entre cero en usuarios sin vistas.
--
-- Uso en IA: variable clave de la propension de compra y disparador de
-- acciones en tiempo real (por ejemplo, un recordatorio a quien agrego
-- al carrito y no compro).
--
-- Resultado: es la feature que mas separa a los grupos: 0.174 en
-- compradores frente a 0.009 en no compradores (unas 19 veces mas).
-- Algunos usuarios tienen ratio mayor que 1 porque agregaron al carrito
-- un producto que habian visto antes de las 00:00, fuera de la ventana.
-- =====================================================================
SELECT
    user_id,
    COUNT(*) FILTER (WHERE event_type = 'cart') AS carritos,
    COUNT(*) FILTER (WHERE event_type = 'view') AS vistas,
    ROUND(COUNT(*) FILTER (WHERE event_type = 'cart')::numeric
          / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0), 3) AS ratio_carrito_vistas
FROM eventos
GROUP BY user_id
HAVING COUNT(*) FILTER (WHERE event_type = 'cart') > 0
ORDER BY ratio_carrito_vistas DESC NULLS LAST
LIMIT 10;


-- =====================================================================
-- F5. minutos_activo
--
-- Definicion: minutos entre el primer y el ultimo evento del usuario.
--
-- Justificacion: mide cuanto tiempo dedica el usuario a la tienda. Un
-- usuario que se queda varios minutos esta evaluando opciones; uno con
-- 0 minutos hizo una sola accion y se fue (rebote).
--
-- Uso en IA: propension de compra y deteccion de rebote. En un modelo
-- de fraude tambien sirve: muchas acciones en muy poco tiempo son
-- tipicas de un bot.
--
-- Resultado: los compradores pasan 7.9 minutos activos en promedio y
-- los no compradores 3.3. Varios usuarios estuvieron activos casi toda
-- la ventana (31 minutos).
-- =====================================================================
SELECT
    user_id,
    MIN(event_time) AS primer_evento,
    MAX(event_time) AS ultimo_evento,
    ROUND(EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time)) / 60, 2) AS minutos_activo
FROM eventos
GROUP BY user_id
ORDER BY minutos_activo DESC
LIMIT 10;


-- =====================================================================
-- F6. concentracion_marca
--
-- Definicion: proporcion de las vistas del usuario que corresponden a
-- su marca mas vista. Va de 0 a 1: 1 significa que solo miro una marca.
--
-- Justificacion: separa a los usuarios leales a una marca de los que
-- comparan entre marcas. Se calcula en dos pasos: primero las vistas
-- por usuario y marca (CTE), luego la marca con mas vistas sobre el
-- total del usuario.
--
-- Uso en IA: recomendacion (a un usuario leal se le recomiendan
-- productos de su marca) y segmentacion por lealtad.
--
-- Resultado: el promedio es 0.84 en los dos grupos, asi que no ayuda a
-- predecir la compra. Si sirve para recomendar: hay usuarios con 21
-- vistas de una sola marca.
-- =====================================================================
WITH vistas_por_marca AS (
    SELECT
        e.user_id,
        m.brand_name,
        COUNT(*) AS vistas
    FROM eventos e
    INNER JOIN productos p ON p.product_id = e.product_id
    INNER JOIN marcas m ON m.brand_id = p.brand_id
    WHERE e.event_type = 'view'
    GROUP BY e.user_id, m.brand_name
)
SELECT
    user_id,
    COUNT(*) AS marcas_vistas,
    SUM(vistas) AS vistas_totales,
    ROUND(MAX(vistas)::numeric / SUM(vistas), 2) AS concentracion_marca
FROM vistas_por_marca
GROUP BY user_id
HAVING SUM(vistas) >= 5
ORDER BY concentracion_marca DESC, vistas_totales DESC
LIMIT 10;


-- =====================================================================
-- D1. Dataset final: las seis features y la variable objetivo
--
-- Una fila por usuario, lista para entrenar un modelo. La variable
-- objetivo compro vale 1 si el usuario hizo al menos una compra.
-- Los usuarios sin vistas quedan con 0 o NULL en las features de
-- vistas, que es lo que el modelo deberia recibir.
-- =====================================================================
WITH actividad AS (
    SELECT
        user_id,
        COUNT(*) FILTER (WHERE event_type = 'view') AS vistas_usuario,
        COUNT(DISTINCT product_id) FILTER (WHERE event_type = 'view') AS productos_distintos_vistos,
        ROUND(AVG(price) FILTER (WHERE event_type = 'view'), 2) AS precio_promedio_visto,
        ROUND(COUNT(*) FILTER (WHERE event_type = 'cart')::numeric
              / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0), 3) AS ratio_carrito_vistas,
        ROUND(EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time)) / 60, 2) AS minutos_activo,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS compro
    FROM eventos
    GROUP BY user_id
),
vistas_por_marca AS (
    SELECT e.user_id, m.brand_name, COUNT(*) AS vistas
    FROM eventos e
    INNER JOIN productos p ON p.product_id = e.product_id
    INNER JOIN marcas m ON m.brand_id = p.brand_id
    WHERE e.event_type = 'view'
    GROUP BY e.user_id, m.brand_name
),
marca AS (
    SELECT user_id, ROUND(MAX(vistas)::numeric / SUM(vistas), 2) AS concentracion_marca
    FROM vistas_por_marca
    GROUP BY user_id
)
SELECT
    a.user_id,
    a.vistas_usuario,
    a.productos_distintos_vistos,
    a.precio_promedio_visto,
    a.ratio_carrito_vistas,
    a.minutos_activo,
    m.concentracion_marca,
    a.compro
FROM actividad a
LEFT JOIN marca m ON m.user_id = a.user_id
ORDER BY a.compro DESC, a.vistas_usuario DESC
LIMIT 15;


-- =====================================================================
-- D2. Compradores frente a no compradores
--
-- Promedio de cada feature en los dos grupos. Si una feature tiene
-- valores muy distintos entre quienes compran y quienes no, es una
-- buena candidata para predecir la compra.
--
-- Resultado: 39 compradores y 973 no compradores. El ratio de carrito
-- (0.174 frente a 0.009), los minutos activos (7.9 frente a 3.3) y las
-- vistas (5.7 frente a 3.9) separan bien a los grupos. La concentracion
-- de marca no (0.84 en ambos).
--
-- Lectura: con solo 39 compradores el dataset esta muy desbalanceado
-- (3.9% de positivos). Un modelo tendria que usar pesos por clase o
-- sobremuestreo, y evaluarse con recall o AUC, no con exactitud.
-- Ademas, en produccion las features deben calcularse solo con los
-- eventos anteriores a la compra, para no filtrar la respuesta.
-- =====================================================================
WITH actividad AS (
    SELECT
        user_id,
        COUNT(*) FILTER (WHERE event_type = 'view') AS vistas_usuario,
        COUNT(DISTINCT product_id) FILTER (WHERE event_type = 'view') AS productos_distintos_vistos,
        AVG(price) FILTER (WHERE event_type = 'view') AS precio_promedio_visto,
        COUNT(*) FILTER (WHERE event_type = 'cart')::numeric
            / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0) AS ratio_carrito_vistas,
        EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time)) / 60 AS minutos_activo,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS compro
    FROM eventos
    GROUP BY user_id
),
vistas_por_marca AS (
    SELECT e.user_id, m.brand_name, COUNT(*) AS vistas
    FROM eventos e
    INNER JOIN productos p ON p.product_id = e.product_id
    INNER JOIN marcas m ON m.brand_id = p.brand_id
    WHERE e.event_type = 'view'
    GROUP BY e.user_id, m.brand_name
),
marca AS (
    SELECT user_id, MAX(vistas)::numeric / SUM(vistas) AS concentracion_marca
    FROM vistas_por_marca
    GROUP BY user_id
)
SELECT
    CASE a.compro WHEN 1 THEN 'compradores' ELSE 'no compradores' END AS grupo,
    COUNT(*) AS usuarios,
    ROUND(AVG(a.vistas_usuario), 2) AS vistas_usuario,
    ROUND(AVG(a.productos_distintos_vistos), 2) AS productos_distintos_vistos,
    ROUND(AVG(a.precio_promedio_visto), 2) AS precio_promedio_visto,
    ROUND(AVG(a.ratio_carrito_vistas), 3) AS ratio_carrito_vistas,
    ROUND(AVG(a.minutos_activo), 2) AS minutos_activo,
    ROUND(AVG(m.concentracion_marca), 2) AS concentracion_marca
FROM actividad a
LEFT JOIN marca m ON m.user_id = a.user_id
GROUP BY a.compro
ORDER BY a.compro DESC;
