-- ===========================================================================
-- 01 - Esquemas y capa RAW (landing del Excel, fiel al origen)
-- Proyecto: Plataforma de datos CORPORACION PERUVIANS SUR S.A.C.
-- ===========================================================================
SET client_encoding = 'UTF8';

CREATE SCHEMA IF NOT EXISTS raw;        -- landing crudo, 1:1 con el archivo fuente
CREATE SCHEMA IF NOT EXISTS core;       -- modelo dimensional normalizado (fuente de verdad)
CREATE SCHEMA IF NOT EXISTS stream;     -- event stream de e-commerce (append-only)
CREATE SCHEMA IF NOT EXISTS mart;       -- vistas analiticas / reportes de negocio
CREATE SCHEMA IF NOT EXISTS audit;      -- control de cargas y calidad de datos

COMMENT ON SCHEMA raw    IS 'Landing zone: copia fiel del Excel origen, sin transformaciones.';
COMMENT ON SCHEMA core   IS 'Modelo dimensional (estrella) con dimensiones conformadas y hechos.';
COMMENT ON SCHEMA stream IS 'Event stream de e-commerce, append-only, particionado por mes.';
COMMENT ON SCHEMA mart   IS 'Vistas de negocio: replican y extienden los reportes del Excel.';
COMMENT ON SCHEMA audit  IS 'Metadatos de ejecucion: cargas, watermarks y checks de calidad.';

-- ---------------------------------------------------------------------------
-- RAW: hoja "Base de datos" tal como viene (todas las columnas como texto
-- cuando corresponde, valores calculados ya resueltos por el lector de Excel)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.ventas_excel;
CREATE TABLE raw.ventas_excel (
    raw_id                  bigserial PRIMARY KEY,
    fila_excel              integer      NOT NULL,
    nombre_cliente          text,
    tipo                    text,
    pais                    text,
    ciudad                  text,
    producto_vendido        text,
    categoria_producto      text,
    sucursal                text,
    vendedor_responsable    text,
    cantidad_vendida        numeric(18,4),
    precio_unitario_pen     numeric(18,4),
    metodo_pago             text,
    subtotal                numeric(18,4),
    igv_18                  numeric(18,4),
    utilidad                numeric(18,4),
    costo_unitario          numeric(18,4),
    costo_total             numeric(18,4),
    total_de_venta          numeric(18,4),
    venta_total_usd         numeric(18,6),
    ganancia                numeric(18,4),
    estado                  text,
    archivo_origen          text         NOT NULL,
    hoja_origen             text         NOT NULL,
    hash_fila               char(64)     NOT NULL,
    ingestion_ts            timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_raw_ventas_excel UNIQUE (archivo_origen, hoja_origen, fila_excel)
);
COMMENT ON TABLE  raw.ventas_excel IS 'Landing 1:1 de la hoja "Base de datos" (30 registros, filas 4..33 del Excel).';
COMMENT ON COLUMN raw.ventas_excel.hash_fila IS 'SHA-256 del contenido de la fila: permite detectar cambios en recargas.';

-- ---------------------------------------------------------------------------
-- RAW: parametros de negocio que en el Excel viven en celdas sueltas (fila 2)
-- OJO: en el Excel los rotulos de N1:P1 estan desplazados una celda a la
-- izquierda respecto a los valores que realmente usan las formulas.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.parametros_excel;
CREATE TABLE raw.parametros_excel (
    clave        text PRIMARY KEY,
    valor        numeric(18,6) NOT NULL,
    celda        text          NOT NULL,
    rotulo_excel text,
    descripcion  text,
    ingestion_ts timestamptz   NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.parametros_excel IS 'Parametros de calculo del Excel (IGV, margen, factor costo, tipo de cambio).';

-- ---------------------------------------------------------------------------
-- RAW: hojas de reportes del Excel (trazabilidad de los resultados esperados)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.reportes_excel;
CREATE TABLE raw.reportes_excel (
    raw_id       bigserial PRIMARY KEY,
    hoja         text NOT NULL,
    reporte      text,
    descripcion  text,
    funcion      text,
    resultado    text,
    fila_excel   integer,
    ingestion_ts timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.reportes_excel IS 'Resultados declarados en las hojas de reportes del Excel: se usan como baseline de validacion.';

-- ---------------------------------------------------------------------------
-- AUDIT: control de ejecuciones y calidad
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.carga_log (
    carga_id      bigserial PRIMARY KEY,
    proceso       text        NOT NULL,
    objeto        text        NOT NULL,
    filas_leidas  bigint,
    filas_escritas bigint,
    estado        text        NOT NULL,
    mensaje       text,
    inicio_ts     timestamptz NOT NULL DEFAULT now(),
    fin_ts        timestamptz
);
COMMENT ON TABLE audit.carga_log IS 'Bitacora de cargas (ETL Excel -> Postgres, generacion de stream, exports).';

CREATE TABLE IF NOT EXISTS audit.data_quality_result (
    check_id     bigserial PRIMARY KEY,
    ejecutado_ts timestamptz NOT NULL DEFAULT now(),
    capa         text NOT NULL,
    objeto       text NOT NULL,
    nombre_check text NOT NULL,
    severidad    text NOT NULL CHECK (severidad IN ('ERROR','WARN','INFO')),
    valor_obs    text,
    valor_esp    text,
    resultado    text NOT NULL CHECK (resultado IN ('PASS','FAIL'))
);
COMMENT ON TABLE audit.data_quality_result IS 'Resultado de los checks de calidad ejecutados sobre raw/core/stream.';

CREATE TABLE IF NOT EXISTS audit.watermark_ingesta (
    objeto_origen   text PRIMARY KEY,
    ultimo_valor_ts timestamptz,
    ultimo_valor_id bigint,
    ultima_corrida  timestamptz NOT NULL DEFAULT now(),
    filas_entregadas bigint DEFAULT 0
);
COMMENT ON TABLE audit.watermark_ingesta IS 'Watermark para ingesta incremental hacia Bronze (Databricks).';
