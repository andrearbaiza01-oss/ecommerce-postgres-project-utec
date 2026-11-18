# -*- coding: utf-8 -*-
"""
ETL 1/3 - Excel -> Postgres (capa RAW y capa CORE).

- Lee la hoja "Base de datos" (filas 4..33 = 30 registros) de forma posicional,
  porque los rotulos de parametros del Excel estan desalineados una celda.
- Carga RAW 1:1 (incluye hash SHA-256 por fila para deteccion de cambios).
- Deriva el modelo estrella CORE (dimensiones conformadas + hecho de ventas).
- Todo el proceso es idempotente: se puede volver a ejecutar sin duplicar.
"""
from __future__ import annotations

import hashlib
import sys
import unicodedata
from decimal import Decimal
from pathlib import Path

import openpyxl
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import ENV, RAIZ, get_conn, log_carga  # noqa: E402

HOJA_DATOS = "Base de datos"
FILA_HEADER = 3
FILA_INICIO = 4
FILA_FIN = 33  # fila 34 = TOTALES, se excluye

COLUMNAS = [  # posicion (1-based) -> nombre logico
    (1, "nombre_cliente"), (2, "tipo"), (3, "pais"), (4, "ciudad"),
    (5, "producto_vendido"), (6, "categoria_producto"), (7, "sucursal"),
    (8, "vendedor_responsable"), (9, "cantidad_vendida"), (10, "precio_unitario_pen"),
    (11, "metodo_pago"), (12, "subtotal"), (13, "igv_18"), (14, "utilidad"),
    (15, "costo_unitario"), (16, "costo_total"), (17, "total_de_venta"),
    (18, "venta_total_usd"), (19, "ganancia"), (20, "estado"),
]

# Parametros: los valores reales que usan las formulas (fila 2)
PARAMETROS = [
    ("IGV",             "M2", "(sin rotulo: el rotulo 'Margen Util.' esta en N1)",
     "Tasa IGV aplicada al subtotal  =L*$M$2"),
    ("MARGEN_UTILIDAD", "N2", "Margen Util.",  "Margen de utilidad sobre subtotal  =L*$N$2"),
    ("FACTOR_COSTO",    "O2", "Factor Costo",  "Costo unitario = precio * factor  =J*$O$2"),
    ("TIPO_CAMBIO_USD", "P2", "TC (S/. a $)",  "Conversion a dolares  =Q/$P$2"),
]

SUCURSAL_META = {
    "Puno":     ("Puno", "Sur"),
    "Arequipa": ("Arequipa", "Sur"),
    "Juliaca":  ("Puno", "Sur"),
}
METODO_PAGO_META = {  # nombre -> (instrumento, es_digital)
    "Yape":          ("billetera_digital", True),
    "Transferencia": ("bancario", True),
    "Depósito":      ("bancario", False),
    "Deposito":      ("bancario", False),
    "Tarjeta":       ("tarjeta", False),
    "Efectivo":      ("efectivo", False),
}


def sin_tildes(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", texto)
                   if unicodedata.category(c) != "Mn")


def hash_fila(valores: list) -> str:
    crudo = "|".join("" if v is None else str(v) for v in valores)
    return hashlib.sha256(crudo.encode("utf-8")).hexdigest()


def leer_excel(ruta: Path) -> tuple[list[dict], list[tuple], list[tuple]]:
    wb = openpyxl.load_workbook(ruta, data_only=True)
    ws = wb[HOJA_DATOS]

    filas: list[dict] = []
    for r in range(FILA_INICIO, FILA_FIN + 1):
        reg = {"fila_excel": r}
        crudos = []
        for col, nombre in COLUMNAS:
            v = ws.cell(row=r, column=col).value
            if isinstance(v, str):
                v = v.strip()
            reg[nombre] = v
            crudos.append(v)
        if reg["nombre_cliente"] is None:
            continue
        reg["hash_fila"] = hash_fila(crudos)
        filas.append(reg)

    params = []
    for clave, celda, rotulo, desc in PARAMETROS:
        valor = ws[celda].value
        params.append((clave, Decimal(str(valor)), celda, rotulo, desc))

    reportes = []
    for hoja in wb.sheetnames:
        if not hoja.lower().startswith("reportes"):
            continue
        wsr = wb[hoja]
        titulo_actual = None
        for i, fila in enumerate(wsr.iter_rows(values_only=True), start=1):
            vals = [None if v is None else (v.strip() if isinstance(v, str) else v) for v in fila]
            if not any(v is not None for v in vals):
                continue
            c0 = vals[0]
            c1 = vals[1] if len(vals) > 1 else None
            c2 = vals[2] if len(vals) > 2 else None
            if isinstance(c0, str) and c0.upper().startswith("REPORTE") and c1 is None:
                titulo_actual = c0
                continue
            if c2 is None:
                continue
            reportes.append((hoja, titulo_actual, str(c0), None if c1 is None else str(c1),
                             str(c2), i))
    return filas, params, reportes


def cargar_raw(conn, filas, params, reportes, archivo: str) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.ventas_excel RESTART IDENTITY;")
        execute_values(cur, """
            INSERT INTO raw.ventas_excel (
                fila_excel, nombre_cliente, tipo, pais, ciudad, producto_vendido,
                categoria_producto, sucursal, vendedor_responsable, cantidad_vendida,
                precio_unitario_pen, metodo_pago, subtotal, igv_18, utilidad,
                costo_unitario, costo_total, total_de_venta, venta_total_usd,
                ganancia, estado, archivo_origen, hoja_origen, hash_fila)
            VALUES %s""",
            [(f["fila_excel"], f["nombre_cliente"], f["tipo"], f["pais"], f["ciudad"],
              f["producto_vendido"], f["categoria_producto"], f["sucursal"],
              f["vendedor_responsable"], f["cantidad_vendida"], f["precio_unitario_pen"],
              f["metodo_pago"], f["subtotal"], f["igv_18"], f["utilidad"],
              f["costo_unitario"], f["costo_total"], f["total_de_venta"],
              f["venta_total_usd"], f["ganancia"], f["estado"],
              archivo, HOJA_DATOS, f["hash_fila"]) for f in filas])

        cur.execute("TRUNCATE raw.parametros_excel;")
        execute_values(cur, """INSERT INTO raw.parametros_excel
                                 (clave, valor, celda, rotulo_excel, descripcion)
                               VALUES %s""", params)

        cur.execute("TRUNCATE raw.reportes_excel RESTART IDENTITY;")
        if reportes:
            execute_values(cur, """INSERT INTO raw.reportes_excel
                                     (hoja, reporte, descripcion, funcion, resultado, fila_excel)
                                   VALUES %s""", reportes)
    print(f"[raw] {len(filas)} ventas, {len(params)} parametros, {len(reportes)} lineas de reporte")


def construir_core(conn) -> int:
    # Las dimensiones se cargan con UPSERT sobre la clave natural, no con DELETE:
    # stream.evento tiene claves foraneas hacia ellas, por lo que un borrado
    # fallaria en la segunda corrida. Asi el ETL es idempotente y las surrogate
    # keys se mantienen estables entre recargas.
    with conn.cursor() as cur:
        cur.execute("TRUNCATE core.fact_venta RESTART IDENTITY;")

        # --- dim_sucursal
        cur.execute("SELECT DISTINCT sucursal FROM raw.ventas_excel ORDER BY 1;")
        sucursales = [r[0] for r in cur.fetchall()]
        execute_values(cur, """INSERT INTO core.dim_sucursal
                                 (sucursal_nk, nombre, departamento, macro_region) VALUES %s
                               ON CONFLICT (sucursal_nk) DO UPDATE
                                 SET nombre = EXCLUDED.nombre,
                                     departamento = EXCLUDED.departamento,
                                     macro_region = EXCLUDED.macro_region""",
                       [(sin_tildes(s).upper(), s, *SUCURSAL_META.get(s, (s, "Sur")))
                        for s in sucursales])

        # --- dim_metodo_pago
        cur.execute("SELECT DISTINCT metodo_pago FROM raw.ventas_excel ORDER BY 1;")
        metodos = [r[0] for r in cur.fetchall()]
        execute_values(cur, """INSERT INTO core.dim_metodo_pago
                                 (metodo_pago_nk, nombre, instrumento, es_digital) VALUES %s
                               ON CONFLICT (metodo_pago_nk) DO UPDATE
                                 SET nombre = EXCLUDED.nombre,
                                     instrumento = EXCLUDED.instrumento,
                                     es_digital = EXCLUDED.es_digital""",
                       [(sin_tildes(m).upper(), m, *METODO_PAGO_META.get(m, ("otro", False)))
                        for m in metodos])

        # --- dim_vendedor (ligado a la sucursal donde mas opera)
        cur.execute("""
            WITH v AS (
                SELECT vendedor_responsable AS vendedor, sucursal, COUNT(*) n,
                       ROW_NUMBER() OVER (PARTITION BY vendedor_responsable
                                          ORDER BY COUNT(*) DESC, sucursal) rn
                FROM raw.ventas_excel GROUP BY 1,2)
            INSERT INTO core.dim_vendedor (vendedor_nk, nombre, sucursal_sk)
            SELECT upper(translate(v.vendedor,'áéíóúÁÉÍÓÚñÑ ','aeiouAEIOUnN_')),
                   v.vendedor, s.sucursal_sk
            FROM v JOIN core.dim_sucursal s ON s.nombre = v.sucursal
            WHERE v.rn = 1
            ON CONFLICT (vendedor_nk) DO UPDATE
               SET nombre = EXCLUDED.nombre, sucursal_sk = EXCLUDED.sucursal_sk;""")

        # --- dim_producto (precio de lista = precio unitario del Excel)
        cur.execute("""
            INSERT INTO core.dim_producto (producto_nk, nombre_producto, categoria, precio_lista_pen)
            SELECT upper(translate(producto_vendido,'áéíóúÁÉÍÓÚñÑ ()','aeiouAEIOUnN___')),
                   producto_vendido, MAX(categoria_producto), MAX(precio_unitario_pen)
            FROM raw.ventas_excel GROUP BY producto_vendido
            ON CONFLICT (producto_nk) DO UPDATE
               SET nombre_producto = EXCLUDED.nombre_producto,
                   categoria = EXCLUDED.categoria,
                   precio_lista_pen = EXCLUDED.precio_lista_pen;""")

        # --- dim_cliente (30 clientes reales)
        cur.execute("""
            INSERT INTO core.dim_cliente
                (cliente_nk, nombre_cliente, tipo_cliente, pais, ciudad, segmento,
                 origen_registro, es_cliente_real, fecha_alta)
            SELECT upper(translate(nombre_cliente,'áéíóúÁÉÍÓÚñÑ.&,- ','aeiouAEIOUnN_____')),
                   nombre_cliente,
                   CASE WHEN tipo ILIKE 'Empresa%' THEN 'Empresa' ELSE 'Persona natural' END,
                   pais, ciudad,
                   CASE WHEN tipo ILIKE 'Empresa%' THEN 'B2B' ELSE 'B2C' END,
                   'EXCEL', true, DATE '2025-01-01'
            FROM raw.ventas_excel
            ON CONFLICT (cliente_nk) DO UPDATE
               SET nombre_cliente = EXCLUDED.nombre_cliente,
                   tipo_cliente = EXCLUDED.tipo_cliente,
                   ciudad = EXCLUDED.ciudad,
                   segmento = EXCLUDED.segmento,
                   es_cliente_real = true;""")

        # --- fact_venta
        cur.execute("""
            INSERT INTO core.fact_venta
                (fila_excel, cliente_sk, producto_sk, sucursal_sk, vendedor_sk,
                 metodo_pago_sk, cantidad, precio_unitario_pen, hash_fila)
            SELECT r.fila_excel, c.cliente_sk, p.producto_sk, s.sucursal_sk,
                   v.vendedor_sk, m.metodo_pago_sk,
                   r.cantidad_vendida, r.precio_unitario_pen, r.hash_fila
            FROM raw.ventas_excel r
            JOIN core.dim_cliente     c ON c.nombre_cliente = r.nombre_cliente
            JOIN core.dim_producto    p ON p.nombre_producto = r.producto_vendido
            JOIN core.dim_sucursal    s ON s.nombre = r.sucursal
            JOIN core.dim_vendedor    v ON v.nombre = r.vendedor_responsable
            JOIN core.dim_metodo_pago m ON m.nombre = r.metodo_pago;""")
        insertadas = cur.rowcount

        # Parametros de negocio sincronizados con el Excel
        cur.execute("""
            INSERT INTO core.parametro_negocio (clave, valor, unidad, descripcion)
            SELECT clave, valor, 'ratio', descripcion FROM raw.parametros_excel
            ON CONFLICT (clave, vigente_desde) DO UPDATE SET valor = EXCLUDED.valor;""")
    print(f"[core] fact_venta: {insertadas} filas")
    return insertadas


def main() -> int:
    nombre = ENV.get("EXCEL_PATH") or ""
    ruta = (RAIZ / nombre) if nombre else None
    if ruta is None or not ruta.exists():
        candidatos = sorted(RAIZ.glob("*.xlsx"))
        if not candidatos:
            raise SystemExit("[etl] No se encontro ningun .xlsx en la raiz del proyecto.")
        ruta = candidatos[0]
    print(f"[etl] Leyendo: {ruta.name}")

    filas, params, reportes = leer_excel(ruta)
    with get_conn() as conn:
        cargar_raw(conn, filas, params, reportes, ruta.name)
        n = construir_core(conn)
    log_carga("10_cargar_excel", "raw+core", len(filas), n)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM mart.v_kpi_global;")
        cols = [d[0] for d in cur.description]
        print("\n[kpi] " + " | ".join(f"{c}={v}" for c, v in zip(cols, cur.fetchone())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
