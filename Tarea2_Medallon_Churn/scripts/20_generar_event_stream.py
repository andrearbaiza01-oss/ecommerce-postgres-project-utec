# -*- coding: utf-8 -*-
"""
ETL 2/3 - Construccion del event stream de e-commerce en Postgres.

CONTEXTO IMPORTANTE (documentado tambien en el README):
El Excel origen tiene 30 filas, una sola operacion por cliente y NINGUNA marca
de tiempo. Con eso no es posible construir ni ingesta incremental ni un modelo
de churn (churn exige historia temporal por cliente). Por eso este script
genera un event stream REPRODUCIBLE (semilla fija) donde:

  * REAL (tomado del Excel): catalogo de productos, categorias, precios,
    sucursales, vendedores, metodos de pago, ciudades, mix de tipo de cliente
    y los 30 clientes originales (marcados es_cliente_real = true).
  * SIMULADO: marcas de tiempo, sesiones, clientes adicionales y la dinamica
    de comportamiento (frecuencia, friccion y abandono).

El mecanismo de abandono es latente: NO se escribe ninguna etiqueta de churn en
la base. La etiqueta se deriva despues en la capa Gold a partir de la
inactividad observada, igual que en un caso real.
"""
from __future__ import annotations

import io
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import ENV, get_conn, log_carga  # noqa: E402

SEMILLA = int(ENV.get("STREAM_SEED", "20261117"))
N_SINTETICOS = int(ENV.get("STREAM_CLIENTES_SINTETICOS", "1500"))
N_MESES = int(ENV.get("STREAM_MESES_HISTORIA", "18"))
FIN_HISTORIA = date(2026, 11, 1)          # exclusivo: ultimo mes completo observado

rng = np.random.default_rng(SEMILLA)

# ---------------------------------------------------------------------------
# Catalogos de comportamiento
# ---------------------------------------------------------------------------
CANALES = ["web", "app_movil", "marketplace", "whatsapp", "tienda"]
P_CANAL = [0.42, 0.28, 0.14, 0.10, 0.06]
DISPOSITIVO_POR_CANAL = {
    "web": (["desktop", "mobile", "tablet"], [0.62, 0.30, 0.08]),
    "app_movil": (["mobile", "tablet"], [0.92, 0.08]),
    "marketplace": (["desktop", "mobile"], [0.45, 0.55]),
    "whatsapp": (["mobile"], [1.0]),
    "tienda": (["pos"], [1.0]),
}
CAMPANIAS = [None, "remarketing_q1", "cyber_oficina", "black_friday_pe",
             "fidelizacion_b2b", "campania_escolar", "aniversario_sur"]
P_CAMPANIA = [0.46, 0.11, 0.10, 0.09, 0.10, 0.08, 0.06]
UTM = ["google_ads", "meta_ads", "organico", "email", "whatsapp_broadcast",
       "directo", "tiktok_ads"]
P_UTM = [0.22, 0.18, 0.21, 0.12, 0.09, 0.13, 0.05]

# Perfiles de comportamiento: frecuencia, conversion, riesgo de abandono, ticket
PERFILES = {
    "campeon":   dict(p=0.11, sesiones=3.1, conv=0.46, hazard=0.012, ticket=1.45, friccion=0.5),
    "leal":      dict(p=0.24, sesiones=2.0, conv=0.34, hazard=0.028, ticket=1.15, friccion=0.7),
    "ocasional": dict(p=0.34, sesiones=1.1, conv=0.21, hazard=0.055, ticket=0.90, friccion=1.0),
    "en_riesgo": dict(p=0.21, sesiones=0.8, conv=0.14, hazard=0.105, ticket=0.75, friccion=1.8),
    "dormido":   dict(p=0.10, sesiones=0.4, conv=0.07, hazard=0.170, ticket=0.60, friccion=2.3),
}
NOMBRES_PERFIL = list(PERFILES)
P_PERFIL = [PERFILES[k]["p"] for k in NOMBRES_PERFIL]

NOMBRES = ["Maria", "Jose", "Luis", "Ana", "Carlos", "Rosa", "Miguel", "Carmen",
           "Jorge", "Lucia", "Pedro", "Elena", "Diego", "Valeria", "Andres",
           "Camila", "Fernando", "Daniela", "Renato", "Milagros", "Cesar",
           "Patricia", "Victor", "Sandra", "Raul", "Gabriela", "Hugo", "Yesenia"]
APELLIDOS = ["Quispe", "Mamani", "Flores", "Apaza", "Choque", "Condori", "Torres",
             "Salas", "Mendoza", "Paredes", "Huaman", "Cardenas", "Lopez",
             "Gutierrez", "Rojas", "Vilca", "Pacheco", "Calderon", "Ramos", "Nina"]
EMPRESA_PRE = ["Comercial", "Inversiones", "Distribuidora", "Servicios", "Corporacion",
               "Consultora", "Importaciones", "Soluciones", "Grupo", "Logistica",
               "Almacenes", "Ferreteria", "Gestion", "Asesoria", "Comercializadora"]
EMPRESA_MED = ["del Sur", "Altiplano", "Andina", "Titicaca", "del Pacifico", "Austral",
               "Integral", "Moderna", "Global", "Regional", "Misti", "Sillustani"]
EMPRESA_SUF = ["S.A.C.", "E.I.R.L.", "S.R.L.", "S.A.", ""]


def uuid_rng() -> uuid.UUID:
    """UUID v4 derivado del generador con semilla: reproducible entre corridas."""
    return uuid.UUID(bytes=bytes(rng.bytes(16)), version=4)


def elegir(opciones, probs=None):
    idx = rng.choice(len(opciones), p=probs)
    return opciones[idx]


def generar_nombre_empresa(usados: set) -> str:
    for _ in range(60):
        nombre = f"{elegir(EMPRESA_PRE)} {elegir(EMPRESA_MED)} {elegir(EMPRESA_SUF)}".strip()
        nombre = " ".join(nombre.split())
        if nombre not in usados:
            usados.add(nombre)
            return nombre
    nombre = f"{elegir(EMPRESA_PRE)} {rng.integers(1000, 9999)} S.A.C."
    usados.add(nombre)
    return nombre


def generar_nombre_persona(usados: set) -> str:
    for _ in range(60):
        nombre = f"{elegir(NOMBRES)} {elegir(APELLIDOS)} {elegir(APELLIDOS)}"
        if nombre not in usados:
            usados.add(nombre)
            return nombre
    nombre = f"{elegir(NOMBRES)} {elegir(APELLIDOS)} {rng.integers(100, 999)}"
    usados.add(nombre)
    return nombre


def clave_natural(nombre: str) -> str:
    tabla = str.maketrans("áéíóúÁÉÍÓÚñÑ.&,- ", "aeiouAEIOUnN_____")
    return nombre.translate(tabla).upper()[:120]


def mes_inicio(offset: int) -> date:
    """Primer dia del mes situado 'offset' meses despues del inicio de la historia."""
    base_m = FIN_HISTORIA.month - 1 - N_MESES + offset
    anio = FIN_HISTORIA.year + base_m // 12
    mes = base_m % 12 + 1
    return date(anio, mes, 1)


def ts_en_mes(offset: int) -> datetime:
    """Timestamp aleatorio dentro del mes, con sesgo a horario comercial."""
    ini = mes_inicio(offset)
    fin = mes_inicio(offset + 1)
    dias = (fin - ini).days
    dia = int(rng.integers(0, dias))
    # horario comercial: mezcla de dos normales (manana / tarde)
    hora = float(rng.normal(11, 2.2)) if rng.random() < 0.55 else float(rng.normal(17.5, 2.6))
    hora = min(max(hora, 7.0), 23.4)
    h = int(hora)
    m = int((hora - h) * 60)
    s = int(rng.integers(0, 60))
    return datetime.combine(ini + timedelta(days=dia), datetime.min.time()).replace(
        hour=h, minute=m, second=s)


# ---------------------------------------------------------------------------
# 1) Clientes: reales (Excel) + sinteticos
# ---------------------------------------------------------------------------
def preparar_clientes(conn) -> list[dict]:
    with conn.cursor() as cur:
        # El stream referencia a los clientes por FK: hay que vaciarlo antes de
        # rehacer la poblacion sintetica (asi el script es reejecutable).
        cur.execute("TRUNCATE stream.evento;")
        cur.execute("DELETE FROM core.dim_cliente WHERE NOT es_cliente_real;")
        cur.execute("""SELECT cliente_sk, nombre_cliente, tipo_cliente, ciudad
                       FROM core.dim_cliente WHERE es_cliente_real ORDER BY cliente_sk;""")
        reales = [dict(cliente_sk=r[0], nombre=r[1], tipo=r[2], ciudad=r[3]) for r in cur.fetchall()]
        if not reales:
            raise SystemExit("[stream] No hay clientes reales. Ejecuta primero 10_cargar_excel.py")

        cur.execute("SELECT DISTINCT ciudad FROM core.dim_cliente WHERE es_cliente_real;")
        ciudades = [r[0] for r in cur.fetchall()]
        cur.execute("""SELECT tipo_cliente, COUNT(*)::float / SUM(COUNT(*)) OVER ()
                       FROM core.dim_cliente WHERE es_cliente_real GROUP BY 1 ORDER BY 1;""")
        mix = cur.fetchall()
        tipos = [m[0] for m in mix]
        p_tipos = [float(m[1]) for m in mix]

        usados = {c["nombre"] for c in reales}
        nuevos = []
        for _ in range(N_SINTETICOS):
            tipo = elegir(tipos, p_tipos)
            nombre = generar_nombre_empresa(usados) if tipo == "Empresa" else generar_nombre_persona(usados)
            ciudad = elegir(ciudades)
            nuevos.append((clave_natural(nombre), nombre, tipo, "Perú", ciudad,
                           "B2B" if tipo == "Empresa" else "B2C",
                           "STREAM_SINTETICO", False,
                           mes_inicio(int(rng.integers(0, max(N_MESES - 1, 1))))))
        execute_values(cur, """
            INSERT INTO core.dim_cliente
                (cliente_nk, nombre_cliente, tipo_cliente, pais, ciudad, segmento,
                 origen_registro, es_cliente_real, fecha_alta)
            VALUES %s ON CONFLICT (cliente_nk) DO NOTHING""", nuevos)

        cur.execute("""SELECT cliente_sk, nombre_cliente, tipo_cliente, ciudad, es_cliente_real
                       FROM core.dim_cliente ORDER BY cliente_sk;""")
        clientes = [dict(cliente_sk=r[0], nombre=r[1], tipo=r[2], ciudad=r[3], real=r[4])
                    for r in cur.fetchall()]
    print(f"[stream] clientes: {len(clientes)} ({len(reales)} reales + {len(clientes)-len(reales)} sinteticos)")
    return clientes


def cargar_catalogos(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""SELECT producto_sk, nombre_producto, categoria, precio_lista_pen
                       FROM core.dim_producto ORDER BY producto_sk;""")
        productos = [dict(sk=r[0], nombre=r[1], categoria=r[2], precio=float(r[3]))
                     for r in cur.fetchall()]
        cur.execute("SELECT metodo_pago_sk, nombre FROM core.dim_metodo_pago ORDER BY 1;")
        metodos = [dict(sk=r[0], nombre=r[1]) for r in cur.fetchall()]
        cur.execute("SELECT sucursal_sk, nombre FROM core.dim_sucursal ORDER BY 1;")
        sucursales = [dict(sk=r[0], nombre=r[1]) for r in cur.fetchall()]
        cur.execute("SELECT vendedor_sk, sucursal_sk FROM core.dim_vendedor ORDER BY 1;")
        vendedores = [dict(sk=r[0], sucursal_sk=r[1]) for r in cur.fetchall()]
    categorias = sorted({p["categoria"] for p in productos})
    return dict(productos=productos, metodos=metodos, sucursales=sucursales,
                vendedores=vendedores, categorias=categorias)


# ---------------------------------------------------------------------------
# 2) Simulacion del stream
# ---------------------------------------------------------------------------
def perfilar_cliente(cli: dict, cat: dict) -> dict:
    perfil = elegir(NOMBRES_PERFIL, P_PERFIL)
    cfg = PERFILES[perfil]
    sucursal = elegir(cat["sucursales"])
    vends = [v for v in cat["vendedores"] if v["sucursal_sk"] == sucursal["sk"]] or cat["vendedores"]
    es_empresa = cli["tipo"] == "Empresa"
    # mes de alta y mes de abandono latente (hazard geometrico)
    alta = int(rng.integers(0, max(N_MESES - 2, 1)))
    hazard = cfg["hazard"] * (0.75 if es_empresa else 1.15)
    churn_mes = alta + int(rng.geometric(min(max(hazard, 0.005), 0.5)))
    return dict(
        cliente_sk=cli["cliente_sk"], perfil=perfil,
        sesiones=cfg["sesiones"] * (1.35 if es_empresa else 1.0),
        conv=min(cfg["conv"] * (1.25 if es_empresa else 1.0), 0.85),
        ticket=cfg["ticket"] * (1.8 if es_empresa else 1.0),
        friccion=cfg["friccion"],
        alta=alta, churn_mes=churn_mes,
        canal_pref=elegir(CANALES, P_CANAL),
        metodo_pref=elegir(cat["metodos"]),
        sucursal=sucursal, vendedor=elegir(vends),
        cat_pref=elegir(cat["categorias"]),
        sensible_precio=float(rng.beta(2, 5)),
    )


def simular(clientes: list[dict], cat: dict) -> list[tuple]:
    productos = cat["productos"]
    prod_por_cat: dict[str, list] = {}
    for p in productos:
        prod_por_cat.setdefault(p["categoria"], []).append(p)

    filas: list[tuple] = []

    for cli in clientes:
        perf = perfilar_cliente(cli, cat)
        for mes in range(perf["alta"], N_MESES):
            desde_churn = mes - perf["churn_mes"]
            if desde_churn >= 0:
                # ya abandono: solo navegacion residual decreciente, sin compras
                lam = perf["sesiones"] * 0.22 * float(np.exp(-0.55 * desde_churn))
                conv = 0.0
                friccion = perf["friccion"] * 0.4
            else:
                # deterioro progresivo en los 3 meses previos al abandono
                prev = -desde_churn
                decay = {1: 0.45, 2: 0.65, 3: 0.85}.get(prev, 1.0)
                escalada = {1: 2.4, 2: 1.8, 3: 1.3}.get(prev, 1.0)
                lam = perf["sesiones"] * decay
                conv = perf["conv"] * decay
                friccion = perf["friccion"] * escalada
            # estacionalidad: noviembre/diciembre y campania escolar (marzo)
            mes_cal = mes_inicio(mes).month
            lam *= 1.35 if mes_cal in (11, 12) else (1.18 if mes_cal == 3 else 1.0)

            n_sesiones = int(rng.poisson(max(lam, 0.0)))
            for _ in range(n_sesiones):
                filas.extend(simular_sesion(perf, cat, prod_por_cat, mes, conv, friccion))
    return filas


def simular_sesion(perf, cat, prod_por_cat, mes, conv, friccion) -> list[tuple]:
    eventos = []
    session_id = uuid_rng()
    t = ts_en_mes(mes)
    canal = perf["canal_pref"] if rng.random() < 0.72 else elegir(CANALES, P_CANAL)
    disp_op, disp_p = DISPOSITIVO_POR_CANAL[canal]
    dispositivo = elegir(disp_op, disp_p)
    campania = elegir(CAMPANIAS, P_CAMPANIA)
    utm = elegir(UTM, P_UTM)
    duracion = int(max(30, rng.normal(420, 240)))

    def add(tipo, dt_seg, producto=None, cantidad=None, monto=None,
            order_id=None, descuento=0.0):
        ts = t + timedelta(seconds=dt_seg)
        eventos.append((
            str(uuid_rng()),
            ts.isoformat(sep=" "), ts.date().isoformat(), tipo, perf["cliente_sk"],
            str(session_id), canal, dispositivo,
            "" if producto is None else producto["sk"],
            "" if producto is None else producto["categoria"],
            "" if cantidad is None else cantidad,
            "" if producto is None else round(producto["precio"], 4),
            "" if monto is None else round(monto, 4),
            perf["metodo_pref"]["sk"] if tipo in ("checkout_start", "order_placed",
                                                  "payment_failed") else "",
            perf["sucursal"]["sk"], perf["vendedor"]["sk"],
            "" if order_id is None else str(order_id),
            "" if campania is None else campania, utm,
            round(descuento, 4), duracion, "ecommerce_web", "true",
        ))

    add("session_start", 0)
    n_vistas = int(min(max(1, rng.poisson(2.6)), 9))
    vistos = []
    for i in range(n_vistas):
        categoria = perf["cat_pref"] if rng.random() < 0.6 else elegir(cat["categorias"])
        prod = elegir(prod_por_cat[categoria])
        vistos.append(prod)
        add("product_view", 25 + i * int(rng.integers(20, 110)), producto=prod)
    if rng.random() < 0.33:
        add("search", 12 + int(rng.integers(5, 60)))

    if not vistos or rng.random() > min(conv * 2.3 + 0.08, 0.95):
        if rng.random() < 0.012 * friccion:
            add("newsletter_unsubscribe", duracion)
        return eventos

    prod = elegir(vistos)
    cantidad = int(min(max(1, rng.poisson(2.1)), 10))
    add("add_to_cart", 180, producto=prod, cantidad=cantidad)
    if rng.random() < 0.17:
        add("remove_from_cart", 220, producto=prod, cantidad=cantidad)

    if rng.random() > conv + 0.12:
        return eventos

    descuento = 0.0
    if rng.random() < 0.25 + perf["sensible_precio"] * 0.4:
        descuento = float(elegir([0.05, 0.10, 0.15, 0.20], [0.4, 0.3, 0.2, 0.1]))
    monto = cantidad * prod["precio"] * (1 - descuento) * float(rng.normal(perf["ticket"], 0.05))
    monto = max(monto, prod["precio"] * 0.5)
    add("checkout_start", 300, producto=prod, cantidad=cantidad, monto=monto, descuento=descuento)

    if rng.random() < 0.06 * friccion:
        add("payment_failed", 340, producto=prod, cantidad=cantidad, monto=monto, descuento=descuento)
        if rng.random() < 0.45:
            return eventos

    if rng.random() > conv + 0.35:
        return eventos

    order_id = uuid_rng()
    add("order_placed", 380, producto=prod, cantidad=cantidad, monto=monto,
        order_id=order_id, descuento=descuento)

    if rng.random() < 0.035 * friccion:
        add("order_cancelled", 3600 * 8, producto=prod, cantidad=cantidad,
            monto=monto, order_id=order_id, descuento=descuento)
    elif rng.random() < 0.028 * friccion:
        add("return_requested", 3600 * 24 * 6, producto=prod, cantidad=cantidad,
            monto=monto, order_id=order_id, descuento=descuento)
    if rng.random() < 0.08 * friccion:
        add("support_ticket", 3600 * 20, producto=prod, order_id=order_id)
    if rng.random() < 0.14:
        add("review_submitted", 3600 * 24 * 3, producto=prod, order_id=order_id)
    return eventos


COLUMNAS_COPY = """event_uuid, event_ts, event_date, event_type, cliente_sk, session_id,
                   canal, dispositivo, producto_sk, categoria, cantidad,
                   precio_unitario_pen, monto_pen, metodo_pago_sk, sucursal_sk,
                   vendedor_sk, order_id, campania, utm_source, descuento_pct,
                   tiempo_sesion_seg, source_system, es_sintetico"""


def copiar(conn, filas: list[tuple]) -> int:
    import csv
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    for f in filas:
        w.writerow(f)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY stream.evento ({COLUMNAS_COPY}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        cur.execute("SELECT COUNT(*) FROM stream.evento;")
        total = cur.fetchone()[0]
        cur.execute("ANALYZE stream.evento;")
        cur.execute("""
            INSERT INTO audit.watermark_ingesta (objeto_origen, ultimo_valor_ts, ultimo_valor_id, filas_entregadas)
            SELECT 'stream.evento', MAX(ingestion_ts), MAX(event_id), 0 FROM stream.evento
            ON CONFLICT (objeto_origen) DO UPDATE
               SET ultimo_valor_ts = EXCLUDED.ultimo_valor_ts,
                   ultimo_valor_id = EXCLUDED.ultimo_valor_id,
                   ultima_corrida  = now();""")
    return total


def main() -> int:
    print(f"[stream] semilla={SEMILLA} meses={N_MESES} fin_historia={FIN_HISTORIA} "
          f"ventana={mes_inicio(0)} .. {FIN_HISTORIA - timedelta(days=1)}")
    with get_conn() as conn:
        clientes = preparar_clientes(conn)
        cat = cargar_catalogos(conn)
        filas = simular(clientes, cat)
        print(f"[stream] eventos simulados: {len(filas):,}")
        total = copiar(conn, filas)
        print(f"[stream] eventos cargados en Postgres: {total:,}")
        with conn.cursor() as cur:
            cur.execute("""SELECT event_type, COUNT(*) FROM stream.evento
                           GROUP BY 1 ORDER BY 2 DESC;""")
            for t, n in cur.fetchall():
                print(f"         {t:<24} {n:>8,}")
            cur.execute("SELECT COUNT(DISTINCT cliente_sk), MIN(event_ts), MAX(event_ts) FROM stream.evento;")
            c, mn, mx = cur.fetchone()
            print(f"[stream] clientes con actividad: {c:,} | rango: {mn:%Y-%m-%d} .. {mx:%Y-%m-%d}")
    log_carga("20_generar_event_stream", "stream.evento", len(filas), total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
