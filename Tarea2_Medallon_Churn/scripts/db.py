"""Utilidades de conexion y bitacora para la plataforma de datos (Tarea 2)."""
from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

RAIZ = Path(__file__).resolve().parents[1]


def cargar_env(archivo: Path | None = None) -> dict:
    """Lee el .env sin dependencias externas (clave=valor, ignora comentarios)."""
    archivo = archivo or (RAIZ / ".env")
    valores: dict[str, str] = {}
    if archivo.exists():
        for linea in archivo.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            k, v = linea.split("=", 1)
            valores[k.strip()] = v.strip()
    # las variables de entorno del sistema tienen prioridad
    for k in list(valores):
        if os.environ.get(k):
            valores[k] = os.environ[k]
    return valores


ENV = cargar_env()


def conn_params() -> dict:
    return dict(
        host=ENV.get("POSTGRES_HOST", "localhost"),
        port=int(ENV.get("POSTGRES_PORT", "5435")),
        dbname=ENV.get("POSTGRES_DB", "ecommerce_dw"),
        user=ENV.get("POSTGRES_USER", "ecom"),
        password=ENV.get("POSTGRES_PASSWORD", "ecom_2026"),
    )


def sqlalchemy_url() -> str:
    p = conn_params()
    return f"postgresql+psycopg2://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['dbname']}"


@contextmanager
def get_conn(autocommit: bool = False):
    conn = psycopg2.connect(**conn_params())
    conn.autocommit = autocommit
    try:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8'; SET search_path TO public;")
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def esperar_postgres(intentos: int = 30, espera: float = 2.0) -> None:
    """Bloquea hasta que Postgres acepte conexiones (util tras docker compose up)."""
    for i in range(1, intentos + 1):
        try:
            with get_conn(autocommit=True):
                print(f"[db] Postgres disponible en {conn_params()['host']}:{conn_params()['port']}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"[db] intento {i}/{intentos}: {exc.__class__.__name__}; reintentando...")
            time.sleep(espera)
    raise SystemExit("[db] No se pudo conectar a Postgres. Revisa 'docker compose ps'.")


def ejecutar_script(ruta: Path) -> None:
    sql = ruta.read_text(encoding="utf-8")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
    print(f"[db] ejecutado {ruta.name}")


def log_carga(proceso: str, objeto: str, leidas: int, escritas: int,
              estado: str = "OK", mensaje: str | None = None) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO audit.carga_log
                   (proceso, objeto, filas_leidas, filas_escritas, estado, mensaje, fin_ts)
               VALUES (%s,%s,%s,%s,%s,%s, now())""",
            (proceso, objeto, leidas, escritas, estado, mensaje),
        )


def consulta(sql: str, params: tuple | None = None) -> list[dict]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


if __name__ == "__main__":
    esperar_postgres()
    print(consulta("SELECT version() AS v")[0]["v"])
    sys.exit(0)
