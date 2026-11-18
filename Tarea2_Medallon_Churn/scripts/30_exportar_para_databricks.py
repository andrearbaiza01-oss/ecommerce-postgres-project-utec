# -*- coding: utf-8 -*-
"""
ETL 3/3 - Exporta los objetos de Postgres a archivos para subir a Databricks.

Se usa cuando Databricks (SaaS) no puede alcanzar el Postgres local por red.
Genera Parquet (recomendado) o CSV en data/export/ y un manifiesto JSON con
conteos de filas para validar la ingesta del lado de Databricks.

Uso:
    python scripts/30_exportar_para_databricks.py                # parquet
    python scripts/30_exportar_para_databricks.py --formato csv
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import RAIZ, log_carga, sqlalchemy_url  # noqa: E402

DESTINO = RAIZ / "data" / "export"

OBJETOS = {
    # nombre_archivo            : consulta
    "raw_ventas_excel":      "SELECT * FROM raw.ventas_excel",
    "raw_parametros_excel":  "SELECT * FROM raw.parametros_excel",
    "raw_reportes_excel":    "SELECT * FROM raw.reportes_excel",
    "core_dim_cliente":      "SELECT * FROM core.dim_cliente",
    "core_dim_producto":     "SELECT * FROM core.dim_producto",
    "core_dim_sucursal":     "SELECT * FROM core.dim_sucursal",
    "core_dim_vendedor":     "SELECT * FROM core.dim_vendedor",
    "core_dim_metodo_pago":  "SELECT * FROM core.dim_metodo_pago",
    "core_dim_fecha":        "SELECT * FROM core.dim_fecha WHERE fecha BETWEEN DATE '2025-01-01' AND DATE '2027-12-31'",
    "core_fact_venta":       "SELECT * FROM core.fact_venta",
    "core_parametro_negocio":"SELECT * FROM core.parametro_negocio",
    "stream_evento":         "SELECT * FROM stream.evento",
}


def normalizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """UUID/Decimal -> tipos serializables en Parquet/CSV sin perder precision."""
    import uuid as _uuid
    from decimal import Decimal as _Dec

    for col in df.columns:
        if df[col].dtype != "object":
            continue
        muestra = df[col].dropna()
        if muestra.empty:
            continue
        primero = muestra.iloc[0]
        if isinstance(primero, _uuid.UUID):
            df[col] = df[col].map(lambda v: None if v is None else str(v))
        elif isinstance(primero, _Dec):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--formato", choices=["parquet", "csv"], default="parquet")
    ap.add_argument("--destino", default=str(DESTINO))
    args = ap.parse_args()

    destino = Path(args.destino)
    destino.mkdir(parents=True, exist_ok=True)
    engine = create_engine(sqlalchemy_url())

    manifiesto = {
        "generado_utc": datetime.now(timezone.utc).isoformat(),
        "formato": args.formato,
        "objetos": {},
    }
    total = 0
    with engine.connect() as con:
        for nombre, sql in OBJETOS.items():
            df = pd.read_sql(text(sql), con)
            df = normalizar_tipos(df)
            if args.formato == "parquet":
                ruta = destino / f"{nombre}.parquet"
                # coerce_timestamps="us": pandas escribe TIMESTAMP(NANOS) por
                # defecto y Spark lo rechaza con PARQUET_TYPE_ILLEGAL. Postgres
                # guarda microsegundos, asi que no se pierde precision real.
                df.to_parquet(ruta, index=False, compression="snappy",
                              coerce_timestamps="us", allow_truncated_timestamps=True)
            else:
                ruta = destino / f"{nombre}.csv"
                df.to_csv(ruta, index=False, encoding="utf-8")
            mb = ruta.stat().st_size / 1024 / 1024
            manifiesto["objetos"][nombre] = {
                "archivo": ruta.name,
                "filas": int(len(df)),
                "columnas": list(df.columns),
                "mb": round(mb, 3),
            }
            total += len(df)
            print(f"[export] {ruta.name:<32} {len(df):>8,} filas  {mb:6.2f} MB")

    (destino / "_manifiesto.json").write_text(
        json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[export] {len(OBJETOS)} objetos / {total:,} filas en {destino}")
    print("[export] manifiesto: _manifiesto.json (usar en 01_bronze para validar conteos)")
    log_carga("30_exportar_para_databricks", str(destino), total, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
