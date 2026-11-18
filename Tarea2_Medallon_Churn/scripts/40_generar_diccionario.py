# -*- coding: utf-8 -*-
"""Genera docs/DICCIONARIO_DATOS.md leyendo el catalogo real de Postgres.

Asi el diccionario nunca queda desactualizado respecto al esquema desplegado.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import RAIZ, consulta  # noqa: E402

DESCRIPCION_ESQUEMA = {
    "raw": "Landing zone. Copia fiel del Excel, sin transformaciones de negocio.",
    "core": "Modelo dimensional (estrella). Fuente de verdad para analitica.",
    "stream": "Event stream de e-commerce, append-only y particionado por mes.",
    "mart": "Vistas de negocio: replican los reportes del Excel y extienden el analisis.",
    "audit": "Metadatos de ejecucion: cargas, watermarks y checks de calidad.",
}

SQL_TABLAS = """
SELECT c.relnamespace::regnamespace::text AS esquema,
       c.relname                          AS objeto,
       CASE c.relkind WHEN 'r' THEN 'tabla' WHEN 'p' THEN 'tabla particionada'
                      WHEN 'v' THEN 'vista' WHEN 'm' THEN 'vista materializada' END AS tipo,
       obj_description(c.oid)             AS comentario,
       COALESCE((SELECT n_live_tup FROM pg_stat_user_tables s WHERE s.relid = c.oid), 0) AS filas_aprox
FROM   pg_class c
WHERE  c.relnamespace::regnamespace::text = ANY(%s)
  AND  c.relkind IN ('r','p','v','m')
  AND  c.relispartition = false
ORDER  BY 1, 3, 2;
"""

SQL_COLUMNAS = """
SELECT a.attname                                        AS columna,
       format_type(a.atttypid, a.atttypmod)             AS tipo,
       NOT a.attnotnull                                 AS acepta_nulos,
       pg_get_expr(d.adbin, d.adrelid)                  AS valor_defecto,
       a.attgenerated <> ''                             AS es_generada,
       col_description(a.attrelid, a.attnum)            AS comentario
FROM   pg_attribute a
LEFT   JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE  a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER  BY a.attnum;
"""


def main() -> int:
    esquemas = list(DESCRIPCION_ESQUEMA)
    objetos = consulta(SQL_TABLAS, (esquemas,))

    lineas: list[str] = [
        "# Diccionario de datos",
        "",
        "> Generado automaticamente desde el catalogo de PostgreSQL con",
        "> `python scripts/40_generar_diccionario.py`. No editar a mano.",
        "",
        "## Indice de esquemas",
        "",
        "| Esquema | Proposito | Objetos |",
        "|---|---|---|",
    ]
    for esq, desc in DESCRIPCION_ESQUEMA.items():
        n = sum(1 for o in objetos if o["esquema"] == esq)
        lineas.append(f"| `{esq}` | {desc} | {n} |")
    lineas.append("")

    for esq, desc in DESCRIPCION_ESQUEMA.items():
        propios = [o for o in objetos if o["esquema"] == esq]
        if not propios:
            continue
        lineas += [f"---", "", f"## Esquema `{esq}`", "", f"*{desc}*", ""]
        for o in propios:
            nombre = f"{esq}.{o['objeto']}"
            lineas.append(f"### `{nombre}`  <sub>({o['tipo']})</sub>")
            lineas.append("")
            if o["comentario"]:
                lineas += [o["comentario"], ""]
            if o["tipo"].startswith("tabla") and o["filas_aprox"]:
                lineas += [f"Filas (aprox.): **{int(o['filas_aprox']):,}**", ""]
            cols = consulta(SQL_COLUMNAS, (nombre,))
            lineas += ["| Columna | Tipo | Nulos | Notas |", "|---|---|---|---|"]
            for c in cols:
                notas = []
                if c["es_generada"]:
                    notas.append("**generada**: " + (c["valor_defecto"] or "").replace("|", r"\|"))
                elif c["valor_defecto"]:
                    notas.append(f"default `{c['valor_defecto']}`")
                if c["comentario"]:
                    notas.append(c["comentario"])
                lineas.append(
                    f"| `{c['columna']}` | `{c['tipo']}` | "
                    f"{'si' if c['acepta_nulos'] else 'no'} | {'; '.join(notas)} |")
            lineas.append("")

    destino = RAIZ / "docs" / "DICCIONARIO_DATOS.md"
    destino.parent.mkdir(exist_ok=True)
    destino.write_text("\n".join(lineas), encoding="utf-8")
    print(f"[docs] {destino} ({len(lineas)} lineas, {len(objetos)} objetos)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
