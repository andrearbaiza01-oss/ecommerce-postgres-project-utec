# Como subir y ejecutar los notebooks en Databricks

Los archivos `.py` de esta carpeta estan en **formato "Databricks notebook source"**
(primera linea `# Databricks notebook source`). Al importarlos, Databricks los
reconstruye como notebooks con sus celdas y su markdown.

## 1. Importar los notebooks

**Opcion A - interfaz web (la mas simple)**

1. Workspace -> carpeta `Shared` -> `Create` -> `Folder` -> nombrala `tarea2`.
2. Entra a la carpeta -> menu `⋮` -> `Import`.
3. Arrastra los 7 archivos `.py` (`00_` a `06_`). Deja `Import as: Auto`.
4. Verifica que aparezcan como notebooks (icono de notebook, no de archivo).

**Opcion B - CLI**

```bash
databricks workspace mkdirs /Shared/tarea2
databricks workspace import-dir ./databricks /Shared/tarea2 --overwrite
```

> Los notebooks se llaman entre si con `%run ./00_config_setup`, por lo que
> **deben quedar todos en la misma carpeta**.

## 2. Subir los datos

Ejecuta primero en tu maquina:

```powershell
python scripts\30_exportar_para_databricks.py
```

Eso genera `data\export\*.parquet` y `_manifiesto.json`.

1. Abre `00_config_setup` y ejecutalo una vez: crea los esquemas y el Volume
   `<catalogo>.tarea2_ops.landing`.
2. `Catalog` -> tu catalogo -> `tarea2_ops` -> `Volumes` -> `landing` -> `Upload to this volume`.
3. Sube **los 12 `.parquet` y el `_manifiesto.json`**.

Con la CLI:

```bash
databricks fs cp ./data/export dbfs:/Volumes/workspace/tarea2_ops/landing --recursive --overwrite
```

## 3. Ejecutar en orden

| # | Notebook | Que deja construido |
|---|---|---|
| 0 | `00_config_setup` | esquemas, Volume, tablas de control (no produce datos) |
| 1 | `01_bronze_ingesta` | `tarea2_bronze.*` |
| 2 | `02_silver_limpieza` | `tarea2_silver.*` + cuarentena |
| 3 | `03_gold_marts` | marts de negocio + `cliente_360` |
| 4 | `04_gold_churn_features` | `churn_features`, `churn_scoring_input` |
| 5 | `05_gold_churn_model` | modelo en MLflow + `churn_scores` |
| 6 | `06_calidad_y_monitoreo` | reconciliacion y reporte de calidad |

Cada notebook se detiene con `AssertionError` si un check de severidad `ERROR`
queda en `FAIL`: es intencional (fail-fast), no se propaga data mala a la capa siguiente.

## 4. Parametros (widgets)

Aparecen arriba del notebook despues de la primera ejecucion:

| Widget | Default | Para que sirve |
|---|---|---|
| `catalogo` | `workspace` | catalogo de Unity Catalog. En Free Edition dejalo en `workspace` |
| `prefijo_esquema` | `tarea2` | genera `tarea2_bronze`, `tarea2_silver`, `tarea2_gold`, `tarea2_ops` |
| `modo_ingesta` | `volume_files` | `volume_files` lee los Parquet; `jdbc` se conecta al Postgres |
| `pg_host` ... `pg_user` | — | solo para `modo_ingesta=jdbc` |
| `secret_scope` | vacio | scope con la clave `pg_password` (modo jdbc) |
| `recarga_completa` | `false` | `true` descarta el watermark y reingesta todo el stream |

## 5. Ingesta por JDBC (opcional)

Databricks SaaS **no alcanza** un Postgres en `localhost`. Para usar `modo_ingesta=jdbc`
necesitas exponerlo (tunel SSH, ngrok/Cloudflare Tunnel, VPN o un Postgres gestionado) y:

```bash
databricks secrets create-scope tarea2
databricks secrets put-secret tarea2 pg_password
```

Luego pon `secret_scope=tarea2`, `pg_host=<host alcanzable>`, `pg_port=5435`.
El driver `org.postgresql.Driver` ya viene incluido en Databricks Runtime.

## 6. Orquestacion

`99_job_pipeline.json` define el Job con las 6 tareas y sus dependencias:

```bash
databricks jobs create --json @databricks/99_job_pipeline.json
```

Queda **pausado**; actualiza los `notebook_path` si no usaste `/Shared/tarea2/`.

## Requisitos del cluster

- Databricks Runtime **13.3 LTS o superior** (o serverless), con ML Runtime
  recomendado para `05_gold_churn_model` (MLflow + Spark ML ya instalados).
- Sin librerias adicionales: todo usa `pyspark.ml`, `mlflow` y Delta Lake.
