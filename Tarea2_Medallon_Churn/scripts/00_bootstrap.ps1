<#
.SYNOPSIS
    Levanta toda la plataforma de datos de la Tarea 2 de punta a punta.
.DESCRIPTION
    1. Verifica Docker.
    2. Levanta Postgres 16 + pgAdmin (docker compose).
    3. Aplica los 5 scripts SQL (esquemas, modelo estrella, event stream, marts, calidad).
    4. Carga el Excel  -> raw + core.
    5. Genera el event stream -> stream.evento.
    6. Ejecuta los checks de calidad.
    7. Exporta los Parquet para subir a Databricks.
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\00_bootstrap.ps1
#>
$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

function Write-Paso($texto) {
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor Cyan
    Write-Host "  $texto" -ForegroundColor Cyan
    Write-Host ("=" * 70) -ForegroundColor Cyan
}

Write-Paso "1/7  Verificando Docker"
docker info --format '{{.ServerVersion}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Docker no esta disponible. Inicia Docker Desktop." }
Write-Host "Docker OK" -ForegroundColor Green

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Creado .env a partir de .env.example" -ForegroundColor Yellow
}

Write-Paso "2/7  Levantando Postgres + pgAdmin"
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up fallo" }

Write-Paso "3/7  Aplicando esquemas SQL"
python scripts\db.py
$scripts = @(
    "01_schemas_raw",
    "02_core_modelo_estrella",
    "03_stream_eventos",
    "04_vistas_mart",
    "05_calidad_datos"
)
$env:MSYS_NO_PATHCONV = "1"
foreach ($s in $scripts) {
    Write-Host "  -> $s.sql"
    docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -q -v ON_ERROR_STOP=1 -f "/sql/$s.sql"
    if ($LASTEXITCODE -ne 0) { throw "Fallo el script $s.sql" }
}
Write-Host "Esquemas aplicados" -ForegroundColor Green

Write-Paso "4/7  Cargando el Excel (raw + core)"
python scripts\10_cargar_excel.py
if ($LASTEXITCODE -ne 0) { throw "Fallo la carga del Excel" }

Write-Paso "5/7  Generando el event stream"
python scripts\20_generar_event_stream.py
if ($LASTEXITCODE -ne 0) { throw "Fallo la generacion del event stream" }

Write-Paso "6/7  Ejecutando checks de calidad"
docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -q -c "CALL audit.sp_ejecutar_checks();"
docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -c "SELECT checks, pass, fail, fail_criticos FROM audit.v_resumen_check LIMIT 1;"
docker exec -i ecom_tarea2_pg psql -U ecom -d ecommerce_dw -c "SELECT objeto, nombre_check, severidad, valor_obs, valor_esp FROM audit.v_ultimo_check WHERE resultado = 'FAIL';"

Write-Paso "7/7  Exportando Parquet para Databricks"
python scripts\30_exportar_para_databricks.py
if ($LASTEXITCODE -ne 0) { throw "Fallo el export" }

Write-Paso "LISTO"
Write-Host "Postgres : localhost:5435  (base ecommerce_dw, usuario ecom)" -ForegroundColor Green
Write-Host "pgAdmin  : http://localhost:5051" -ForegroundColor Green
Write-Host "Parquet  : data\export\  -> subir a Databricks (ver databricks\README_IMPORT.md)" -ForegroundColor Green
