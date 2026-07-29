@echo off
REM AUREUS Data Pipeline - Script de Parada (Windows)

echo [INFO] Parando processos do AUREUS Data Pipeline...

set SCRIPT_DIR=%~dp0
set DATA_PIPELINES_DIR=%SCRIPT_DIR%..
set LOGS_DIR=%DATA_PIPELINES_DIR%\logs
set PLATFORM_DIR=%DATA_PIPELINES_DIR%\..\data-platform

cd /d "%DATA_PIPELINES_DIR%"

if exist "%LOGS_DIR%\sync.pid" (
    for /f "usebackq" %%a in ("%LOGS_DIR%\sync.pid") do taskkill /PID %%a /F 2>nul
    del "%LOGS_DIR%\sync.pid" 2>nul
    echo [SUCCESS] Processo sync encerrado
)
if exist "%LOGS_DIR%\analytics.pid" (
    for /f "usebackq" %%a in ("%LOGS_DIR%\analytics.pid") do taskkill /PID %%a /F 2>nul
    del "%LOGS_DIR%\analytics.pid" 2>nul
    echo [SUCCESS] Processo analytics encerrado
)
if exist "%LOGS_DIR%\spark.pid" (
    for /f "usebackq" %%a in ("%LOGS_DIR%\spark.pid") do taskkill /PID %%a /F 2>nul
    del "%LOGS_DIR%\spark.pid" 2>nul
    echo [SUCCESS] Processo spark encerrado
)
if exist "%LOGS_DIR%\flink.pid" (
    for /f "usebackq" %%a in ("%LOGS_DIR%\flink.pid") do taskkill /PID %%a /F 2>nul
    del "%LOGS_DIR%\flink.pid" 2>nul
    echo [SUCCESS] Processo flink encerrado
)
if exist "%LOGS_DIR%\compliance.pid" (
    for /f "usebackq" %%a in ("%LOGS_DIR%\compliance.pid") do taskkill /PID %%a /F 2>nul
    del "%LOGS_DIR%\compliance.pid" 2>nul
    echo [SUCCESS] Processo compliance encerrado
)

echo [INFO] Parando serviços de dados (Docker)...

if exist "%PLATFORM_DIR%\clickhouse\docker-compose.yml" (
    echo [INFO] Parando ClickHouse...
    cd /d "%PLATFORM_DIR%\clickhouse"
    docker-compose down 2>nul
    echo [SUCCESS] ClickHouse parado
)

if exist "%PLATFORM_DIR%\timescaledb\docker-compose.yml" (
    echo [INFO] Parando TimescaleDB...
    cd /d "%PLATFORM_DIR%\timescaledb"
    docker-compose down 2>nul
    echo [SUCCESS] TimescaleDB parado
)

echo [SUCCESS] AUREUS Data Pipeline encerrado.
pause
