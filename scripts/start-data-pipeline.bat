@echo off
REM AUREUS Data Pipeline - Script de Inicialização (Windows)
REM Inicia todos os componentes do data-pipeline

echo =============================================
echo AUREUS Data Pipeline - Iniciando Sistema
echo =============================================

REM Verificar se Docker está rodando
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker não está rodando. Por favor, inicie o Docker primeiro.
    exit /b 1
)

REM Verificar se Docker Compose está disponível
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose não está instalado.
    exit /b 1
)

echo [INFO] Verificando dependências...

REM Verificar se Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python não está instalado.
    exit /b 1
)

REM Verificar se pip está instalado
pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] pip não está instalado.
    exit /b 1
)

echo [SUCCESS] Dependências verificadas

REM Instalar dependências Python
echo [INFO] Instalando dependências Python...

REM Spark
if exist "spark\requirements.txt" (
    echo [INFO] Instalando dependências do Spark...
    pip install -r spark\requirements.txt
)

REM Flink
if exist "flink\requirements.txt" (
    echo [INFO] Instalando dependências do Flink...
    pip install -r flink\requirements.txt
)

REM Analytics
if exist "analytics\requirements.txt" (
    echo [INFO] Instalando dependências do Analytics...
    pip install -r analytics\requirements.txt
)

REM Compliance
if exist "compliance\requirements.txt" (
    echo [INFO] Instalando dependências do Compliance...
    pip install -r compliance\requirements.txt
)

echo [SUCCESS] Dependências Python instaladas

REM Iniciar serviços de dados
echo [INFO] Iniciando serviços de dados...

REM ClickHouse
echo [INFO] Iniciando ClickHouse...
cd ..\data-platform\clickhouse
docker-compose up -d

REM Aguardar ClickHouse estar pronto
echo [INFO] Aguardando ClickHouse estar pronto...
timeout /t 30 /nobreak >nul

REM Verificar se ClickHouse está rodando
curl -f http://localhost:8123/ping >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARNING] ClickHouse pode não estar pronto ainda. Continuando...
)

REM TimescaleDB
echo [INFO] Iniciando TimescaleDB...
cd ..\timescaledb
docker-compose up -d

REM Aguardar TimescaleDB estar pronto
echo [INFO] Aguardando TimescaleDB estar pronto...
timeout /t 30 /nobreak >nul

REM Voltar ao diretório de data-pipelines
cd ..\..\data-pipelines

echo [SUCCESS] Serviços de dados iniciados

REM Criar diretório de logs se não existir
if not exist "logs" mkdir logs

REM Iniciar pipeline de sincronização
echo [INFO] Iniciando pipeline de sincronização PostgreSQL → ClickHouse...
start /b python sync\postgres_to_clickhouse.py > logs\sync.log 2>&1

REM Iniciar analytics em tempo real
echo [INFO] Iniciando analytics em tempo real...
start /b python analytics\real_time_analytics.py > logs\analytics.log 2>&1

REM Iniciar pipeline Spark
echo [INFO] Iniciando pipeline Spark...
start /b python spark\transactions_processor.py > logs\spark.log 2>&1

REM Iniciar pipeline Flink
echo [INFO] Iniciando pipeline Flink...
start /b python flink\transactions_processor.py > logs\flink.log 2>&1

REM Iniciar sistema de compliance
echo [INFO] Iniciando sistema de compliance...
start /b python compliance\data_compliance.py > logs\compliance.log 2>&1

REM Aguardar um pouco para os serviços estabilizarem
echo [INFO] Aguardando serviços estabilizarem...
timeout /t 10 /nobreak >nul

REM Verificar status dos serviços
echo [INFO] Verificando status dos serviços...

REM ClickHouse
curl -f http://localhost:8123/ping >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] ClickHouse: OK
) else (
    echo [WARNING] ClickHouse: Pode não estar pronto
)

REM Analytics Dashboard
curl -f http://localhost:5000/api/health >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] Analytics Dashboard: OK
) else (
    echo [WARNING] Analytics Dashboard: Pode não estar pronto
)

REM Mostrar informações de acesso
echo.
echo =============================================
echo AUREUS Data Pipeline - SISTEMA INICIADO
echo =============================================
echo.
echo Serviços disponíveis:
echo   • ClickHouse: http://localhost:8123
echo   • TimescaleDB: localhost:5433
echo   • Analytics Dashboard: http://localhost:5000
echo.
echo Logs disponíveis em:
echo   • Sincronização: logs\sync.log
echo   • Analytics: logs\analytics.log
echo   • Spark: logs\spark.log
echo   • Flink: logs\flink.log
echo   • Compliance: logs\compliance.log
echo.
echo Para parar o sistema:
echo   stop-data-pipeline.bat
echo.
echo =============================================

echo [SUCCESS] AUREUS Data Pipeline iniciado com sucesso!
pause
