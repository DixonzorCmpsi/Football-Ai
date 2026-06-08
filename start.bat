@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem start.bat - Windows local dev runner for Football-Ai.
rem
rem Modes:
rem   start.bat                 -> docker (full stack via docker compose)
rem   start.bat local           -> local (Postgres in docker, backend+frontend on host)
rem   start.bat frontend        -> frontend dev server; starts/checks backend first
rem   start.bat backend         -> backend only (Postgres in docker if available, CSV fallback otherwise)
rem   start.bat db              -> database only (Postgres in docker)
rem   start.bat stop            -> stop docker stack
rem
rem Env overrides:
rem   BACKEND_PORT (default 8000)
rem   FRONTEND_PORT (default 5273)
rem   DB_CONNECTION_STRING (default postgresql://admin:password@localhost:5432/football_ai)
rem   RUN_ETL_ON_STARTUP (default true for local mode)
rem   ALLOW_CSV_FALLBACK (default true for local mode)

set "ROOT=%~dp0"
set "MODE=%~1"
if "%MODE%"=="" set "MODE=docker"

if "%BACKEND_PORT%"=="" set "BACKEND_PORT=8000"
if "%FRONTEND_PORT%"=="" set "FRONTEND_PORT=5273"
if "%DB_CONNECTION_STRING%"=="" set "DB_CONNECTION_STRING=postgresql://admin:password@localhost:5432/football_ai"

if /I "%MODE%"=="docker" goto docker
if /I "%MODE%"=="local" goto local
if /I "%MODE%"=="backend" goto backend
if /I "%MODE%"=="frontend" goto frontend
if /I "%MODE%"=="db" goto db
if /I "%MODE%"=="stop" goto stop

echo Unknown mode: %MODE%
echo Usage: start.bat [docker^|local^|backend^|frontend^|db^|stop]
exit /b 1

:require_docker
where docker >nul 2>nul
if errorlevel 1 (
  echo Docker is required to start Postgres. Install Docker Desktop and retry.
  exit /b 1
)
docker info >nul 2>nul
if errorlevel 1 (
  echo Docker is not running. Start Docker Desktop and retry.
  exit /b 1
)
exit /b 0

:wait_db
echo Waiting for Postgres to accept connections...
for /L %%I in (1,1,45) do (
  docker compose exec -T db pg_isready -U admin -d football_ai >nul 2>nul
  if not errorlevel 1 (
    echo Postgres is ready on localhost:5432
    exit /b 0
  )
  timeout /t 1 /nobreak >nul
)
echo Postgres did not become ready in time. Run "docker compose logs db" for details.
exit /b 1

:ensure_db
call :require_docker || exit /b 1
echo Starting Postgres container...
pushd "%ROOT%" >nul
docker compose up -d db
if errorlevel 1 (
  popd >nul
  exit /b 1
)
call :wait_db
set "DB_READY_ERROR=%ERRORLEVEL%"
popd >nul
exit /b %DB_READY_ERROR%

:ensure_db_or_csv_fallback
call :require_docker
if errorlevel 1 (
  echo Docker is not available; starting backend with CSV fallback data.
  set "DB_CONNECTION_STRING="
  set "ALLOW_CSV_FALLBACK=true"
  exit /b 0
)
call :ensure_db || exit /b 1
if "%ALLOW_CSV_FALLBACK%"=="" set "ALLOW_CSV_FALLBACK=true"
exit /b 0

:backend_ready
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $h = Invoke-RestMethod -Uri 'http://127.0.0.1:%BACKEND_PORT%/health' -TimeoutSec 5; if ($h.ready -eq $true) { exit 0 } else { exit 1 } } catch { exit 1 }" >nul 2>nul
exit /b %ERRORLEVEL%

:wait_backend
echo Waiting for backend player data...
for /L %%I in (1,1,90) do (
  call :backend_ready
  if not errorlevel 1 (
    echo Backend is ready on http://localhost:%BACKEND_PORT%
    exit /b 0
  )
  timeout /t 1 /nobreak >nul
)
echo Backend did not become data-ready. Check the backend window/logs.
exit /b 1

:ensure_backend
call :backend_ready
if not errorlevel 1 (
  echo Backend already ready on :%BACKEND_PORT%
  exit /b 0
)
echo Starting backend dependency...
start "Football AI Backend" cmd /k ""%~f0" backend"
call :wait_backend
exit /b %ERRORLEVEL%

:docker
call :require_docker || exit /b 1
pushd "%ROOT%" >nul
echo Starting database first...
docker compose up --build -d db
if errorlevel 1 (
  popd >nul
  exit /b 1
)
call :wait_db || (
  popd >nul
  exit /b 1
)
echo Starting backend + frontend via docker compose...
docker compose up --build -d backend frontend
if errorlevel 1 (
  popd >nul
  exit /b 1
)
echo -^> Frontend:  http://localhost
echo -^> Backend:   http://localhost:8000
echo -^> Postgres:  localhost:5432 (admin/password)
docker compose ps
popd >nul
exit /b 0

:local
call :ensure_db_or_csv_fallback || exit /b 1
echo Launching backend and frontend in separate windows.
start "Football AI Backend" cmd /k ""%~f0" backend"
call :wait_backend || exit /b 1
start "Football AI Frontend" cmd /k ""%~f0" frontend"
echo -^> Frontend:  http://localhost:%FRONTEND_PORT%
echo -^> Backend:   http://localhost:%BACKEND_PORT%
echo -^> Postgres:  localhost:5432 (admin/password)
exit /b 0

:backend
call :ensure_db_or_csv_fallback || exit /b 1
pushd "%ROOT%backend" >nul
echo Starting backend on :%BACKEND_PORT%
if not exist ".venv\Scripts\python.exe" (
  call :create_venv || (
    popd >nul
    exit /b 1
  )
)
call ".venv\Scripts\activate.bat"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt
if errorlevel 1 (
  echo requirements.txt install failed; installing minimum backend dependencies.
  python -m pip install --quiet fastapi uvicorn polars sqlalchemy psycopg2-binary apscheduler joblib xgboost scikit-learn nflreadpy requests python-dotenv
  if errorlevel 1 (
    popd >nul
    exit /b 1
  )
)
if "%ALLOW_CSV_FALLBACK%"=="" set "ALLOW_CSV_FALLBACK=true"
if "%RUN_ETL_ON_STARTUP%"=="" set "RUN_ETL_ON_STARTUP=true"
set "DB_CONNECTION_STRING=%DB_CONNECTION_STRING%"
python -m uvicorn applications.server:app --reload --host 0.0.0.0 --port %BACKEND_PORT%
set "BACKEND_ERROR=%ERRORLEVEL%"
popd >nul
exit /b %BACKEND_ERROR%

:create_venv
where py >nul 2>nul
if not errorlevel 1 (
  py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>nul
  if not errorlevel 1 (
    echo Creating Python venv with py -3.11...
    py -3.11 -m venv .venv
    exit /b %ERRORLEVEL%
  )
  py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>nul
  if not errorlevel 1 (
    echo Creating Python venv with py -3...
    py -3 -m venv .venv
    exit /b %ERRORLEVEL%
  )
)
where python >nul 2>nul
if not errorlevel 1 (
  python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>nul
  if not errorlevel 1 (
    echo Creating Python venv with python...
    python -m venv .venv
    exit /b %ERRORLEVEL%
  )
)
echo No Python 3.10+ found. Install Python 3.10 or newer and retry.
exit /b 1

:frontend
call :ensure_backend || exit /b 1
pushd "%ROOT%Dashboard\predictor-frontend" >nul
echo Starting frontend on :%FRONTEND_PORT%
if not exist "node_modules" (
  echo Installing frontend dependencies...
  call npm install
  if errorlevel 1 (
    popd >nul
    exit /b 1
  )
)
set "VITE_API_BASE_URL=http://localhost:%BACKEND_PORT%/api"
call npm run dev -- --port %FRONTEND_PORT% --host
set "FRONTEND_ERROR=%ERRORLEVEL%"
popd >nul
exit /b %FRONTEND_ERROR%

:db
call :ensure_db || exit /b 1
pushd "%ROOT%" >nul
docker compose ps db
popd >nul
exit /b 0

:stop
pushd "%ROOT%" >nul
echo Stopping docker stack...
docker compose down
set "STOP_ERROR=%ERRORLEVEL%"
popd >nul
exit /b %STOP_ERROR%
