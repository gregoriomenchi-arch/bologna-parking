@echo off
cd /d "%~dp0"

echo Attivazione venv...
call venv\Scripts\activate.bat

echo Avvio uvicorn in background (log: backend\uvicorn.log)...
start /B "" venv\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000 >> uvicorn.log 2>&1

echo Avvio scheduler raccolta dati overnight...
start /B "" venv\Scripts\python.exe scheduler.py >> scheduler.log 2>&1

echo Avvio webapp su http://localhost:3000 ...
cd /d "%~dp0..\webapp"
start /B "" python -m http.server 3000 >> ..\backend\webapp.log 2>&1

echo.
echo ============================================
echo  Backend avviato - raccolta dati in corso
echo  API:    http://localhost:8000/docs
echo  Webapp: http://localhost:3000
echo  Log:    backend\uvicorn.log
echo         backend\scheduler.log
echo ============================================
echo.
