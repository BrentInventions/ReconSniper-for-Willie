@echo off
setlocal
cd /d "%~dp0"
title Recon Sniper — first run
echo Installing HUD (pywebview)...
py -3 -m pip install -r requirements.txt
if errorlevel 1 python -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Python failed. Install Python 3 from python.org and check Add to PATH.
  pause
  exit /b 1
)
echo.
echo Starting Recon Sniper...
call "%~dp0START-RECON-SNIPER.bat"
endlocal
