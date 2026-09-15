@echo off
cd /d "%~dp0"
set "MARK2_OFFLINE_OK=1"
set "PYTHONNOUSERSITE=1"
set "PYTHONPATH=%~dp0"
set "MARK2_FRONTEND=%~dp0mark2\frontend"
set "RECON_APP_HOME=%~dp0"
start "BRENTS TRADING BOT" /D "%~dp0" cmd /k "%~dp0mark2\watch-8tcm.bat"
start "" /D "%~dp0" wscript.exe //nologo "%~dp0mark2\launch-bot.vbs"
exit /b 0
