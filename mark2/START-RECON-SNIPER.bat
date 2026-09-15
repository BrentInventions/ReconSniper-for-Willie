@echo off
:: Bot HUD in the background. Separate CMD shows the 8TCM state machine.
cd /d "%~dp0.."
if not exist "%cd%\mark2\watch-8tcm.bat" cd /d "%~dp0"
start "BRENTS TRADING BOT" cmd /k "%~dp0watch-8tcm.bat"
start "" wscript.exe //nologo "%~dp0launch-bot.vbs"
exit /b 0
