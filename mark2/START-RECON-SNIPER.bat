@echo off
cd /d "%~dp0.."
py -3 -m mark2
if errorlevel 1 python -m mark2
pause
