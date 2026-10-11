@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

where py.exe >nul 2>nul
if not errorlevel 1 goto use_py
where python.exe >nul 2>nul
if not errorlevel 1 goto use_python

echo Python 3 was not found.
echo Install Python from https://www.python.org/downloads/windows/
echo During setup, enable Add python.exe to PATH.
pause
goto end

:use_py
py.exe "%~dp0ClipHouse.py"
goto end

:use_python
python.exe "%~dp0ClipHouse.py"

:end
endlocal