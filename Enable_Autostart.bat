@echo off
chcp 65001 >nul
setlocal
set "PYEXE="
for /f "delims=" %%P in ('where python.exe 2^>nul') do if not defined PYEXE set "PYEXE=%%P"
if not defined PYEXE (
  echo Не удалось найти python.exe через PATH.
  echo Запусти ClipHouse вручную через Start_ClipHouse.bat или добавь Python в PATH.
  pause
  exit /b 1
)
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "VBS=%STARTUP%\ClipHouse.vbs"
set "SCRIPT=%~dp0ClipHouse.py"
set "LOG=%LOCALAPPDATA%\ClipHouse\startup.log"
if not exist "%LOCALAPPDATA%\ClipHouse" mkdir "%LOCALAPPDATA%\ClipHouse"
(
  echo Set sh = CreateObject("WScript.Shell")
  echo sh.Run Chr(34) ^& "%PYEXE%" ^& Chr(34) ^& " " ^& Chr(34) ^& "%SCRIPT%" ^& Chr(34) ^& " ^>^> " ^& Chr(34) ^& "%LOG%" ^& Chr(34) ^& " 2^>^&1", 0, False
) > "%VBS%"
echo Автозапуск создан: %VBS%
echo ClipHouse будет запускаться скрыто при входе в Windows.
echo Лог запуска: %LOG%
echo Важно: папку программы нельзя перемещать после настройки автозапуска.
pause