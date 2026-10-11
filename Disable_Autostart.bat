@echo off
chcp 65001 >nul
set "VBS=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\ClipHouse.vbs"
if exist "%VBS%" del "%VBS%"
echo Автозапуск ClipHouse отключён (если ярлык существовал).
pause