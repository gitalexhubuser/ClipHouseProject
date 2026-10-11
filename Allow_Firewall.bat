@echo off
chcp 65001 >nul
net session >nul 2>&1
if not "%errorlevel%"=="0" (
  echo Нужны права администратора. Щёлкни файл правой кнопкой и выбери "Запуск от имени администратора".
  pause
  exit /b 1
)
netsh advfirewall firewall add rule name="ClipHouse HTTP 8787" dir=in action=allow protocol=TCP localport=8787 profile=private
netsh advfirewall firewall add rule name="ClipHouse discovery 8788" dir=in action=allow protocol=UDP localport=8788 profile=private
echo.
echo Правила добавлены только для профиля частной сети.
echo Не включай эти порты для публичных сетей и не настраивай проброс портов в интернет.
pause