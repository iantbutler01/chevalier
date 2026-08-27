@echo off
setlocal EnableExtensions
set "MEDIA=%~1"
set "TARGET="
for %%D in (C D E F G H I J K L M N O P Q R S T U V W X Y Z) do if exist %%D:\ProgramData\Chevalier\image-manifest.json set "TARGET=%%D:"
if not defined TARGET exit /b 10
set "DEST=%TARGET%\ProgramData\Chevalier\winfsp-e2e"
if not exist "%DEST%" mkdir "%DEST%"
copy /y "%MEDIA%\chevalier-vfs-winfsp.exe" "%DEST%\chevalier-vfs-winfsp.exe" || exit /b 11
copy /y "%MEDIA%\run-e2e.ps1" "%DEST%\run-e2e.ps1" || exit /b 12
copy /y "%MEDIA%\e2e-config.json" "%DEST%\e2e-config.json" || exit /b 13
copy /y "%MEDIA%\unattend-specialize.xml" "%TARGET%\Windows\Panther\unattend.xml" || exit /b 14
echo staged>"%DEST%\winpe-staged.txt"
wpeutil shutdown
exit /b 0
