@echo off
setlocal EnableExtensions
set "MEDIA=%~1"
set "TARGET="
for %%D in (C D E F G H I J K L M N O P Q R S T U V W X Y Z) do if exist %%D:\ProgramData\Chevalier\image-manifest.json set "TARGET=%%D:"
if not defined TARGET exit /b 10
if exist "%TARGET%\ProgramData\Chevalier\runtime\runtime.json" exit /b 11
set "DEST=%TARGET%\ProgramData\Chevalier\runtime-services-staged"
if not exist "%DEST%" mkdir "%DEST%"
copy /y "%MEDIA%\chevalier-vfs-winfsp-arm64.exe" "%DEST%\chevalier-vfs-winfsp-arm64.exe" || exit /b 20
copy /y "%MEDIA%\chevalier-guest-agent-arm64.exe" "%DEST%\chevalier-guest-agent-arm64.exe" || exit /b 21
copy /y "%MEDIA%\initialize-state.ps1" "%DEST%\initialize-state.ps1" || exit /b 22
copy /y "%MEDIA%\install-runtime-services.ps1" "%DEST%\install-runtime-services.ps1" || exit /b 23
copy /y "%MEDIA%\chevalier-guest-services.SHA256SUMS" "%DEST%\chevalier-guest-services.SHA256SUMS" || exit /b 24
copy /y "%MEDIA%\image-manifest.json" "%TARGET%\ProgramData\Chevalier\image-manifest.json" || exit /b 25
if not exist "%TARGET%\Windows\Setup\Scripts" mkdir "%TARGET%\Windows\Setup\Scripts"
copy /y "%MEDIA%\SetupComplete-services.cmd" "%TARGET%\Windows\Setup\Scripts\SetupComplete.cmd" || exit /b 26
copy /y "%MEDIA%\unattend-runtime-arm64.xml" "%TARGET%\Windows\Panther\unattend.xml" || exit /b 27
echo staged>"%DEST%\offline-staged.txt"
wpeutil shutdown
exit /b 0
