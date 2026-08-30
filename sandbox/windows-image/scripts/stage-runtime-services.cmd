@echo off
setlocal EnableExtensions
set "MEDIA=%~1"
set "TARGET="
for %%D in (C D E F G H I J K L M N O P Q R S T U V W X Y Z) do if exist %%D:\ProgramData\Chevalier\image-manifest.json set "TARGET=%%D:"
if not defined TARGET exit /b 10
if exist "%TARGET%\ProgramData\Chevalier\runtime\runtime.json" exit /b 11
set "DEST=%TARGET%\ProgramData\Chevalier\runtime-services-staged"
if not exist "%DEST%" mkdir "%DEST%"
for %%F in (chevalier-vfs-winfsp-arm64.exe chevalier-vfs-winfsp-amd64.exe) do if exist "%MEDIA%\%%F" copy /y "%MEDIA%\%%F" "%DEST%\%%F" || exit /b 20
for %%F in (chevalier-guest-agent-arm64.exe chevalier-guest-agent-amd64.exe) do if exist "%MEDIA%\%%F" copy /y "%MEDIA%\%%F" "%DEST%\%%F" || exit /b 21
copy /y "%MEDIA%\initialize-state.ps1" "%DEST%\initialize-state.ps1" || exit /b 22
copy /y "%MEDIA%\install-runtime-services.ps1" "%DEST%\install-runtime-services.ps1" || exit /b 23
copy /y "%MEDIA%\chevalier-guest-services.SHA256SUMS" "%DEST%\chevalier-guest-services.SHA256SUMS" || exit /b 24
copy /y "%MEDIA%\image-manifest.json" "%TARGET%\ProgramData\Chevalier\image-manifest.json" || exit /b 25
if not exist "%TARGET%\Windows\Setup\Scripts" mkdir "%TARGET%\Windows\Setup\Scripts"
copy /y "%MEDIA%\SetupComplete-services.cmd" "%TARGET%\Windows\Setup\Scripts\SetupComplete.cmd" || exit /b 26
if not exist "%TARGET%\Windows\Panther\Unattend" mkdir "%TARGET%\Windows\Panther\Unattend"
del /q "%TARGET%\Windows\Panther\unattend.xml" 2>nul
del /q "%TARGET%\Windows\Panther\Autounattend.xml" 2>nul
del /q "%TARGET%\Windows\Panther\unattend-original.xml" 2>nul
copy /y "%MEDIA%\Unattend-runtime.xml" "%TARGET%\Windows\Panther\Unattend\Unattend.xml" || exit /b 27
reg load HKLM\OB_SYSTEM "%TARGET%\Windows\System32\Config\SYSTEM" || exit /b 28
reg add HKLM\OB_SYSTEM\Setup /v UnattendFile /t REG_SZ /d "C:\Windows\Panther\Unattend\Unattend.xml" /f || exit /b 29
reg unload HKLM\OB_SYSTEM || exit /b 30
echo staged>"%DEST%\offline-staged.txt"
wpeutil shutdown
exit /b 0
