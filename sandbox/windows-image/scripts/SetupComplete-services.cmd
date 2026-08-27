@echo off
setlocal EnableExtensions
set "STAGED=C:\ProgramData\Chevalier\runtime-services-staged"
set "LOG=C:\ProgramData\Chevalier\runtime-services-install.log"
"C:\Program Files\PowerShell\7\pwsh.exe" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%STAGED%\install-runtime-services.ps1" -ArtifactRoot "%STAGED%" -StartGuestService >"%LOG%.tmp" 2>&1
set "INSTALL_EXIT=%ERRORLEVEL%"
move /y "%LOG%.tmp" "%LOG%" >nul
if not "%INSTALL_EXIT%"=="0" exit /b %INSTALL_EXIT%
rmdir /s /q "%STAGED%"
del /q "%~f0"
exit /b 0
