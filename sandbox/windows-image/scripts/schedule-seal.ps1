$ErrorActionPreference = "Stop"

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\ProgramData\Chevalier\finalize-image.ps1"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(15)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName "OpenBracketFinalizeImage" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName "OpenBracketFinalizeImage"
