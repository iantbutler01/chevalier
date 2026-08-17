$ErrorActionPreference = "Stop"

Remove-Item -Recurse -Force "C:\Windows\Temp\OpenBracketImage" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\Windows\Panther\unattend.xml" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\Windows\Panther\Unattend\unattend.xml" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\Windows\Panther\Autounattend.xml" -ErrorAction SilentlyContinue

$winrmPolicy = "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WinRM\Service"
Remove-ItemProperty -Path $winrmPolicy -Name AllowBasic -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winrmPolicy -Name AllowUnencryptedTraffic -ErrorAction SilentlyContinue
& gpupdate.exe /Target:Computer /Force | Out-Null

Get-NetFirewallRule -DisplayGroup "Windows Remote Management" -ErrorAction SilentlyContinue | Disable-NetFirewallRule
$winrmService = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WSMAN\Service"
New-ItemProperty -Path $winrmService -Name auth_basic -PropertyType DWord -Value 0 -Force | Out-Null
New-ItemProperty -Path $winrmService -Name allow_unencrypted -PropertyType DWord -Value 0 -Force | Out-Null
Get-ChildItem WSMan:\localhost\Listener -ErrorAction SilentlyContinue | ForEach-Object {
    Remove-Item -Path $_.PSPath -Recurse -Force -ErrorAction SilentlyContinue
}
Set-Service -Name WinRM -StartupType Disabled
Remove-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" -Name LocalAccountTokenFilterPolicy -ErrorAction SilentlyContinue

if (@(Get-ChildItem WSMan:\localhost\Listener -ErrorAction SilentlyContinue).Count -ne 0) {
    throw "WinRM listeners remain after image cleanup"
}
if ((Get-ItemProperty -Path $winrmService -Name auth_basic).auth_basic -ne 0) {
    throw "WinRM Basic authentication remains enabled"
}
if ((Get-ItemProperty -Path $winrmService -Name allow_unencrypted).allow_unencrypted -ne 0) {
    throw "WinRM unencrypted transport remains enabled"
}
if ((Get-Service -Name WinRM).StartType -ne "Disabled") {
    throw "WinRM is not disabled"
}
if (Get-NetFirewallRule -DisplayGroup "Windows Remote Management" -ErrorAction SilentlyContinue | Where-Object Enabled -eq "True") {
    throw "Windows Remote Management firewall rules remain enabled"
}

$winlogon = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
Remove-ItemProperty -Path $winlogon -Name AutoAdminLogon -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name DefaultPassword -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name DefaultUserName -ErrorAction SilentlyContinue

$passwordBytes = New-Object byte[] 48
$passwordGenerator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $passwordGenerator.GetBytes($passwordBytes)
} finally {
    $passwordGenerator.Dispose()
}
$scrubbedAdministratorPassword = ConvertTo-SecureString ([Convert]::ToBase64String($passwordBytes)) -AsPlainText -Force
Set-LocalUser -Name "Administrator" -Password $scrubbedAdministratorPassword

Unregister-ScheduledTask -TaskName "OpenBracketFinalizeImage" -Confirm:$false -ErrorAction SilentlyContinue
Remove-Item -Force "C:\ProgramData\Chevalier\schedule-seal.ps1" -ErrorAction SilentlyContinue

$sysprep = Start-Process -FilePath "$env:WINDIR\System32\Sysprep\Sysprep.exe" -ArgumentList "/generalize", "/oobe", "/shutdown", "/quiet" -PassThru
Start-Sleep -Seconds 5
if ($sysprep.HasExited) {
    throw "Sysprep exited before generalization completed with code $($sysprep.ExitCode)"
}

Remove-Item -Force $PSCommandPath -ErrorAction SilentlyContinue
