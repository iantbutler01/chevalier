$ErrorActionPreference = "Stop"

Set-LocalUser -Name "Administrator" -PasswordNeverExpires $true
Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" -Name LocalAccountTokenFilterPolicy -Type DWord -Value 1
$winrmPolicy = "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WinRM\Service"
New-Item -Path $winrmPolicy -Force | Out-Null
New-ItemProperty -Path $winrmPolicy -Name AllowBasic -PropertyType DWord -Value 1 -Force | Out-Null
New-ItemProperty -Path $winrmPolicy -Name AllowUnencryptedTraffic -PropertyType DWord -Value 1 -Force | Out-Null
Enable-PSRemoting -SkipNetworkProfileCheck -Force
Set-Item -Path WSMan:\localhost\MaxTimeoutms -Value 1800000
Set-Item -Path WSMan:\localhost\Shell\MaxMemoryPerShellMB -Value 2048
& gpupdate.exe /Target:Computer /Force | Out-Null
Set-Service -Name WinRM -StartupType Automatic
Restart-Service -Name WinRM
