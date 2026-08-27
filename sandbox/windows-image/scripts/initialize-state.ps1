$ErrorActionPreference = "Stop"

$stateRoot = "C:\ProgramData\Chevalier\state-volume"
$stateDiskSerial = "openbracket-vfs"
$allDisks = @(Get-Disk)
$disks = @($allDisks | Where-Object {
    $_.SerialNumber -and $_.SerialNumber.Trim().StartsWith($stateDiskSerial, [StringComparison]::OrdinalIgnoreCase)
})
if ($disks.Count -ne 1) {
    $observed = ($allDisks | ForEach-Object { "number=$($_.Number),serial=$($_.SerialNumber),style=$($_.PartitionStyle)" }) -join "; "
    throw "Expected exactly one Chevalier state disk with serial $stateDiskSerial, found $($disks.Count); observed: $observed"
}
$disk = $disks[0]
if ($disk.IsOffline) {
    Set-Disk -Number $disk.Number -IsOffline $false
}
if ($disk.IsReadOnly) {
    Set-Disk -Number $disk.Number -IsReadOnly $false
}
if ($disk.PartitionStyle -eq "RAW") {
    Initialize-Disk -Number $disk.Number -PartitionStyle GPT | Out-Null
    $partition = New-Partition -DiskNumber $disk.Number -UseMaximumSize
    Format-Volume -Partition $partition -FileSystem NTFS -NewFileSystemLabel "OBVFSSTATE" -Confirm:$false | Out-Null
} else {
    $partitions = @(Get-Partition -DiskNumber $disk.Number | Where-Object { $_.Type -eq "Basic" })
    if ($partitions.Count -ne 1) {
        throw "Expected one basic partition on the Chevalier state disk, found $($partitions.Count)"
    }
    $partition = $partitions[0]
    $volume = Get-Volume -Partition $partition
    if ($volume.FileSystem -ne "NTFS" -or $volume.FileSystemLabel -ne "OBVFSSTATE") {
        throw "Chevalier state disk identity or filesystem does not match"
    }
}

New-Item -ItemType Directory -Force -Path $stateRoot | Out-Null
$accessPaths = @((Get-Partition -DiskNumber $disk.Number -PartitionNumber $partition.PartitionNumber).AccessPaths)
$desiredAccessPath = "$stateRoot\"
if ($accessPaths -notcontains $desiredAccessPath) {
    Add-PartitionAccessPath -DiskNumber $disk.Number -PartitionNumber $partition.PartitionNumber -AccessPath $desiredAccessPath
}
New-Item -ItemType Directory -Force -Path (Join-Path $stateRoot "workspace") | Out-Null
& icacls.exe $stateRoot /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Failed to secure Chevalier state volume"
}
