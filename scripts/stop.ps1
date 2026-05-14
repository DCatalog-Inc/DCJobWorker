$nssm = "C:\DCatalog\nssm\nssm.exe"
& $nssm stop JobWorker

# Wait for the service to fully stop
$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($svc) {
    try { $svc.WaitForStatus('Stopped', (New-TimeSpan -Seconds 30)) } catch {}
}

# Kill any tool processes that hold locks on files in the install directory
Get-Process -Name "dcproxy","dcmutool" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

# Give processes a moment to release file handles
Start-Sleep -Seconds 2

# Remove old installation so CodeDeploy has a clean directory to install into
$installDir = "C:\DCatalog\JobWorker"
if (Test-Path $installDir) {
    Remove-Item -Path $installDir -Recurse -Force
}
