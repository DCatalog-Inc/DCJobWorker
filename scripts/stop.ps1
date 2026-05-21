$nssm = "C:\DCatalog\nssm\nssm.exe"
& $nssm stop JobWorker

# Wait for the service to fully stop
$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($svc) {
    try { $svc.WaitForStatus('Stopped', (New-TimeSpan -Seconds 30)) } catch {}
}

# Kill any dcproxy/dcmutool processes that may still hold file locks
Get-Process -Name "dcproxy","dcmutool" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

# Remove the dcproxy directory so CodeDeploy's manifest cleanup can succeed.
# These binaries are locked while the service runs; stopping it above frees them.
Remove-Item "C:\DCatalog\JobWorker\Tools\dcproxy" -Recurse -Force -ErrorAction SilentlyContinue

# Clean up any leftover staging directory from a previous failed deployment
Remove-Item "C:\DCatalog\JobWorker-staging" -Recurse -Force -ErrorAction SilentlyContinue
