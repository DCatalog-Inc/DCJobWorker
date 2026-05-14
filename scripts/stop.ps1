$nssm = "C:\DCatalog\nssm\nssm.exe"
& $nssm stop JobWorker

# Wait for the service to fully stop
$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($svc) {
    try { $svc.WaitForStatus('Stopped', (New-TimeSpan -Seconds 30)) } catch {}
}

# Clean up any leftover staging directory from a previous failed deployment
Remove-Item "C:\DCatalog\JobWorker-staging" -Recurse -Force -ErrorAction SilentlyContinue
