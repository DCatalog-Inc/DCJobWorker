$nssm = "C:\DCatalog\nssm\nssm.exe"
& $nssm stop JobWorker

# Wait for the service to fully stop
$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($svc) {
    try { $svc.WaitForStatus('Stopped', (New-TimeSpan -Seconds 30)) } catch {}
}

# Clear CodeDeploy's deployment archive so it has no old manifest to clean up.
# Without this, CodeDeploy tries to rmdir Tools\dcproxy from the previous revision
# even when deploying to a different destination.
$deployArchive = "C:\ProgramData\Amazon\CodeDeploy"
if (Test-Path $deployArchive) {
    Get-ChildItem $deployArchive -Directory -ErrorAction SilentlyContinue |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

# Clean up any leftover staging directory from a previous failed deployment
Remove-Item "C:\DCatalog\JobWorker-staging" -Recurse -Force -ErrorAction SilentlyContinue
