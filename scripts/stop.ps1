$nssm = "C:\DCatalog\nssm\nssm.exe"
& $nssm stop JobWorker

# Wait for the service to fully stop
$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($svc) {
    try { $svc.WaitForStatus('Stopped', (New-TimeSpan -Seconds 30)) } catch {}
}

# Remove app files only — leave Tools\ in place (locked native binaries)
$installDir = "C:\DCatalog\JobWorker"
if (Test-Path $installDir) {
    Get-ChildItem $installDir -Exclude "Tools" | Remove-Item -Recurse -Force
}
