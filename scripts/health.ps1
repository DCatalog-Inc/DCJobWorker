$svc = Get-Service -Name "JobWorker" -ErrorAction SilentlyContinue
if ($null -eq $svc -or $svc.Status -ne "Running") { exit 1 }
exit 0