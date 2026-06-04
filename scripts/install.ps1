$svc     = "JobWorker"
$exe     = "C:\DCatalog\JobWorker\JobWorker.exe"
$nssm    = "C:\DCatalog\nssm\nssm.exe"
$staging = "C:\DCatalog\JobWorker-staging"
$dest    = "C:\DCatalog\JobWorker"

# Remove old service registration
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
    & $nssm stop $svc
    & $nssm remove $svc confirm
}

# Ensure destination and logs directories exist
New-Item -ItemType Directory -Force -Path $dest | Out-Null
New-Item -ItemType Directory -Force -Path "$dest\logs" | Out-Null

# Robocopy new files over and PURGE stale ones — the May 2026 outage was caused by
# leftover self-contained runtime files mixing with a framework-dependent build.
# Tools\ and logs\ live only in $dest (not in staging) and are protected from /PURGE.
robocopy $staging $dest /E /IS /IT /COPYALL /PURGE /XD "$dest\Tools" "$dest\logs" /NFL /NDL /NJH /NJS
Remove-Item $staging -Recurse -Force -ErrorAction SilentlyContinue

# Install and configure the service
& $nssm install $svc $exe
& $nssm set $svc AppDirectory $dest
& $nssm set $svc Start SERVICE_AUTO_START
& $nssm set $svc AppRestartDelay 5000
& $nssm set $svc AppStdout "$dest\logs\stdout.log"
& $nssm set $svc AppStderr "$dest\logs\stderr.log"
