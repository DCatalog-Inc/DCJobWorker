$svc = "JobWorker"

$exe  = "C:\DCatalog\JobWorker\JobWorker.exe"
$nssm = "C:\DCatalog\JobWorker\Tools\nssm\nssm.exe"

# If service already exists, remove it cleanly
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
    & $nssm stop $svc
    & $nssm remove $svc confirm
}

# Install the service
& $nssm install $svc $exe

# Set working directory
& $nssm set $svc AppDirectory "C:\DCatalog\JobWorker"

# Auto restart on crash
& $nssm set $svc Start SERVICE_AUTO_START
& $nssm set $svc AppRestartDelay 5000

# Optional: log stdout/stderr
& $nssm set $svc AppStdout "C:\DCatalog\JobWorker\logs\stdout.log"
& $nssm set $svc AppStderr "C:\DCatalog\JobWorker\logs\stderr.log"

# Ensure logs folder exists
New-Item -ItemType Directory -Force -Path "C:\DCatalog\JobWorker\logs" | Out-Null
