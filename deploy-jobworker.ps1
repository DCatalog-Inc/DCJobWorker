<#
  deploy-jobworker.ps1  -  reliable, validated deploy of the DCJobWorker SQS worker.

  Why this script exists (the failures it prevents):
   1. WRONG BRANCH. DCJobWorker's own code deploys from 'search-index-hardening'
      (it has the full PDFTron page-op suite); core/DCJobs now come from the unified
      'dotnet-8-all' (search-index-hardening was merged into it + deleted 2026-06-23).
      'main' has historically been BEHIND; building from main produced an incomplete
      worker that crashed at runtime. This script syncs every repo to the correct
      branch from ORIGIN, and WARNS if DCJobWorker/main has commits not on its deploy
      branch (i.e. you committed worker code to the wrong branch).
   2. WRONG PUBLISH MODE. The worker must be published SELF-CONTAINED. A
      framework-dependent publish drops the .NET runtime AND the ~290
      Microsoft.AspNetCore.* DLLs the box needs, so the service will not start.
      This script publishes --self-contained and VALIDATES they are present.
   3. MISSING TOOL FIX. The patched PDFUtils.exe (page-insert fix) lives in
      Tools/PDFUtils/ and is not git-tracked; validated by md5 before bundling.
   4. DEPLOYING MID-JOB. Restarting the service kills in-flight jobs; this checks
      the SQS queues are drained (0 in-flight) before deploying.

  Builds from ORIGIN (push your commits first). Run from anywhere.
  Usage:  powershell -File .\deploy-jobworker.ps1            (interactive confirm)
          powershell -File .\deploy-jobworker.ps1 -Yes       (skip confirm)
#>
param([switch]$Yes)

$ErrorActionPreference = 'Stop'
$env:AWS_PROFILE = 'Release-profile'
$Region   = 'us-east-1'
$Root     = 'D:\DCatalog\GitHub'
$Bucket   = 'elasticbeanstalk-us-east-1-435155863387'
$CDApp    = 'JobWorker'
$CDGroup  = 'JobWorker-Prod'
$PdfUtilsMd5 = '38985e7af16a02fb3f8e39722f4148f0'   # the page-insert (PageInsert iterator) fix

# repo -> branch the worker build needs. core/DCJobs were UNIFIED onto dotnet-8-all
# 2026-06-23 (search-index-hardening merged in + deleted) so admin, jobs AND the worker
# now all build core/DCJobs from the SAME branch. Only DCJobWorker's own code lives on
# search-index-hardening. PDFGenerator/FlippingBook have no dotnet-8-all/sih split.
$Branches = [ordered]@{
  'DCJobWorker'  = 'search-index-hardening'
  'core'         = 'dotnet-8-all'
  'DCJobs'       = 'dotnet-8-all'
  'PDFGenerator' = 'dotnet-8-all'
  'FlippingBook' = 'main'
}

function Fail($m){ Write-Host "ABORT: $m" -ForegroundColor Red; exit 1 }
function Step($m){ Write-Host ("`n==> " + $m) -ForegroundColor Cyan }

Step "1/8  Aligning every repo to its deploy branch (from ORIGIN; local changes in these repos are reset)"
foreach ($repo in $Branches.Keys) {
  $br = $Branches[$repo]; $path = Join-Path $Root $repo
  if (-not (Test-Path $path)) { Fail "repo missing: $path" }
  Write-Host "  $repo -> $br"
  git -C $path fetch origin --quiet
  git -C $path checkout $br --quiet 2>$null
  if ($LASTEXITCODE -ne 0) { Fail "${repo}: cannot checkout $br (uncommitted changes? commit or stash first)" }
  git -C $path reset --hard "origin/$br" --quiet
}

Step "2/8  Wrong-branch guard: any DCJobWorker commits on 'main' NOT on the deploy branch?"
$dcw = Join-Path $Root 'DCJobWorker'
$stray = git -C $dcw log --oneline 'origin/search-index-hardening..origin/main' 2>$null
if ($stray) {
  Write-Host "  WARNING: origin/main has commits not on search-index-hardening:" -ForegroundColor Yellow
  $stray | ForEach-Object { Write-Host "    $_" -ForegroundColor Yellow }
  Write-Host "  If those are your worker changes, cherry-pick them onto search-index-hardening FIRST," -ForegroundColor Yellow
  Write-Host "  or this deploy will NOT include them." -ForegroundColor Yellow
  if (-not $Yes) { $a = Read-Host "  Continue anyway? (y/N)"; if ($a -ne 'y') { Fail 'stopped - reconcile the branch first' } }
}

Step "3/8  Verifying the patched PDFUtils.exe (page-insert fix)"
$pu = Join-Path $dcw 'Tools\PDFUtils\PDFUtils.exe'
if (-not (Test-Path $pu)) { Fail "PDFUtils.exe missing at $pu" }
$m = (Get-FileHash $pu -Algorithm MD5).Hash.ToLower()
if ($m -ne $PdfUtilsMd5) { Fail "PDFUtils.exe md5 $m != expected $PdfUtilsMd5 (stale tool; rebuild epaperflip/PDFUtils and copy it here)" }
Write-Host "  ok ($m)"

Step "4/8  Publishing SELF-CONTAINED (win-x64)"
$pub = Join-Path $dcw 'publish-deploy'
if (Test-Path $pub) { Remove-Item $pub -Recurse -Force }
# NOTE: call dotnet.exe explicitly and DO NOT pipe to Out-Null. On this box an
# extensionless C:\WINDOWS\system32\dotnet exists; `& dotnet ... | Out-Null`
# makes PowerShell resolve that as a *document* mid-pipeline and abort
# ("Cannot run a document in the middle of a pipeline"). Redirect to $null instead.
& dotnet.exe publish (Join-Path $dcw 'JobWorker.csproj') -c Release -r win-x64 --self-contained true -o $pub *> $null
if ($LASTEXITCODE -ne 0) { Fail 'dotnet publish failed' }

Step "5/8  Validating the published output is COMPLETE"
foreach ($f in @('JobWorker.dll','coreclr.dll','hostpolicy.dll','System.Private.CoreLib.dll','Tools\PDFUtils\PDFUtils.exe')) {
  if (-not (Test-Path (Join-Path $pub $f))) { Fail "publish missing $f (bad/incomplete build)" }
}
$aspnet = (Get-ChildItem $pub -Filter 'Microsoft.AspNetCore.*.dll' -File).Count
if ($aspnet -lt 50) { Fail "only $aspnet Microsoft.AspNetCore.*.dll present; NOT self-contained (would crash on the box)" }
$jwKB = [int]((Get-Item (Join-Path $pub 'JobWorker.dll')).Length/1KB)
if ($jwKB -lt 300) { Fail "JobWorker.dll only ${jwKB}KB; incomplete (stale branch?)" }
Write-Host "  ok: runtime + $aspnet AspNetCore dlls + JobWorker.dll ${jwKB}KB + patched PDFUtils.exe"

Step "6/8  Building the CodeDeploy bundle"
Push-Location $dcw
& python make_deploy_zip.py
Pop-Location
$zip = Get-ChildItem $dcw -Filter 'jobworker-*.zip' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $zip) { Fail 'no bundle produced' }
Write-Host "  bundle: $($zip.Name)  $([int]($zip.Length/1MB)) MB"

Step "7/8  Drain check: worker SQS queues must have 0 in-flight jobs"
foreach ($q in @('Distributed_Jobs_Queue','Distributed_Jobs_Queue_HP','Distributed_Jobs_Queue_Testing')) {
  $url = (aws sqs get-queue-url --queue-name $q --region $Region --query QueueUrl --output text 2>$null)
  if (-not $url -or $url -eq 'None') { continue }
  $inflight = (aws sqs get-queue-attributes --queue-url $url --attribute-names ApproximateNumberOfMessagesNotVisible --region $Region --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text)
  Write-Host "  $q in-flight: $inflight"
  if ([int]$inflight -gt 0) { Fail "$q has $inflight jobs in flight; wait for them to finish (do not kill in-flight page-ops)" }
}

if (-not $Yes) { $a = Read-Host ("`nReady to upload + deploy " + $zip.Name + " to $CDApp/$CDGroup (OneAtATime). Proceed? (y/N)"); if ($a -ne 'y') { Fail 'stopped by user' } }

Step "8/8  Upload + CodeDeploy (OneAtATime) + monitor"
$key = "jobworker/$($zip.Name)"
aws s3 cp $zip.FullName "s3://$Bucket/$key" --region $Region | Out-Null
$did = (aws deploy create-deployment --application-name $CDApp --deployment-group-name $CDGroup `
  --s3-location "bucket=$Bucket,key=$key,bundleType=zip" `
  --deployment-config-name CodeDeployDefault.OneAtATime `
  --description "deploy-jobworker.ps1 $(Get-Date -Format s)" `
  --region $Region --query deploymentId --output text)
Write-Host "  deployment: $did"
do {
  Start-Sleep 20
  $st = (aws deploy get-deployment --deployment-id $did --region $Region --query 'deploymentInfo.status' --output text)
  $ov = (aws deploy get-deployment --deployment-id $did --region $Region --query 'deploymentInfo.deploymentOverview.[Succeeded,InProgress,Pending,Failed]' --output text)
  Write-Host "  $st  (succ/inprog/pend/fail: $ov)"
} while ($st -notin @('Succeeded','Failed','Stopped'))
if ($st -eq 'Succeeded') { Write-Host "`nDEPLOY SUCCEEDED" -ForegroundColor Green }
else { Fail "deployment $st - check CodeDeploy console for $did" }
