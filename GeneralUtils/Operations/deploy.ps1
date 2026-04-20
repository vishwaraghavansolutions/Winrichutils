# deploy.ps1 - Manifest-based deployment for Winrich
# Step 1 - Copy files to server:
#   .\deploy.ps1 -Env Test -Manifest 1 -Step Copy
#   .\deploy.ps1 -Env Prod -Manifest 1 -Step Copy
#
# Step 2 - Build and deploy on server:
#   .\deploy.ps1 -Env Test -Manifest 1 -Step Deploy
#   .\deploy.ps1 -Env Prod -Manifest 1 -Step Deploy

param(
    [Parameter(Mandatory)][ValidateSet("Test","Prod")][string]$Env,
    [Parameter(Mandatory)][int]$Manifest,
    [Parameter(Mandatory)][ValidateSet("Copy","Deploy")][string]$Step
)

$ErrorActionPreference = "Stop"

# ── Config — edit KEYS_ROOT to match your local keys folder ──────────────────
$KEYS_ROOT = "C:\MyWorkSpace\Ourlife\Apps\keys"   # <-- update this for your machine

$MANIFEST_FILE = "$PSScriptRoot\Deploy_$Manifest.manifest"

$TEST_SERVER  = "ubuntu@34.223.1.167"
$TEST_KEY     = "$KEYS_ROOT\winwizedev.pem"

$PROD_SERVER  = "ubuntu@52.27.234.146"
$PROD_KEY     = "$KEYS_ROOT\winwinzeprod.pem"

$TEST_UI_REMOTE      = "/home/ubuntu/frontend/winrich-stock-manager-ui"
$TEST_BACKEND_REMOTE = "/home/ubuntu/winrich/frappe-bench/apps/stock_portfolio_management"
$BUILD_DEST          = "/var/www/prod"

$PROD_UI_REMOTE      = $TEST_UI_REMOTE
$PROD_BACKEND_REMOTE = $TEST_BACKEND_REMOTE
# ──────────────────────────────────────────────────────────────────────────────

# Resolve env-specific values
if ($Env -eq "Test") {
    $SERVER         = $TEST_SERVER
    $KEY            = $TEST_KEY
    $UI_REMOTE      = $TEST_UI_REMOTE
    $BACKEND_REMOTE = $TEST_BACKEND_REMOTE
} else {
    $SERVER         = $PROD_SERVER
    $KEY            = $PROD_KEY
    $UI_REMOTE      = $PROD_UI_REMOTE
    $BACKEND_REMOTE = $PROD_BACKEND_REMOTE
}

# SSH / SCP helpers
function Invoke-SSH($cmd) {
    $null | ssh -i $KEY -o StrictHostKeyChecking=no -o BatchMode=yes -T $SERVER $cmd
    if ($LASTEXITCODE -ne 0) { throw "SSH failed: $cmd" }
}

function Invoke-SCP($localFile, $remotePath) {
    $remoteDir = ($remotePath -replace '[^/]+$', '').TrimEnd('/')
    Invoke-SSH "mkdir -p '$remoteDir'"
    scp -i $KEY -o StrictHostKeyChecking=no -o BatchMode=yes $localFile "${SERVER}:${remotePath}"
    if ($LASTEXITCODE -ne 0) { throw "SCP failed: $localFile -> $remotePath" }
}

# ── Read manifest ─────────────────────────────────────────────────────────────
if (-not (Test-Path $MANIFEST_FILE)) {
    Write-Host "ERROR: Manifest not found: $MANIFEST_FILE" -ForegroundColor Red
    exit 1
}

$uiFiles      = @()
$backendFiles = @()

Get-Content $MANIFEST_FILE | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }

    $parts = $line -split ":", 2
    if ($parts.Count -ne 2) {
        Write-Host "  SKIP (bad format): $line" -ForegroundColor Yellow
        return
    }

    $project = $parts[0].Trim()
    $relPath  = $parts[1].Trim().Replace("/", "\")

    if ($project -eq "winrich-stock-manager-ui") {
        $uiFiles += $relPath
    } elseif ($project -eq "winrich-stock-manager") {
        $backendFiles += $relPath
    } else {
        Write-Host "  SKIP (unknown project): $line" -ForegroundColor Yellow
    }
}

if ($uiFiles.Count -eq 0 -and $backendFiles.Count -eq 0) {
    Write-Host "No files found in manifest." -ForegroundColor Yellow
    exit 0
}

# ── Banner ────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Winrich Deploy | Env: $Env | Manifest: $Manifest | Step: $Step" -ForegroundColor Cyan
Write-Host "  Server       : $SERVER" -ForegroundColor Cyan
Write-Host "  UI files     : $($uiFiles.Count)" -ForegroundColor Cyan
Write-Host "  Backend files: $($backendFiles.Count)" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# ══════════════════════════════════════════════════════
#  STEP 1 - COPY: Fetch and checkout only manifest files
# ══════════════════════════════════════════════════════
if ($Step -eq "Copy") {

    if ($backendFiles.Count -gt 0) {
        Write-Host "[Fetch] Backend repo..." -ForegroundColor Green
        Invoke-SSH "cd $BACKEND_REMOTE && git fetch origin develop"
        foreach ($rel in $backendFiles) {
            $relFwd = $rel.Replace('\', '/')
            Write-Host "  Checking out: $relFwd" -ForegroundColor Green
            Invoke-SSH "cd $BACKEND_REMOTE && git checkout origin/develop -- '$relFwd'"
        }
        Write-Host "  Backend files updated." -ForegroundColor Green
    }

    if ($uiFiles.Count -gt 0) {
        Write-Host "[Fetch] UI repo..." -ForegroundColor Green
        Invoke-SSH "cd $UI_REMOTE && git fetch origin develop"
        foreach ($rel in $uiFiles) {
            $relFwd = $rel.Replace('\', '/')
            Write-Host "  Checking out: $relFwd" -ForegroundColor Green
            Invoke-SSH "cd $UI_REMOTE && git checkout origin/develop -- '$relFwd'"
        }
        Write-Host "  UI files updated." -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "Copy complete. Now run Step 2:" -ForegroundColor Yellow
    Write-Host "  .\deploy.ps1 -Env $Env -Manifest $Manifest -Step Deploy" -ForegroundColor Yellow
    Write-Host ""
}

# ══════════════════════════════════════════════════════
#  STEP 2 - DEPLOY: Build and restart on server
# ══════════════════════════════════════════════════════
if ($Step -eq "Deploy") {

    if ($backendFiles.Count -gt 0) {
        Write-Host "[Deploy] Checking Python syntax..." -ForegroundColor Green
        foreach ($rel in $backendFiles) {
            if ($rel -match "\.py$") {
                $remotePath = "$BACKEND_REMOTE/$($rel.Replace('\', '/'))"
                Invoke-SSH "python3 -m py_compile '$remotePath' && echo '  OK: $rel'"
            }
        }
    }

    if ($uiFiles.Count -gt 0) {
        Write-Host "[Deploy] npm install..." -ForegroundColor Green
        Invoke-SSH "bash -lc 'cd $UI_REMOTE && npm install 2>&1 | tail -5'"
        Write-Host "  npm install done." -ForegroundColor Green

        Write-Host "[Deploy] Building React app (this may take ~2 min)..." -ForegroundColor Green
        Invoke-SSH "bash -lc 'cd $UI_REMOTE && GENERATE_SOURCEMAP=false CI=false npm run build:prod'"
        Write-Host "  Build complete." -ForegroundColor Green

        Write-Host "[Deploy] Copying build to $BUILD_DEST..." -ForegroundColor Green
        Invoke-SSH "sudo cp -r $UI_REMOTE/build/* $BUILD_DEST/"
        Write-Host "  Build deployed." -ForegroundColor Green
    }

    Write-Host "[Deploy] Restarting Frappe..." -ForegroundColor Green
    Invoke-SSH "pm2 restart frappe-dev || pm2 restart all"
    Write-Host "  Waiting 15s for Frappe..."
    Start-Sleep -Seconds 15

    $ping = ""
    try { $ping = Invoke-SSH "curl -s http://localhost:8000/api/method/frappe.ping" } catch {}
    if ($ping -match "pong") {
        Write-Host "  Frappe is up." -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Frappe ping failed - check pm2 logs on server." -ForegroundColor Yellow
    }
}

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Done! Env: $Env | Step: $Step" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
