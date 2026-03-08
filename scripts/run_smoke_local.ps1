Param()
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir\.. | Out-Null

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not on PATH. Install Docker Desktop and retry."
    Exit 1
}

if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Error "docker-compose not found. Ensure docker-compose is available (Docker Desktop includes it)."
    Exit 1
}

Write-Host "Starting docker-compose..."
docker-compose up -d

Write-Host "Waiting for Postgres to be ready..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $dbContainer = docker-compose ps -q db
        if ($dbContainer) {
            # Call pg_isready and ignore stdout/stderr; rely on $LASTEXITCODE to determine readiness.
            docker exec $dbContainer pg_isready -U postgres -d elite_bet_sync > $null 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "Postgres is ready"
                $ready = $true
                break
            }
        }
    } catch {
        # ignore and retry
    }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Error "Postgres did not become ready in time. Check 'docker-compose ps' for container status."
    docker-compose logs db | Select-Object -Last 50
    docker-compose down
    Exit 1
}

Write-Host "Running migrations..."
npm run migrate

Write-Host "Running smoke tests..."
npm run test:smoke

Write-Host "Tearing down docker-compose..."
docker-compose down

Pop-Location | Out-Null

Write-Host "Done."