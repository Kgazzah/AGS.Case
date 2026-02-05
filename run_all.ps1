<#
run_all.ps1
-----------
Script d'orchestration "end-to-end" du cas d'usage.

Enchaînement:
1) init DB (dbt run-operation init_db)
2) ingestion fichiers ERP -> silver_raw (Python)
3) construction vues Silver (dbt run --select silver)
4) historisation Gold SCD2 (Python)
5) tests Silver (dbt test)

Idempotence:
- ingestion : checksum fichier + etl.batch_run
- gold : record_hash pour n'insérer une nouvelle version que si changement
#>

param(
  [switch]$InitDb = $true,
  [switch]$RunScenarios = $true,
  [switch]$Verbose = $false
)

$ErrorActionPreference = "Stop"

function Step([string]$msg) {
  Write-Host ""
  Write-Host "=== $msg ===" -ForegroundColor Cyan
}

function Run-Cmd([string]$cmd) {
  if ($Verbose) { Write-Host $cmd -ForegroundColor DarkGray }

  # Exécute la commande dans le shell courant
  iex $cmd

  # Si la commande exécutée est un binaire (python/dbt), on vérifie le code retour
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed with exit code ${LASTEXITCODE}: $cmd"
  }
}

# --- Paths ---
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot
$DataDir = Join-Path $ProjectRoot "data"

# --- Helpers ---
function Assert-File([string]$path) {
  if (!(Test-Path $path)) {
    throw "Missing file: $path"
  }
}

function Assert-Tool([string]$tool) {
  $null = Get-Command $tool -ErrorAction Stop
}

# --- Pre-checks ---
Step "Pre-checks"
Assert-Tool "python"
Assert-Tool "dbt"

Run-Cmd "python --version"
Run-Cmd "dbt --version"

# --- Optional: init DB objects via dbt macro ---
if ($InitDb) {
  Step "DB init (schemas/tables) via dbt run-operation init_db"
  Run-Cmd "dbt run-operation init_db"
}

# --- DBT silver run ---
function Run-Silver() {
  Step "DBT run (Silver views)"
  Run-Cmd "dbt run --select silver"
}

# --- Ingestion helpers ---
function Load-Silver([string]$dataset, [string]$asOf, [string]$filePath) {
  Assert-File $filePath
  Step "Ingestion: $dataset as_of=$asOf file=$(Split-Path $filePath -Leaf)"
  Run-Cmd "python -m scripts.bronze.load_file --dataset $dataset --as-of $asOf --file `"$filePath`""
}
function Generate-Scenarios() {
  $gen = Join-Path $ProjectRoot "scripts\generate_scenarios.py"
  if (Test-Path $gen) {
    Step "Generate scenario files (02/09, 10/09)"
    Run-Cmd "python -m scripts.generate_scenarios"
  } else {
    Write-Host "Skipping scenario generation (missing scripts\generate_scenarios.py)" -ForegroundColor Yellow
  }
}

# --- Gold helpers ---
function Gold-Salarie([string]$asOf) {
  Step "Gold SCD2: salarie as_of=$asOf"
  Run-Cmd "python -m scripts.gold.apply_gold_salarie --as-of $asOf"
}

function Gold-Paiement([string]$asOf) {
  Step "Gold SCD2: paiement as_of=$asOf"
  Run-Cmd "python -m scripts.gold.apply_gold_paiement --as-of $asOf"
}

# ✅ Demande ONLY : plus de batch-dataset, plus de join paiement
function Gold-Demande([string]$asOf) {
  Step "Gold SCD2: demande_avance as_of=$asOf"
  Run-Cmd "python -m scripts.gold.apply_gold_demande_avance --as-of $asOf"
}

# ------------------------------------------------------------
# MAIN FLOW
# ------------------------------------------------------------

# 25/08 - Flux initial (salaries + demandes)
Step "FLOW 25/08 - Initial"
$sal25 = Join-Path $DataDir "salaries.xlsx"
$dmd25 = Join-Path $DataDir "demandes_avance.xlsx"

Assert-File $sal25
Assert-File $dmd25

Load-Silver "salarie" "2024-08-25" $sal25
Load-Silver "demande_avance" "2024-08-25" $dmd25

Run-Silver
Gold-Salarie "2024-08-25"
Gold-Demande "2024-08-25"

# 03/09 - Flux paiement (paiements)
Step "FLOW 03/09 - Paiement"
$pay03 = Join-Path $DataDir "paiements.xlsx"
Assert-File $pay03

Load-Silver "paiement" "2024-09-03" $pay03
Run-Silver

Gold-Paiement "2024-09-03"
# ✅ On ne relance pas Gold-Demande ici : demande = demande only

# Scénarios 02/09 et 10/09 (si fichiers disponibles)
if ($RunScenarios) {

  $sal0209 = Join-Path $DataDir "salaries_2024-09-02.xlsx"
  $dmd0209 = Join-Path $DataDir "demandes_avance_2024-09-02.xlsx"
  $sal1009 = Join-Path $DataDir "salaries_2024-09-10.xlsx"
  $dmd1009 = Join-Path $DataDir "demandes_avance_2024-09-10.xlsx"

  if ((Test-Path $sal0209) -and (Test-Path $dmd0209)) {
    Step "FLOW 02/09 - Corrections + suppression"
    Load-Silver "salarie" "2024-09-02" $sal0209
    Load-Silver "demande_avance" "2024-09-02" $dmd0209
    Run-Silver
    Gold-Salarie "2024-09-02"
    Gold-Demande "2024-09-02"
  } else {
    Write-Host "Skipping 02/09 scenario (missing scenario files)." -ForegroundColor Yellow
  }

  if ((Test-Path $sal1009) -and (Test-Path $dmd1009)) {
    Step "FLOW 10/09 - Rectifications post-paiement"
    Load-Silver "salarie" "2024-09-10" $sal1009
    Load-Silver "demande_avance" "2024-09-10" $dmd1009
    Run-Silver
    Gold-Salarie "2024-09-10"
    Gold-Demande "2024-09-10"
  } else {
    Write-Host "Skipping 10/09 scenario (missing scenario files)." -ForegroundColor Yellow
  }
}

Step "DBT tests (Silver)"
Run-Cmd "dbt test --select silver"

Step "Success"
Write-Host "Orchestration completed successfully." -ForegroundColor Green
