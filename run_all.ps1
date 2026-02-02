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
  iex $cmd
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
  Run-Cmd "python -m scripts.load_silver --dataset $dataset --as-of $asOf --file `"$filePath`""
}

# --- Gold helpers ---
function Gold-Salarie([string]$asOf) {
  Step "Gold SCD2: salarie as_of=$asOf"
  Run-Cmd "python -m scripts.apply_gold_salarie --as-of $asOf"
}

function Gold-Paiement([string]$asOf) {
  Step "Gold SCD2: paiement as_of=$asOf"
  Run-Cmd "python -m scripts.apply_gold_paiement --as-of $asOf"
}

function Gold-Demande([string]$asOf, [string]$batchDataset = "demande_avance") {
  Step "Gold SCD2: demande_avance as_of=$asOf (batch-dataset=$batchDataset)"
  Run-Cmd "python -m scripts.apply_gold_demande_avance --as-of $asOf --batch-dataset $batchDataset"
}

# ------------------------------------------------------------
# MAIN FLOW
# ------------------------------------------------------------

# 25/08 - Flux initial (salaries + demandes)
Step "FLOW 25/08 - Initial"
Assert-File (Join-Path $DataDir "salaries.xlsx")
Assert-File (Join-Path $DataDir "demandes_avance.xlsx")

Load-Silver "salarie" "2024-08-25" (Join-Path $DataDir "salaries.xlsx")
Load-Silver "demande_avance" "2024-08-25" (Join-Path $DataDir "demandes_avance.xlsx")

Run-Silver
Gold-Salarie "2024-08-25"
Gold-Demande "2024-08-25" "demande_avance"

# 03/09 - Flux paiement (paiements)
Step "FLOW 03/09 - Paiement"
Assert-File (Join-Path $DataDir "paiements.xlsx")

Load-Silver "paiement" "2024-09-03" (Join-Path $DataDir "paiements.xlsx")
Run-Silver

Gold-Paiement "2024-09-03"
# Enrichissement des demandes déclenché par le flux paiement
Gold-Demande "2024-09-03" "paiement"

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
    Gold-Demande "2024-09-02" "demande_avance"
  } else {
    Write-Host "Skipping 02/09 scenario (missing scenario files)." -ForegroundColor Yellow
  }

  if ((Test-Path $sal1009) -and (Test-Path $dmd1009)) {
    Step "FLOW 10/09 - Rectifications post-paiement"
    Load-Silver "salarie" "2024-09-10" $sal1009
    Load-Silver "demande_avance" "2024-09-10" $dmd1009
    Run-Silver
    Gold-Salarie "2024-09-10"
    Gold-Demande "2024-09-10" "demande_avance"
  } else {
    Write-Host "Skipping 10/09 scenario (missing scenario files)." -ForegroundColor Yellow
  }
}

Step "DBT tests (Silver)"
Run-Cmd "dbt test --select silver"

Step "Success"
Write-Host "Orchestration completed successfully." -ForegroundColor Green
