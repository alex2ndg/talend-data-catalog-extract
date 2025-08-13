<# =====================================================================
 Talend-Qlik-Exports.ps1
 v3.0  (PowerShell 5.1+)

 PURPOSE
   - Export Talend Cloud catalog (artifacts, tasks, plans, connections, resources)
   - Export Observability executions for TASKS and PLANS
   - Incrementally combine into a single executions.csv (fast, offset-based)
   - Optionally deduplicate exact lines via Windows sort.exe /unique
   - Produce derived CSVs and metrics (optionally skipping daily/hourly metrics)

 HIGHLIGHTS
   - Main de-dupe during paging is by runId/id (per page, in memory).
   - Optional post-combine de-dupe uses sort.exe /unique, removing true
     byte-identical duplicates deterministically and very fast.
   - Executions export uses a fixed window per run: [now-DeltaDays, now]
     plus a configurable overlap (ExecOverlapMinutes) to avoid gaps.

 AUTH
   - Talend PAT (Personal Access Token) is required:
       - Pass with -Pat "...", or
       - Set env var TALEND_PAT
   - Script aborts if PAT is missing.

 OUTPUTS (default under .\talend_exports\):
   - artifacts.csv / tasks.csv / plans.csv / task_plan.csv / plan_steps.csv
   - connections.csv / resources.csv
   - executions_tasks.csv / executions_plans.csv / executions.csv
   - observability_components.csv (if -ComponentsDays > 0)
   - derived: task_health_summary.csv, plan_health_summary.csv,
              top_errors.csv, queue_latency.csv, orphan_tasks.csv,
              artifact_drift.csv
   - metrics: metrics_counts.csv, metrics_exec_daily.csv,
              metrics_exec_hourly.csv, metrics_ko_recurrent_6m.csv

 PARAMETERS (most useful)
   -Mode: all | catalog | executions | observability | derived | metrics
   -FullRescan: force a full re-export (ignores execution anchors)
   -DeltaDays: time window (days back) for executions
   -UseSortLineDedup: run a fast exact-line dedup on executions.csv
   -DisableExecDailyHourly: skip daily/hourly exec metrics
   -EnvIncludeNames / EnvIncludeIds: filter environments to export
   -ExecOverlapMinutes: overlap in minutes to avoid pagination gaps
   -RecurrentKOThreshold: min failures in 6m window to list recurrent KOs

 REQUIREMENTS
   - Windows (uses %SystemRoot%\System32\sort.exe)
   - PowerShell 5.1+ (or compatible)
   - Network access to https://api.<region>.cloud.talend.com

 ===================================================================== #>

param(
  # --- Talend / basics ---
  [string]$RegionApi = 'https://api.eu.cloud.talend.com',
  [string]$Pat = $env:TALEND_PAT,
  [string]$OutDir = (Join-Path -Path $PSScriptRoot -ChildPath 'talend_exports'),

  # What to run
  [ValidateSet('all','catalog','executions','observability','derived','metrics')]
  [string[]]$Mode = @('all'),

  # Extraction / performance
  [switch]$FullRescan,
  [int]$DeltaDays = 1,
  [int]$HttpTimeoutSec = 120,
  [int]$SleepMsBetweenPages = 400,
  [int]$MaxExecCsvRowsPerPage = 100000,
  [int]$MaxListPageSize = 100,

  # Observability (component metrics)
  [int]$ComponentsDays = 0,

  # Tracing
  [switch]$TraceCatalog,

  # Environment filters (optional)
  [string[]]$EnvIncludeNames = @('DEV_8','PRO_8'),
  [string[]]$EnvIncludeIds   = @(),

  # Post-process / metrics
  [switch]$DisableExecDailyHourly,
  [switch]$UseSortLineDedup,
  [switch]$IgnoreExecAnchors,
  [int]$ExecOverlapMinutes = 15,
  [int]$RecurrentKOThreshold = 2,   # recurrent KO threshold in 6 months

  # Final copy (optional). Leave empty to skip.
  [string]$ExportCopyDir = ''
)

# --- Base config & safety ---
$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13

# PAT required
if ([string]::IsNullOrWhiteSpace($Pat)) {
  throw "Missing Talend PAT. Pass -Pat or set TALEND_PAT environment variable."
}

# UTF-8, PS 5.1 friendly
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$OutputEncoding = $utf8NoBom
$PSDefaultParameterValues = @{
  'Out-File:Encoding'     = 'utf8'
  'Set-Content:Encoding'  = 'utf8'
  'Add-Content:Encoding'  = 'utf8'
  'Export-Csv:Encoding'   = 'utf8'
}
try { if([type]::GetType('System.Console')){ [System.Console]::OutputEncoding = $utf8NoBom } } catch {}

# Folders
Write-Host "== Talend-Qlik Exports (API: $RegionApi)" -ForegroundColor Cyan
Write-Host "== Output: $OutDir" -ForegroundColor Cyan
$global:STATE_DIR = Join-Path $OutDir '.state'
$null = New-Item -ItemType Directory -Force -Path $OutDir, $STATE_DIR | Out-Null

# ======================== GENERIC UTILITIES =========================

function Get-AuthHeaders {
  <#
    .SYNOPSIS
      HTTP headers with PAT and JSON.
  #>
  @{ 'Authorization'="Bearer $Pat"; 'Accept'='application/json'; 'Content-Type'='application/json' }
}

function NowUtc(){ [DateTime]::UtcNow }
function Get-Millis([DateTime]$dt){ [int64]([DateTimeOffset]$dt).ToUnixTimeMilliseconds() }

function Coalesce([object[]]$vals){
  foreach($v in $vals){ if($null -ne $v -and ($v -isnot [string] -or $v -ne '')){ return $v } }
  return $null
}

function Select-Environments([array]$envs){
  <#
    .SYNOPSIS
      Filter environments by name or id if provided.
  #>
  $res = $envs
  if($EnvIncludeNames -and $EnvIncludeNames.Count -gt 0){
    $set = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    foreach($n in $EnvIncludeNames){ [void]$set.Add([string]$n) }
    $res = $res | Where-Object { $_.name -and $set.Contains([string]$_.name) }
  }
  if($EnvIncludeIds -and $EnvIncludeIds.Count -gt 0){
    $set2 = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    foreach($n in $EnvIncludeIds){ [void]$set2.Add([string]$n) }
    $res = $res | Where-Object { $_.id -and $set2.Contains([string]$_.id) }
  }
  return ,@($res)
}

function Copy-FinalExports {
  <#
    .SYNOPSIS
      Copy final CSVs to an external folder (if provided).
  #>
  param([string]$DestDir)

  if ([string]::IsNullOrWhiteSpace($DestDir)) {
    Write-Host ">> Final copy skipped: ExportCopyDir not set." -ForegroundColor DarkYellow
    return
  }

  $files = @(
    'artifacts.csv',
    'connections.csv',
    'executions.csv',
    'metrics_ko_recurrent_6m.csv',
    'plans.csv',
    'tasks.csv'
  )

  try {
    $null = New-Item -ItemType Directory -Force -Path $DestDir
    foreach ($name in $files) {
      $src = Join-Path $OutDir $name
      if (Test-Path -LiteralPath $src) {
        $dst = Join-Path $DestDir $name
        Copy-Item -LiteralPath $src -Destination $dst -Force
        Write-Host ("   [Copy] {0} -> {1}" -f $name, $dst) -ForegroundColor DarkGray
      } else {
        Write-Host ("   [Copy] Skipped (missing): {0}" -f $name) -ForegroundColor DarkYellow
      }
    }
    Write-Host (">> Final copy completed: {0}" -f $DestDir) -ForegroundColor Green
  } catch {
    Write-Warning ("[Copy] Error copying to {0}: {1}" -f $DestDir, $_.Exception.Message)
  }
}

function Test-WorkspaceAccess([string]$EnvironmentId){
  <#
    .SYNOPSIS
      Try listing workspaces to confirm PAT visibility for the env.
  #>
  try{
    $ws = Get-Workspaces -environmentId $EnvironmentId
    return ($ws -and $ws.Count -gt 0)
  }catch{ return $false }
}

function Dedup-ExecCsvByLineSort {
  <#
    .SYNOPSIS
      Exact line-level dedup of executions.csv using sort.exe /unique.
    .NOTES
      - Header line is preserved.
      - Body is sorted; downstream tools can restore temporal order if needed.
  #>
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) { return }

  $dir      = Split-Path -Parent $Path
  $tmpBody  = Join-Path $dir ("tmp_body_"   + ([guid]::NewGuid().ToString('N')) + ".txt")
  $tmpOut   = Join-Path $dir ("tmp_out_"    + ([guid]::NewGuid().ToString('N')) + ".csv")
  $tmpSorted= Join-Path $dir ("tmp_sorted_" + ([guid]::NewGuid().ToString('N')) + ".txt")

  $header = (Get-Content -LiteralPath $Path -Encoding UTF8 -TotalCount 1)

  Get-Content -LiteralPath $Path -Encoding UTF8 | Select-Object -Skip 1 |
    Set-Content -LiteralPath $tmpBody -Encoding UTF8

  $sortExe = Join-Path $env:SystemRoot 'System32\sort.exe'
  if (-not (Test-Path $sortExe)) { $sortExe = 'sort.exe' }
  Start-Process -FilePath $sortExe `
    -ArgumentList '/unique', "`"$tmpBody`"" `
    -NoNewWindow -Wait -RedirectStandardOutput $tmpSorted

  Set-Content -LiteralPath $tmpOut -Encoding UTF8 -Value $header
  Get-Content -LiteralPath $tmpSorted -Encoding UTF8 |
    Add-Content -LiteralPath $tmpOut -Encoding UTF8

  Move-Item -LiteralPath $tmpOut -Destination $Path -Force
  Remove-Item -LiteralPath $tmpBody,$tmpSorted -Force -ErrorAction SilentlyContinue

  Write-Host ">> Dedup (exact line) completed with sort.exe /unique" -ForegroundColor Green
}

function Get-RunIdSet([string]$Kind,[string]$EnvironmentId){
  <#
    .SYNOPSIS
      Persistent set of seen runIds by type (tasks|plans) and environment.
  #>
  $file = Join-Path $global:STATE_DIR ("seen_runids_{0}_{1}.txt" -f $Kind,$EnvironmentId)
  $set  = New-Object System.Collections.Generic.HashSet[string]
  if(Test-Path $file){
    try{
      Get-Content -LiteralPath $file -Encoding UTF8 | ForEach-Object { if($_){ [void]$set.Add($_) } }
    }catch{}
  }
  return @{ File=$file; Set=$set; Kind=$Kind; Env=$EnvironmentId }
}

function Save-RunIdSet($Info){
  <#
    .SYNOPSIS
      Persist the seen set (sorted) to disk.
  #>
  try{
    $tmp = [System.IO.Path]::GetTempFileName()
    $Info.Set | Sort-Object | Set-Content -LiteralPath $tmp -Encoding UTF8
    Move-Item -LiteralPath $tmp -Destination $Info.File -Force
  }catch{}
}

function Ensure-ExecutionsCombinedHeader {
  <#
    .SYNOPSIS
      Ensure executions.csv exists with a valid header (borrowed from tasks/plans).
  #>
  param([string]$CombinedPath,[string]$TasksCsv,[string]$PlansCsv)
  if (Test-Path $CombinedPath) { return }
  $header = $null
  foreach($src in @($TasksCsv,$PlansCsv)){
    if (Test-Path $src) {
      $lines = Get-Content -LiteralPath $src -Encoding UTF8 -TotalCount 1
      if ($lines -and $lines.Count -gt 0) { $header = $lines[0]; break }
    }
  }
  if ($header) { Set-Content -LiteralPath $CombinedPath -Encoding UTF8 -Value $header }
}

function Select-CsvByNewRunIds {
  <#
    .SYNOPSIS
      From a downloaded CSV page, append ONLY rows with unseen runId/id
      to the destination CSV and update the persistent set.
    .OUTPUTS
      @{ hadData=bool; appended=int; lastId=string }
  #>
  param(
    [string]$Src,
    [string]$Dst,
    $SeenInfo
  )
  $result = @{ hadData=$false; appended=0; lastId=$null }
  if(-not (Test-Path $Src)){ return $result }

  $fmt  = Get-CsvFormat -CsvPath $Src
  $rows = @()
  try { $rows = Import-Csv -Path $Src -Delimiter $fmt.Delim } catch { return $result }
  if(-not $rows -or $rows.Count -eq 0){ return $result }
  $result.hadData = $true

  $toAdd = New-Object System.Collections.Generic.List[object]
  foreach($r in $rows){
    $rid = if($r.PSObject.Properties.Name -contains 'runId' -and $r.runId){ [string]$r.runId }
           elseif($r.PSObject.Properties.Name -contains 'id' -and $r.id){ [string]$r.id }
           else { $null }
    if($rid){ $result.lastId = $rid }
    if($rid -and -not $SeenInfo.Set.Contains($rid)){
      [void]$SeenInfo.Set.Add($rid)
      $toAdd.Add($r)
    }
  }

  if($toAdd.Count -gt 0){
    if(Test-Path $Dst){ $toAdd | Export-Csv -Path $Dst -Append -NoTypeInformation -Encoding UTF8 }
    else              { $toAdd | Export-Csv -Path $Dst         -NoTypeInformation -Encoding UTF8 }
    $result.appended = $toAdd.Count
  }

  Save-RunIdSet $SeenInfo
  return $result
}

function Get-State($name,$def=$null){
  <#
    .SYNOPSIS
      Read .state\<name>.json if present, else return default.
  #>
  $f=Join-Path $STATE_DIR "$name.json"
  if(Test-Path $f){ try{ Get-Content $f -Raw | ConvertFrom-Json } catch { $def } } else { $def }
}
function Set-State($name,$obj){
  <#
    .SYNOPSIS
      Persist JSON state to .state\<name>.json
  #>
  $f=Join-Path $STATE_DIR "$name.json"
  ($obj | ConvertTo-Json -Depth 10) | Out-File -Encoding UTF8 -FilePath $f
}

function Invoke-TalendJson {
  <#
    .SYNOPSIS
      JSON calls (GET/POST/...) with retries for 429/5xx/timeouts.
  #>
  param(
    [ValidateSet('GET','POST','PUT','PATCH','DELETE')] [string]$Method,
    [string]$Path,
    [hashtable]$Query,
    $Body,
    [int]$Retry = 6
  )

  $uri = [System.UriBuilder]::new("$RegionApi$Path")
  if ($Query) {
    $pairs=@()
    foreach($k in $Query.Keys){
      $pairs += ("{0}={1}" -f [Uri]::EscapeDataString($k), [Uri]::EscapeDataString([string]$Query[$k]))
    }
    $uri.Query = ($pairs -join '&')
  }

  $json = $null
  if ($PSBoundParameters.ContainsKey('Body') -and $null -ne $Body) {
    $json = ($Body | ConvertTo-Json -Depth 12)
  }

  $timeout = 120
  try { if ($HttpTimeoutSec -and [int]$HttpTimeoutSec -gt 0) { $timeout = [int]$HttpTimeoutSec } } catch {}

  $try = 0
  while ($true) {
    try {
      if ($TraceCatalog) { Write-Host ("   [HTTP {0}] {1}" -f $Method, $uri.Uri.AbsoluteUri) -ForegroundColor DarkGray }
      return Invoke-RestMethod -Method $Method -Uri $uri.Uri.AbsoluteUri `
        -Headers (Get-AuthHeaders) -Body $json -TimeoutSec $timeout

    } catch {
      $resp = $_.Exception.Response
      $code = if($resp){ try { [int]$resp.StatusCode } catch { 0 } } else { 0 }
      $raw = ''
      if ($resp -and $resp.GetResponseStream()) {
        try { $sr = New-Object IO.StreamReader($resp.GetResponseStream()); $raw = $sr.ReadToEnd() } catch {}
      }

      $isTimeout = $false
      try {
        if ($_.Exception -is [System.Net.WebException] -and $_.Exception.Status -eq [System.Net.WebExceptionStatus]::Timeout) { $isTimeout = $true }
      } catch {}
      if ($code -eq 408) { $isTimeout = $true }
      if ($code -eq 0 -and ($_.Exception.Message -match 'timed out|timeout')) { $isTimeout = $true }

      if ( ($code -eq 429 -or ($code -ge 500 -and $code -lt 600) -or $isTimeout) -and ($try -lt $Retry) ) {
        $waitSec = 0
        try {
          $ra = if($resp){ $resp.Headers['Retry-After'] } else { $null }
          if ($ra) { if (-not [double]::TryParse([string]$ra, [ref]([double]$waitSec))) { $waitSec = 5 } }
        } catch { $waitSec = 0 }
        if ($waitSec -le 0) { $waitSec = [Math]::Min(60, [int][Math]::Ceiling([math]::Pow(2, $try))) }
        $suffix = $(if($isTimeout){'/timeout'}else{''})
        Write-Host ("   [RETRY] {0} -> HTTP {1}{2}. Waiting {3}s (try {4}/{5})." -f $uri.Uri.AbsoluteUri, $code, $suffix, $waitSec, ($try+1), $Retry) -ForegroundColor DarkYellow
        Start-Sleep -Seconds $waitSec
        Start-Sleep -Milliseconds (Get-Random -Minimum 200 -Maximum 800)
        $try++; continue
      }

      throw "HTTP $Method $($uri.Uri.AbsoluteUri) failed: $($_.Exception.Message)`n$raw"
    }
  }
}

function Invoke-TalendCsvDownload {
  <#
    .SYNOPSIS
      CSV POST with Accept text/csv (retries). Used by:
      /monitoring/observability/executions/download
  #>
  param([string]$Path, $Body, [string]$OutFile)

  $json = ($Body | ConvertTo-Json -Depth 12)
  $uri  = "$RegionApi$Path"

  $timeout = 120
  try { if ($HttpTimeoutSec -and [int]$HttpTimeoutSec -gt 0) { $timeout = [int]$HttpTimeoutSec } } catch {}

  $maxRetry = 4
  $try = 0

  while ($true) {
    try {
      if ($TraceCatalog) { Write-Host "   [CSV POST] $uri (Accept=text/csv) -> $OutFile" -ForegroundColor DarkGray }
      $headers = Get-AuthHeaders
      $headers['Accept'] = 'text/csv'
      Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $json -OutFile $OutFile -TimeoutSec $timeout | Out-Null
      return
    } catch {
      $resp = $_.Exception.Response
      $code = 0; if ($resp) { try { $code = [int]$resp.StatusCode } catch {} }

      $raw = ''
      if ($resp -and $resp.GetResponseStream()) {
        try { $sr = New-Object IO.StreamReader($resp.GetResponseStream()); $raw = $sr.ReadToEnd() } catch {}
      }

      $isTimeout = $false
      try {
        if ($_.Exception -is [System.Net.WebException] -and $_.Exception.Status -eq [System.Net.WebExceptionStatus]::Timeout) { $isTimeout = $true }
      } catch {}
      if ($code -eq 408) { $isTimeout = $true }
      if ($code -eq 0 -and ($_.Exception.Message -match 'timed out|timeout')) { $isTimeout = $true }

      if ($code -eq 406) {
        if ($TraceCatalog) { Write-Host "   [CSV RETRY] $uri Accept=text/csv => 406; trying Accept=*/*..." -ForegroundColor DarkYellow }
        try {
          $headers = Get-AuthHeaders; $headers['Accept'] = '*/*'
          Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $json -OutFile $OutFile -TimeoutSec $timeout | Out-Null
          return
        } catch {
          $resp2 = $_.Exception.Response
          $raw2 = ''
          if ($resp2 -and $resp2.GetResponseStream()) {
            try { $sr2 = New-Object IO.StreamReader($resp2.GetResponseStream()); $raw2 = $sr2.ReadToEnd() } catch {}
          }
          throw "CSV download POST $uri failed: $($_.Exception.Message)`n$raw2"
        }
      }

      if ( ($code -eq 429 -or ($code -ge 500 -and $code -lt 600) -or $isTimeout) -and ($try -lt $maxRetry) ) {
        $waitSec = 0
        try {
          $ra = if($resp){ $resp.Headers['Retry-After'] } else { $null }
          if ($ra) { if (-not [double]::TryParse([string]$ra, [ref]([double]$waitSec))) { $waitSec = 5 } }
        } catch { $waitSec = 0 }
        if ($waitSec -le 0) { $waitSec = [Math]::Min(60, [int][Math]::Ceiling([math]::Pow(2, $try))) }
        $suffix = $(if($isTimeout){'/timeout'}else{''})
        Write-Host ("   [CSV RETRY] {0} -> HTTP {1}{2}. Waiting {3}s (try {4}/{5})." -f $uri, $code, $suffix, $waitSec, ($try+1), $maxRetry) -ForegroundColor DarkYellow
        Start-Sleep -Seconds $waitSec
        Start-Sleep -Milliseconds (Get-Random -Minimum 200 -Maximum 800)
        $try++; continue
      }

      throw "CSV download POST $uri failed: $($_.Exception.Message)`n$raw"
    }
  }
}

function Get-CsvFormat {
  <#
    .SYNOPSIS
      Detect delimiter (',' or ';') and id/runId index from the header.
  #>
  param([string]$CsvPath)
  $lines = Get-Content -Path $CsvPath -Encoding UTF8 -TotalCount 1
  if(-not $lines -or $lines.Count -lt 1){ return @{ Delim=','; IdIndex=-1; IdName='id' } }
  $header=$lines[0]
  $semi=($header -split ';').Count; $comma=($header -split ',').Count
  $delim = if ($semi -gt $comma) {';'} else {','}
  $cols=$header -split [regex]::Escape($delim)

  $idIndex=-1; $idName='id'
  for($i=0;$i -lt $cols.Length;$i++){ if($cols[$i].Trim('"') -eq 'id'){ $idIndex=$i; $idName='id'; break } }
  if($idIndex -lt 0){ for($i=0;$i -lt $cols.Length;$i++){ if($cols[$i].Trim('"') -eq 'runId'){ $idIndex=$i; $idName='runId'; break } } }

  return @{ Delim=$delim; IdIndex=$idIndex; IdName=$idName }
}

function Remove-ObservabilityComponentsCsv {
  <#
    .SYNOPSIS
      Clean observability_components.csv:
      - drop “empty” rows (no useful data)
      - remove exact duplicates by key fields
  #>
  param([string]$CsvPath)

  if(-not (Test-Path $CsvPath)){ return }

  $fmt = Get-CsvFormat -CsvPath $CsvPath

  $rows = @()
  try { $rows = Import-Csv -Path $CsvPath -Delimiter $fmt.Delim } catch {
    Write-Warning ("Clean-ObservabilityComponentsCsv: cannot read {0} ({1})" -f (Split-Path $CsvPath -Leaf), $_.Exception.Message)
    return
  }

  $total = $rows.Count
  if($total -le 1){ return }

  $useful = $rows | Where-Object {
    ($_.runId -and $_.runId.Trim() -ne '') -or
    ($_.message -and $_.message.Trim() -ne '') -or
    ($_.component -and $_.component.Trim() -ne '') -or
    ($_.executableId -and $_.executableId.Trim() -ne '') -or
    ($_.executableName -and $_.executableName.Trim() -ne '') -or
    ($_.timestamp -and $_.timestamp.Trim() -ne '') -or
    ($_.level -and $_.level.Trim() -ne '')
  }

  $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)
  $filtered = New-Object System.Collections.Generic.List[object]
  foreach($r in $useful){
    $key = "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}" -f
      $r.environmentId,$r.environmentName,$r.runId,$r.executableId,$r.executableName,$r.component,$r.level,$r.timestamp
    if(-not $seen.Contains($key)){ [void]$seen.Add($key); $filtered.Add($r) }
  }

  $kept = $filtered.Count
  if($kept -eq $total){
    Write-Host ("   [CleanObs] {0}: no changes (rows {1})." -f (Split-Path $CsvPath -Leaf), $total) -ForegroundColor DarkGray
    return
  }

  $tmp = [IO.Path]::Combine([IO.Path]::GetDirectoryName($CsvPath), "tmp_clean_obs_{0}.csv" -f ([Guid]::NewGuid().ToString('N')))
  $filtered | Export-Csv -Path $tmp -NoTypeInformation -Encoding UTF8
  Move-Item -LiteralPath $tmp -Destination $CsvPath -Force

  Write-Host ("   [CleanObs] {0}: total={1}, kept={2}, removed={3}" -f (Split-Path $CsvPath -Leaf), $total, $kept, ($total-$kept)) -ForegroundColor DarkGray
}

function Test-CsvHasDataRows {
  <#
    .SYNOPSIS
      True if CSV has header + ≥1 data row (reads only two lines).
  #>
  param([string]$CsvPath)
  try{
    $lines = Get-Content -Path $CsvPath -Encoding UTF8 -TotalCount 2
    return ($lines.Count -gt 1)
  }catch{ return $false }
}

function Combine-ExecutionsCsvFast {
  <#
    .SYNOPSIS
      Incremental combine of executions_tasks.csv + executions_plans.csv
      copying only the delta (by byte offset) into executions.csv.
    .STATE
      .state\combine_offsets.json  -> { tasksLen, plansLen }
  #>
  $tasks = Join-Path $OutDir 'executions_tasks.csv'
  $plans = Join-Path $OutDir 'executions_plans.csv'
  $out   = Join-Path $OutDir 'executions.csv'

  Ensure-ExecutionsCombinedHeader -CombinedPath $out -TasksCsv $tasks -PlansCsv $plans

  $state = Get-State 'combine_offsets' $null
  $needBootstrap = $false
  if ($null -eq $state -or -not ($state.PSObject.Properties.Name -contains 'tasksLen') -or -not ($state.PSObject.Properties.Name -contains 'plansLen')) {
    $state = @{ tasksLen = 0L; plansLen = 0L }
    $needBootstrap = $true
  }

  $hasOutData = Test-CsvHasDataRows -CsvPath $out
  if ($needBootstrap -and $hasOutData) {
    $tLen = 0L; if (Test-Path $tasks) { $tLen = [int64](Get-Item -LiteralPath $tasks).Length }
    $pLen = 0L; if (Test-Path $plans) { $pLen = [int64](Get-Item -LiteralPath $plans).Length }
    $state.tasksLen = $tLen
    $state.plansLen = $pLen
    Set-State 'combine_offsets' $state
    Write-Host "[Combine] Bootstrap: executions.csv already has data. Advancing offsets, no copy." -ForegroundColor DarkYellow
    Write-Host ("   [Combine] Offsets saved: tasksLen={0}, plansLen={1}" -f $state.tasksLen,$state.plansLen) -ForegroundColor DarkGray
    Write-Host ">> OK executions.csv combined (bootstrap, no copy)" -ForegroundColor Green
    return
  }

  function Append-Delta {
    param([string]$Kind,[string]$Src,[Int64]$LastLen)

    if (-not (Test-Path $Src)) {
      Write-Host ("   [Combine] {0}: source missing." -f $Kind) -ForegroundColor DarkGray
      return $LastLen
    }

    $curLen = [Int64](Get-Item -LiteralPath $Src).Length
    $base   = [Math]::Max(0,$LastLen)
    if ($curLen -le $base) {
      Write-Host ("   [Combine] {0}: unchanged (curLen={1}, lastLen={2})" -f $Kind,$curLen,$LastLen) -ForegroundColor DarkGray
      return $curLen
    }

    $bytesToCopy = $curLen - $base
    Write-Host ("   [Combine] {0}: copying ~{1:N0} bytes delta..." -f $Kind,$bytesToCopy) -ForegroundColor DarkGray

    $fsIn  = [System.IO.File]::Open($Src, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read,  [System.IO.FileShare]::ReadWrite)
    $fsOut = [System.IO.File]::Open($out, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::Write, [System.IO.FileShare]::Read)
    try {
      $enc = $utf8NoBom; if (-not $enc) { $enc = New-Object System.Text.UTF8Encoding($false) }
      $sr  = New-Object System.IO.StreamReader($fsIn,  $enc, $true)
      $sw  = New-Object System.IO.StreamWriter($fsOut, $enc)
      try {
        if ($base -gt 0) { $fsIn.Position = $base } else { [void]$sr.ReadLine() }  # skip header
        $fsOut.Position = $fsOut.Length

        $added = 0
        while (($line = $sr.ReadLine()) -ne $null) {
          $sw.WriteLine($line)
          $added++
          if (($added % 500000) -eq 0) { $sw.Flush() }
        }
        Write-Host ("   [Combine] {0}: rows appended={1}" -f $Kind,$added) -ForegroundColor DarkGray
      } finally { try { $sw.Flush() } catch {}; try { $sw.Close() } catch {}; try { $sr.Close() } catch {} }
    } finally { try { $fsOut.Close() } catch {}; try { $fsIn.Close() } catch {} }

    return $curLen
  }

  $state.tasksLen = Append-Delta -Kind 'tasks' -Src $tasks -LastLen ([Int64]$state.tasksLen)
  $state.plansLen = Append-Delta -Kind 'plans' -Src $plans -LastLen ([Int64]$state.plansLen)

  Set-State 'combine_offsets' $state
  Write-Host ("   [Combine] Offsets saved: tasksLen={0}, plansLen={1}" -f $state.tasksLen,$state.plansLen) -ForegroundColor DarkGray
  Write-Host ">> OK executions.csv combined" -ForegroundColor Green
}

# ======================== DISCOVERY (ENV / WS) =========================

function Get-Environments {
  <#
    .SYNOPSIS
      List environments (robust paging) + filter with Select-Environments.
  #>
  $items=@(); $offset=0
  while($true){
    $page=Invoke-TalendJson -Method GET -Path "/orchestration/environments" -Query @{ offset=$offset; limit=$MaxListPageSize }
    if($null -eq $page){ break }
    if(($page -is [array]) -or ($page.psobject.Properties.Name -notcontains 'items')){ $items+=$page } else { $items+=$page.items }
    if(($page -is [array]) -and $page.Count -lt $MaxListPageSize){break}
    if(($page.psobject.Properties.Name -contains 'total') -and [int]$page.total -le ($offset+$MaxListPageSize)){ break }
    $offset+=$MaxListPageSize; Start-Sleep -Milliseconds $SleepMsBetweenPages
  }
  return (Select-Environments $items)
}

function Get-Workspaces([string]$environmentId){
  <#
    .SYNOPSIS
      List workspaces in an environment (robust paging).
  #>
  $res=@(); $offset=0
  $pageLimit = $MaxListPageSize
  $maxPagesGuard = 2000

  while($true){
    $page = Invoke-TalendJson -Method GET -Path "/orchestration/workspaces" `
             -Query @{ environmentId=$environmentId; offset=$offset; limit=$pageLimit }

    if ($null -eq $page) {
      Write-Host ("   [WS] env={0} offset={1} -> NULL (stop)" -f $environmentId,$offset) -ForegroundColor DarkYellow
      break
    }

    if ($page -is [array]) {
      $count = $page.Count
      Write-Host ("   [WS] env={0} offset={1} -> array items={2}" -f $environmentId,$offset,$count) -ForegroundColor DarkGray
      if ($count -eq 0) { break }
      $res += $page
      if ($count -lt $pageLimit) { break }
    }
    elseif ($page.psobject.Properties.Name -contains 'items') {
      $items = $page.items
      $count = if($items){ $items.Count } else { 0 }
      Write-Host ("   [WS] env={0} offset={1} -> items={2}" -f $environmentId,$offset,$count) -ForegroundColor DarkGray
      if ($count -eq 0) { break }
      $res += $items
      if ($page.psobject.Properties.Name -contains 'total') {
        $total = [int]$page.total
        if ($total -le ($offset + $pageLimit)) { break }
      }
      if ($count -lt $pageLimit) { break }
    }
    else {
      Write-Host ("   [WS] env={0} offset={1} -> single object (no 'items')" -f $environmentId,$offset) -ForegroundColor DarkGray
      $res += $page
      break
    }

    $offset += $pageLimit
    if (($offset / $pageLimit) -gt $maxPagesGuard) {
      Write-Warning ("Get-Workspaces: exceeded guard of {0} pages in env {1}. Stopping." -f $maxPagesGuard, $environmentId)
      break
    }
    Start-Sleep -Milliseconds $SleepMsBetweenPages
  }

  return $res
}

# ============================ 1) CATALOG ==============================

function Export-Catalog {
  <#
    .SYNOPSIS
      Export artifacts, tasks, plans (+steps & task_plan mapping),
      connections and resources by environment/workspace.
  #>
  Write-Host ">> Catalog..." -ForegroundColor Yellow

  $fA   = Join-Path $OutDir 'artifacts.csv'
  $fT   = Join-Path $OutDir 'tasks.csv'
  $fP   = Join-Path $OutDir 'plans.csv'
  $fTP  = Join-Path $OutDir 'task_plan.csv'
  $fPS  = Join-Path $OutDir 'plan_steps.csv'
  $fConn= Join-Path $OutDir 'connections.csv'
  $fRes = Join-Path $OutDir 'resources.csv'

  foreach($p in @($fA,$fT,$fP,$fTP,$fPS,$fConn,$fRes)){ if(Test-Path $p){ Remove-Item $p -Force } }

  $envs = Get-Environments

  foreach($env in $envs){
    $envId = $env.id
    $envName = Coalesce @($env.name,$envId)

    # Workspaces
    $wss = @()
    $offset = 0
    while($true){
      Write-Host ("   [HTTP GET] {0}/orchestration/workspaces?offset={1}&environmentId={2}&limit={3}" -f $RegionApi,$offset,$envId,$MaxListPageSize) -ForegroundColor DarkGray
      $page = Invoke-TalendJson -Method GET -Path "/orchestration/workspaces" -Query @{ environmentId=$envId; offset=$offset; limit=$MaxListPageSize }
      $items = @()
      if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }
      Write-Host ("   [WS] env={0} offset={1} -> array items={2}" -f $envId,$offset, ($items|Measure-Object).Count)
      $wss += $items
      if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
      $offset += $MaxListPageSize
      Start-Sleep -Milliseconds $SleepMsBetweenPages
    }

    # Artifacts per workspace
    foreach($ws in $wss){
      $wsId   = $ws.id
      $wsName = Coalesce @($ws.name,$wsId)
      Write-Host ("   [Artifacts] env={0} ws={1}" -f $envName,$wsName)
      $offset = 0
      while($true){
        Write-Host ("   [HTTP GET] {0}/orchestration/artifacts?offset={1}&environmentId={2}&workspaceId={3}&limit={4}" -f $RegionApi,$offset,$envId,$wsId,$MaxListPageSize) -ForegroundColor DarkGray
        $page = Invoke-TalendJson -Method GET -Path "/orchestration/artifacts" -Query @{ environmentId=$envId; workspaceId=$wsId; limit=$MaxListPageSize; offset=$offset }
        $items=@(); if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }
        $rows=@()
        foreach($it in $items){
          $rows += [pscustomobject]@{
            environmentId=$envId; environmentName=$envName;
            workspaceId=$wsId;    workspaceName=$wsName;
            artifactId=$it.id;    artifactName=$it.name; type=$it.type;
            versions=((($it.versions) -join '|'));
            createDate=$it.createDate; description=$it.description
          }
        }
        if($rows.Count -gt 0){
          $rows | Export-Csv -Path $fA -Append:([bool](Test-Path $fA)) -NoTypeInformation -Encoding UTF8
        }
        if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
        $offset += $MaxListPageSize
        Start-Sleep -Milliseconds $SleepMsBetweenPages
      }
    }

    # Connections
    Write-Host ("   [Connections] env={0}" -f $envName)
    $offset=0
    while($true){
      Write-Host ("   [HTTP GET] {0}/orchestration/connections?offset={1}&environmentId={2}&limit={3}" -f $RegionApi,$offset,$envId,$MaxListPageSize) -ForegroundColor DarkGray
      $page = Invoke-TalendJson -Method GET -Path "/orchestration/connections" -Query @{ environmentId=$envId; limit=$MaxListPageSize; offset=$offset }
      $items=@(); if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }
      $rows=@()
      foreach($it in $items){
        $rows += [pscustomobject]@{
          environmentId=$envId; environmentName=$envName;
          connectionId=$it.id; connectionName=$it.name; type=$it.type;
          workspaceId=$it.workspace.id; workspaceName=$it.workspace.name;
          created=$it.createDate; updated=$it.updateDate
        }
      }
      if($rows){ $rows | Export-Csv -Path $fConn -Append:([bool](Test-Path $fConn)) -NoTypeInformation -Encoding UTF8 }
      if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
      $offset += $MaxListPageSize
      Start-Sleep -Milliseconds $SleepMsBetweenPages
    }

    # Resources
    Write-Host ("   [Resources] env={0}" -f $envName)
    $offset=0
    while($true){
      Write-Host ("   [HTTP GET] {0}/orchestration/resources?offset={1}&environmentId={2}&limit={3}" -f $RegionApi,$offset,$envId,$MaxListPageSize) -ForegroundColor DarkGray
      $page = Invoke-TalendJson -Method GET -Path "/orchestration/resources" -Query @{ environmentId=$envId; limit=$MaxListPageSize; offset=$offset }
      $items=@(); if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }
      $rows=@()
      foreach($it in $items){
        $rows += [pscustomobject]@{
          environmentId=$envId; environmentName=$envName;
          resourceId=$it.id; name=$it.name; type=$it.type;
          workspaceId=$it.workspace.id; workspaceName=$it.workspace.name;
          created=$it.createDate; updated=$it.updateDate
        }
      }
      if($rows){ $rows | Export-Csv -Path $fRes -Append:([bool](Test-Path $fRes)) -NoTypeInformation -Encoding UTF8 }
      if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
      $offset += $MaxListPageSize
      Start-Sleep -Milliseconds $SleepMsBetweenPages
    }

    # Tasks
    Write-Host ("   [Tasks] env={0}" -f $envName)
    $offset=0
    while($true){
      Write-Host ("   [HTTP GET] {0}/orchestration/executables/tasks?offset={1}&environmentId={2}&limit={3}" -f $RegionApi,$offset,$envId,$MaxListPageSize) -ForegroundColor DarkGray
      $page = Invoke-TalendJson -Method GET -Path "/orchestration/executables/tasks" -Query @{ environmentId=$envId; limit=$MaxListPageSize; offset=$offset }
      $items=@(); if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }
      $rows=@()
      foreach($it in $items){
        $rows += [pscustomobject]@{
          environmentId=$envId; environmentName=$envName;
          workspaceId=$it.workspace.id; workspaceName=$it.workspace.name;
          taskId=$it.id; taskName=$it.name; type=$it.type;
          artifactId=$it.artifact.id; artifactName=$it.artifact.name; artifactVer=$it.artifact.version;
          runtimeType=$it.runtime.type; runtimeId=$it.runtime.id;
          created=$it.createDate; updated=$it.updateDate; tags=((($it.tags) -join '|'))
        }
      }
      if($rows){ $rows | Export-Csv -Path $fT -Append:([bool](Test-Path $fT)) -NoTypeInformation -Encoding UTF8 }
      if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
      $offset += $MaxListPageSize
      Start-Sleep -Milliseconds $SleepMsBetweenPages
    }

    # Plans (+detail steps and task_plan)
    Write-Host ("   [Plans] env={0}" -f $envName)
    $offset=0
    while($true){
      Write-Host ("   [HTTP GET] {0}/orchestration/executables/plans?offset={1}&environmentId={2}&limit={3}" -f $RegionApi,$offset,$envId,$MaxListPageSize) -ForegroundColor DarkGray
      $page = Invoke-TalendJson -Method GET -Path "/orchestration/executables/plans" -Query @{ environmentId=$envId; limit=$MaxListPageSize; offset=$offset }
      $items=@(); if($page -and $page.psobject.Properties.Name -contains 'items'){ $items=$page.items } elseif($page){ $items=$page }

      foreach($pl in $items){
        [pscustomobject]@{
          environmentId=$envId; environmentName=$envName;
          workspaceId=$pl.workspace.id; workspaceName=$pl.workspace.name;
          planId=$pl.id; planName=$pl.name; created=$pl.createDate; updated=$pl.updateDate;
          tags=((($pl.tags) -join '|'))
        } | Export-Csv -Path $fP -Append:([bool](Test-Path $fP)) -NoTypeInformation -Encoding UTF8

        $planId = $pl.id
        if([string]::IsNullOrWhiteSpace($planId)){ continue }

        Write-Host ("   [HTTP GET] {0}/orchestration/executables/plans/{1}" -f $RegionApi,$planId) -ForegroundColor DarkGray
        $detail = Invoke-TalendJson -Method GET -Path "/orchestration/executables/plans/$planId"
        if($detail -and $detail.steps){
          $seq=0
          foreach($st in $detail.steps){
            $seq++
            $taskId = $null; $taskName = $null
            if($st.task){ $taskId=$st.task.id; $taskName=$st.task.name }
            [pscustomobject]@{
              environmentId=$envId; planId=$planId; planName=$pl.name;
              workspaceId=$pl.workspace.id; workspaceName=$pl.workspace.name;
              stepSeq=$seq; stepType=$st.type; taskId=$taskId; taskName=$taskName; onError=$st.onError
            } | Export-Csv -Path (Join-Path $OutDir 'plan_steps.csv') -Append:([bool](Test-Path (Join-Path $OutDir 'plan_steps.csv'))) -NoTypeInformation -Encoding UTF8
            if($taskId){
              [pscustomobject]@{
                environmentId=$envId; planId=$planId; planName=$pl.name;
                workspaceId=$pl.workspace.id; workspaceName=$pl.workspace.name;
                taskId=$taskId; taskName=$taskName
              } | Export-Csv -Path (Join-Path $OutDir 'task_plan.csv') -Append:([bool](Test-Path (Join-Path $OutDir 'task_plan.csv'))) -NoTypeInformation -Encoding UTF8
            }
          }
        }
      }

      if(($items|Measure-Object).Count -lt $MaxListPageSize){ break }
      $offset += $MaxListPageSize
      Start-Sleep -Milliseconds $SleepMsBetweenPages
    }
  }

  Write-Host ">> OK catalog." -ForegroundColor Green
}

# ========================== 2) EXECUTIONS ============================

function Export-Executions {
  <#
    .SYNOPSIS
      Download TASK and PLAN executions from Observability within a fixed
      window [now-DeltaDays..now] (with overlap), then combine to executions.csv.
  #>
  param([switch]$FullDump)

  $fTasks = Join-Path $OutDir 'executions_tasks.csv'
  $fPlans = Join-Path $OutDir 'executions_plans.csv'

  if ($FullDump) {
    foreach ($f in @($fTasks,$fPlans)) { if (Test-Path $f) { Remove-Item $f -Force } }
  }

  $runType = if ($FullDump) { 'full' } else { "delta -$DeltaDays d" }
  Write-Host ">> Executions ($runType)..." -ForegroundColor Yellow

  $envs = Get-Environments

  $snapshotUtc        = NowUtc
  $toMs               = Get-Millis $snapshotUtc
  $minFromMsByDelta   = Get-Millis ($snapshotUtc.AddDays(-$DeltaDays))
  $overlapMs          = [int64]($ExecOverlapMinutes * 60 * 1000)
  $maxWindowPages     = 200

  foreach($env in $envs){
    $envId   = $env.id
    $envName = Coalesce @($env.name,$envId)

    Write-Host ("-- ENV start: {0} ({1})" -f $envName,$envId) -ForegroundColor DarkCyan

    if (-not (Test-WorkspaceAccess $envId)) {
      Write-Host ("   [SKIP] Env {0} ({1}): no visible workspaces." -f $envName,$envId) -ForegroundColor DarkYellow
      Write-Host ("-- ENV done : {0} ({1})" -f $envName,$envId) -ForegroundColor DarkCyan
      continue
    }

    function Invoke-ExecWindowPaged {
      param([string]$Kind)   # 'tasks' | 'plans'

      $seen   = Get-RunIdSet $Kind $envId
      $outCsv = if ($Kind -eq 'tasks') { $fTasks } else { $fPlans }

      $stateName = "exec_{0}_{1}" -f $Kind, $envId
      $st = if ($FullDump -or $IgnoreExecAnchors) { $null } else { Get-State $stateName }
      $fromMs = $minFromMsByDelta
      if (-not $FullDump -and $st -and $st.lastToMs) {
        $fromMs = [math]::Max([int64]$st.lastToMs - $overlapMs, $minFromMsByDelta)
      }

      $page = 0
      $pagedLastId = $null

      Write-Host ("   [{0}] Fixed window env={1} {2}..{3} (ms)" -f $Kind.ToUpper(), $envId, $fromMs, $toMs) -ForegroundColor DarkGray

      while ($true) {
        $page++
        if ($page -gt $maxWindowPages) {
          Write-Warning ("   [{0}] Stopping at guard of {1} pages (env {2})." -f $Kind.ToUpper(), $maxWindowPages, $envId)
          break
        }

        $body = @{
          environmentId = $envId
          category      = 'ETL'
          limit         = $MaxExecCsvRowsPerPage
          from          = $fromMs
          to            = $toMs
          exclude       = $(if ($Kind -eq 'tasks') { 'PLAN_EXECUTIONS' } else { 'TASK_EXECUTIONS_TRIGGERED_BY_PLAN' })
        }
        if ($pagedLastId) { $body.lastId = $pagedLastId }

        $tmp = Join-Path $OutDir ("tmp_{0}_{1}_{2}.csv" -f $Kind,$envId,([Guid]::NewGuid().ToString('N')))
        try {
          Invoke-TalendCsvDownload -Path '/monitoring/observability/executions/download' -Body $body -OutFile $tmp
        } catch {
          $msg = $_.Exception.Message
          if ($msg -match 'lastId is not valid id') {
            if ($pagedLastId) {
              $pagedLastId = $null
              try { if (Test-Path $tmp) { Remove-Item $tmp -Force } } catch {}
              Write-Host ("   [{0}] Notice: lastId rejected; retrying without lastId." -f $Kind.ToUpper()) -ForegroundColor DarkYellow
              continue
            }
          }
          throw
        }

        if ((Get-Item $tmp).Length -eq 0) {
          Remove-Item $tmp -Force
          Write-Host ("   [{0}] Page {1}: empty" -f $Kind.ToUpper(), $page) -ForegroundColor DarkGray
          break
        }

        $res = Select-CsvByNewRunIds -Src $tmp -Dst $outCsv -SeenInfo $seen
        try { Remove-Item $tmp -Force } catch {}

        Write-Host ("   [{0}] Page {1}: added {2} new rows." -f $Kind.ToUpper(), $page, $res.appended) -ForegroundColor DarkGray

        if (-not $res.hadData) { break }

        if ($res.lastId) {
          $ok2 = $true; try { [void][Guid]::Parse([string]$res.lastId) } catch { $ok2 = $false }
          $pagedLastId = if ($ok2) { [string]$res.lastId } else { $null }
        } else { $pagedLastId = $null }

        if ($res.appended -eq 0) { break }

        Start-Sleep -Milliseconds $SleepMsBetweenPages
      }

      if (-not $FullDump) {
        Set-State $stateName @{ lastId=$pagedLastId; lastToMs=$toMs; ts=(NowUtc) }
      }
    }

    [void](Invoke-ExecWindowPaged -Kind 'tasks')
    [void](Invoke-ExecWindowPaged -Kind 'plans')

    Write-Host ("-- ENV done : {0} ({1})" -f $envName,$envId) -ForegroundColor DarkCyan
  }

  Write-Host "[Combine] starting..." -ForegroundColor DarkGray
  Combine-ExecutionsCsvFast

  if ($UseSortLineDedup) {
    Write-Host ">> De-duplicating executions.csv by exact line (sort.exe /unique)..." -ForegroundColor Yellow
    Dedup-ExecCsvByLineSort -Path (Join-Path $OutDir 'executions.csv')
  }

  Write-Host ">> OK executions." -ForegroundColor Green
}

# =================== 3) OBSERVABILITY: COMPONENT METRICS ==============

function Export-Observability {
  <#
    .SYNOPSIS
      Download component metrics per day over ComponentsDays.
      Clean “empty” rows and duplicates by relevant keys.
  #>
  if($ComponentsDays -le 0){ return }
  Write-Host ">> Observability (component metrics) last $ComponentsDays day(s)..." -ForegroundColor Yellow

  $out = Join-Path $OutDir 'observability_components.csv'
  if(Test-Path $out){ Remove-Item $out -Force }

  $ObsSleepMs = [int]([Math]::Max($SleepMsBetweenPages, 2500))
  $limit = 200
  $maxPagesPerDay = 500

  $envsAll = Get-Environments
  $envs = $envsAll
  try {
    if ($EnvIncludeNames -and $EnvIncludeNames.Count -gt 0) {
      $set = [System.Collections.Generic.HashSet[string]]::new([string[]]$EnvIncludeNames,[System.StringComparer]::OrdinalIgnoreCase)
      $envs = $envsAll | Where-Object { $set.Contains( [string](Coalesce @($_.name,$_.id)) ) }
    }
  } catch { $envs = $envsAll }

  $nowUtc = NowUtc

  foreach($env in $envs){
    $envId   = $env.id
    $envName = Coalesce @($env.name,$envId)

    if (-not (Test-WorkspaceAccess $envId)) {
      Write-Host ("   [SKIP] Env {0} ({1}): no visible workspaces for PAT." -f $envName,$envId) -ForegroundColor DarkYellow
      continue
    }

    for($i=$ComponentsDays; $i -gt 0; $i--){
      $start = $nowUtc.Date.AddDays(-$i)
      $end   = $start.AddDays(1).AddMilliseconds(-1)
      $offset = 0
      $pages  = 0

      $startIso = $start.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")
      $endIso   = $end.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")

      if($TraceCatalog){
        Write-Host ("   [CompMetrics] env={0} day={1:yyyy-MM-dd} ({2}..{3})" -f $envName,$start,$startIso,$endIso) -ForegroundColor DarkCyan
      }

      while($true){
        if($pages -ge $maxPagesPerDay){
          Write-Warning ("   [CompMetrics] Stopping at guard of {0} pages on {1:yyyy-MM-dd} (env {2})." -f $maxPagesPerDay,$start,$envName)
          break
        }

        $body = @{
          environmentId = $envId
          startTime     = $startIso
          endTime       = $endIso
          limit         = $limit
          offset        = $offset
        }

        $data = $null
        try {
          $data = Invoke-TalendJson -Method POST -Path "/monitoring/observability/metrics/component" -Body $body -Retry 3
        } catch {
          if(($_.Exception.Message -match 'ISO date') -or ($_.Exception.Message -match 'Time parameter')){
            $body = @{ environmentId=$envId; from=$startIso; to=$endIso; limit=$limit; offset=$offset }
            $data = Invoke-TalendJson -Method POST -Path "/monitoring/observability/metrics/component" -Body $body -Retry 3
          } else { throw }
        }

        $items = @()
        if($data -and $data.psobject.Properties.Name -contains 'items'){ $items = $data.items }
        elseif($data -is [array]){ $items = $data }
        elseif($data){ $items = @($data) }

        if(-not $items -or $items.Count -eq 0){ break }

        $rows = @()
        foreach($it in $items){
          $hasUseful = $false
          $runId=$null; $execId=$null; $execName=$null; $execType=$null; $compName=$null; $level=$null; $ts=$null; $msg=$null
          try { if($it.PSObject.Properties.Name -contains 'runId' -and $it.runId){ $runId=[string]$it.runId; $hasUseful=$true } } catch {}
          try { if($it.PSObject.Properties.Name -contains 'message' -and $it.message){ $msg=[string]$it.message; $hasUseful=$true } } catch {}
          try { if($it.PSObject.Properties.Name -contains 'time' -and $it.time){ $ts=$it.time; $hasUseful=$true } } catch {}
          try { if($it.PSObject.Properties.Name -contains 'level' -and $it.level){ $level=[string]$it.level } } catch {}
          try {
            if($it.PSObject.Properties.Name -contains 'component' -and $it.component){
              if($it.component.PSObject.Properties.Name -contains 'name' -and $it.component.name){
                $compName=[string]$it.component.name; $hasUseful=$true
              }
            }
          } catch {}
          try {
            if($it.PSObject.Properties.Name -contains 'executable' -and $it.executable){
              if($it.executable.PSObject.Properties.Name -contains 'id'   -and $it.executable.id){   $execId=[string]$it.executable.id;   $hasUseful=$true }
              if($it.executable.PSObject.Properties.Name -contains 'name' -and $it.executable.name){ $execName=[string]$it.executable.name; $hasUseful=$true }
              if($it.executable.PSObject.Properties.Name -contains 'type' -and $it.executable.type){ $execType=[string]$it.executable.type }
            }
          } catch {}
          if(-not $hasUseful){ continue }
          $rows += [pscustomobject]@{
            environmentId   = $envId
            environmentName = $envName
            runId           = $runId
            executableId    = $execId
            executableName  = $execName
            executableType  = $execType
            component       = $compName
            level           = $level
            timestamp       = $ts
            message         = $msg
          }
        }

        if($rows -and $rows.Count -gt 0){
          $rows | Export-Csv -Path $out -Append:([bool](Test-Path $out)) -NoTypeInformation -Encoding UTF8
        } else {
          Write-Host ("   [CompMetrics] Page with {0} items but no useful data (env={1}, day={2:yyyy-MM-dd}, offset={3}). Stopping day." -f $items.Count,$envName,$start,$offset) -ForegroundColor DarkGray
          break
        }

        $offset += $items.Count
        $pages++
        if($items.Count -lt $limit){ break }
        Start-Sleep -Milliseconds $ObsSleepMs
      }
    }
  }
  Remove-ObservabilityComponentsCsv -CsvPath (Join-Path $OutDir 'observability_components.csv')
  Write-Host ">> OK observability_components.csv" -ForegroundColor Green
}

# ====================== 4) DERIVED (auxiliary CSVs) ==================

function Export-Derived {
  <#
    .SYNOPSIS
      Compute convenient derived CSVs from catalog and executions.
  #>
  Write-Host ">> Derived..." -ForegroundColor Yellow

  $fTasks = Join-Path $OutDir 'executions_tasks.csv'
  $fPlans = Join-Path $OutDir 'executions_plans.csv'

  if (-not (Test-Path $fTasks)) {
    Write-Warning "Missing executions_tasks.csv"
    return
  }

  $fmtTasks  = Get-CsvFormat -CsvPath $fTasks
  $tasksData = Import-Csv -Path $fTasks -Delimiter $fmtTasks.Delim

  # 1) Task health summary
  $out1 = Join-Path $OutDir 'task_health_summary.csv'
  if (Test-Path $out1) { Remove-Item $out1 -Force }
  $taskRows = $tasksData |
    Group-Object taskId |
    ForEach-Object {
      $grp   = $_
      $runs  = $grp.Count
      $ok    = ($grp.Group | Where-Object { $_.status -match 'SUCC' }).Count
      $ko    = $runs - $ok
      $last  = $grp.Group | Sort-Object triggerTime,endTime,runEndTime,finishTimestamp | Select-Object -Last 1
      $lastRun = Coalesce @($last.runEndTime,$last.endTime,$last.finishTimestamp,$last.triggerTime)
      $tName = if ($last.PSObject.Properties.Name -contains 'taskName') { $last.taskName } else { '' }
      [pscustomobject]@{
        taskId   = $grp.Name
        taskName = $tName
        runs     = $runs
        ok       = $ok
        ko       = $ko
        ok_pct   = [math]::Round(100 * ($ok / [double]([math]::Max(1, $runs))), 2)
        lastRun  = $lastRun
      }
    }
  if ($taskRows) { $taskRows | Export-Csv -Path $out1 -NoTypeInformation -Encoding UTF8 }

  # 2) Plan health summary
  if (Test-Path $fPlans) {
    $fmtPlans  = Get-CsvFormat -CsvPath $fPlans
    $plansData = Import-Csv -Path $fPlans -Delimiter $fmtPlans.Delim
    $out2      = Join-Path $OutDir 'plan_health_summary.csv'
    if (Test-Path $out2) { Remove-Item $out2 -Force }
    $planRows = $plansData |
      Group-Object planId |
      ForEach-Object {
        $grp   = $_
        $runs  = $grp.Count
        $ok    = ($grp.Group | Where-Object { $_.status -match 'SUCC' }).Count
        $ko    = $runs - $ok
        $last  = $grp.Group | Sort-Object triggerTime,endTime,runEndTime,finishTimestamp | Select-Object -Last 1
        $lastRun = Coalesce @($last.runEndTime,$last.endTime,$last.finishTimestamp,$last.triggerTime)
        $pName = if ($last.PSObject.Properties.Name -contains 'planName') { $last.planName } else { '' }
        [pscustomobject]@{
          planId   = $grp.Name
          planName = $pName
          runs     = $runs
          ok       = $ok
          ko       = $ko
          ok_pct   = [math]::Round(100 * ($ok / [double]([math]::Max(1, $runs))), 2)
          lastRun  = $lastRun
        }
      }
    if ($planRows) { $planRows | Export-Csv -Path $out2 -NoTypeInformation -Encoding UTF8 }
  }

  # 3) Top 50 error messages
  $out3 = Join-Path $OutDir 'top_errors.csv'
  if (Test-Path $out3) { Remove-Item $out3 -Force }
  $errorRows = $tasksData |
    Where-Object { $_.status -and $_.status -notmatch 'SUCC' } |
    Group-Object message |
    Sort-Object Count -Descending |
    Select-Object -First 50 |
    ForEach-Object { [pscustomobject]@{ message = $_.Name; count = $_.Count } }
  if ($errorRows) { $errorRows | Export-Csv -Path $out3 -NoTypeInformation -Encoding UTF8 }

  # 4) Queue latency
  $out4 = Join-Path $OutDir 'queue_latency.csv'
  if (Test-Path $out4) { Remove-Item $out4 -Force }
  $latencyRows = $tasksData |
    Where-Object { $_.startTime -and $_.triggerTime } |
    ForEach-Object {
      $start   = [int64]$_.startTime
      $trigger = [int64]$_.triggerTime
      $tName = if ($_.PSObject.Properties.Name -contains 'taskName') { $_.taskName } else { '' }
      [pscustomobject]@{
        runId     = $_.id
        taskId    = $_.taskId
        taskName  = $tName
        latencyMs = $start - $trigger
      }
    }
  if ($latencyRows) { $latencyRows | Export-Csv -Path $out4 -NoTypeInformation -Encoding UTF8 }

  # 5) Orphan tasks (no schedule and not in any plan)
  $out5         = Join-Path $OutDir 'orphan_tasks.csv'
  $tasksCsv     = Join-Path $OutDir 'tasks.csv'
  $taskSchedCsv = Join-Path $OutDir 'task_schedule.csv'
  $taskPlanCsv  = Join-Path $OutDir 'task_plan.csv'
  if (Test-Path $out5) { Remove-Item $out5 -Force }

  if (Test-Path $tasksCsv) {
    $catalog = Import-Csv $tasksCsv
    $schedList = if (Test-Path $taskSchedCsv) { Import-Csv $taskSchedCsv } else { @() }
    $planList  = if (Test-Path $taskPlanCsv)  { Import-Csv $taskPlanCsv }  else { @() }

    $orphans = @()
    foreach ($t in $catalog) {
      $hasSched = $false; foreach ($s in $schedList) { if ($s.taskId -eq $t.taskId) { $hasSched = $true; break } }
      $inPlan = $false; foreach ($p in $planList)  { if ($p.taskId -eq $t.taskId)  { $inPlan = $true;  break } }
      if (-not $hasSched -and -not $inPlan) { $orphans += $t }
    }
    if ($orphans) {
      $orphans | Select-Object environmentId,workspaceId,taskId,taskName,type |
        Export-Csv -Path $out5 -NoTypeInformation -Encoding UTF8
    }
  }

  # 6) Artifact drift (deployed vs latest)
  $out6    = Join-Path $OutDir 'artifact_drift.csv'
  $artsCsv = Join-Path $OutDir 'artifacts.csv'
  if ((Test-Path $artsCsv) -and (Test-Path $tasksCsv)) {
    $arts    = Import-Csv $artsCsv
    $catalog = Import-Csv $tasksCsv

    $latestVer = @{}
    foreach ($a in $arts) {
      $versions = $a.versions -split '\|'
      [array]::Sort($versions)
      $latestVer[$a.artifactId] = $versions[-1]
    }

    $drift = $catalog |
      Where-Object {
        $id = $_.artifactId
        $latestVer.ContainsKey($id) -and $_.artifactVer -ne $latestVer[$id]
      } |
      Select-Object taskId,taskName,artifactId,
        @{ Name = 'usedVersion'   ; Expression = { $_.artifactVer           } },
        @{ Name = 'latestVersion' ; Expression = { $latestVer[$_.artifactId] } }

    if ($drift) { $drift | Export-Csv -Path $out6 -NoTypeInformation -Encoding UTF8 }
  }

  Write-Host ">> OK derived" -ForegroundColor Green
}

# ============================ 5) METRICS ==============================

function _ToUtc([object]$val){
  if($null -eq $val -or ($val -is [string] -and [string]::IsNullOrWhiteSpace($val))){ return $null }
  $s=[string]$val; $ms=0L
  if([Int64]::TryParse($s,[ref]$ms)){ try{ return ([DateTimeOffset]::FromUnixTimeMilliseconds($ms)).UtcDateTime }catch{} }
  try{ return ([DateTimeOffset]$s).UtcDateTime }catch{
    try{ $dt=[DateTime]$s; if($dt.Kind -eq 'Utc'){ return $dt } else { return $dt.ToUniversalTime() } }catch{ return $null }
  }
}
function New-MetricRow([string]$Metric,[string]$Level,[string]$Env,[string]$Ws,[string]$BucketDate,[Nullable[int]]$Hour,[double]$Value,[string]$Subtype){
  [pscustomobject]@{ metric=$Metric; level=$Level; environmentId=$Env; workspaceId=$Ws; bucketDate=$BucketDate; hour=$Hour; value=$Value; subtype=$Subtype }
}

function Get-Metrics {
  <#
    .SYNOPSIS
      Aggregate catalog and executions into useful CSV metrics.
  #>
  Write-Host ">> Metrics (composition & aggregates)..." -ForegroundColor Yellow
  $fArts     = Join-Path $OutDir 'artifacts.csv'
  $fTasksC   = Join-Path $OutDir 'tasks.csv'
  $fPlansC   = Join-Path $OutDir 'plans.csv'
  $fPlanSteps= Join-Path $OutDir 'plan_steps.csv'
  $fConn     = Join-Path $OutDir 'connections.csv'
  $fRes      = Join-Path $OutDir 'resources.csv'
  $fExecT    = Join-Path $OutDir 'executions_tasks.csv'
  $fExecP    = Join-Path $OutDir 'executions_plans.csv'

  $mCounts = Join-Path $OutDir 'metrics_counts.csv'
  $mDaily  = Join-Path $OutDir 'metrics_exec_daily.csv'
  $mHourly = Join-Path $OutDir 'metrics_exec_hourly.csv'
  $mRecKO  = Join-Path $OutDir 'metrics_ko_recurrent_6m.csv'

  foreach($f in @($mCounts,$mDaily,$mHourly,$mRecKO)){ if(Test-Path $f){ Remove-Item $f -Force } }

  # Load catalog
  $arts = @(); if(Test-Path $fArts){ $arts = Import-Csv $fArts }
  $tasks= @(); if(Test-Path $fTasksC){ $tasks = Import-Csv $fTasksC }
  $plans= @(); if(Test-Path $fPlansC){ $plans = Import-Csv $fPlansC }
  $steps= @(); if(Test-Path $fPlanSteps){ $steps = Import-Csv $fPlanSteps }
  $conns= @(); if(Test-Path $fConn){ $conns = Import-Csv $fConn }
  $ress = @(); if(Test-Path $fRes){ $ress = Import-Csv $fRes }

  # Executions
  $exec = @()
  if(Test-Path $fExecT){ $fmt=Get-CsvFormat -CsvPath $fExecT; $exec += Import-Csv -Path $fExecT -Delimiter $fmt.Delim }
  if(Test-Path $fExecP){ $fmt=Get-CsvFormat -CsvPath $fExecP; $exec += Import-Csv -Path $fExecP -Delimiter $fmt.Delim }

  # -------- A) Catalog counts / types --------
  $rows = @()

  if($tasks.Count -gt 0){
    $artType=@{}; foreach($a in $arts){ if($a.artifactId){ $artType[$a.artifactId]=$a.type } }
    $g = $tasks | Group-Object environmentId,workspaceId
    foreach($grp in $g){
      $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]
      $distinctArtifacts = ($grp.Group | Select-Object -ExpandProperty artifactId | Sort-Object -Unique).Count
      $rows += (New-MetricRow 'ArtifactsDeployed' 'env_ws' $env $ws $null $null $distinctArtifacts $null)
      $rows += (New-MetricRow 'Tasks'              'env_ws' $env $ws $null $null $grp.Count $null)
      $byTaskType = $grp.Group | Group-Object type
      foreach($t in $byTaskType){ $rows += (New-MetricRow 'TasksByType' 'env_ws' $env $ws $null $null $t.Count $t.Name) }
      $ids = $grp.Group | Select-Object -ExpandProperty artifactId | Sort-Object -Unique
      $byArtType=@{}; foreach($id in $ids){ $tpe = if($artType.ContainsKey($id)){$artType[$id]}else{'UNKNOWN'}; if(-not $byArtType.ContainsKey($tpe)){ $byArtType[$tpe]=0 }; $byArtType[$tpe]++ }
      foreach($k in $byArtType.Keys){ $rows += (New-MetricRow 'ArtifactsDeployedByType' 'env_ws' $env $ws $null $null $byArtType[$k] $k) }
    }
  }
  if($plans.Count -gt 0){
    $g = $plans | Group-Object environmentId,workspaceId
    foreach($grp in $g){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $rows += (New-MetricRow 'Plans' 'env_ws' $env $ws $null $null $grp.Count $null) }
  }
  if($steps.Count -gt 0){
    $g = $steps | Group-Object environmentId,workspaceId,stepType
    foreach($grp in $g){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $stype=$parts[2]; $rows += (New-MetricRow 'PlanStepsByType' 'env_ws' $env $ws $null $null $grp.Count $stype) }
  }
  if($conns.Count -gt 0){
    $g = $conns | Group-Object environmentId,workspaceId
    foreach($grp in $g){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $rows += (New-MetricRow 'Connections' 'env_ws' $env $ws $null $null $grp.Count $null) }
    $byType = $conns | Group-Object environmentId,workspaceId,type
    foreach($grp in $byType){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $tpe=$parts[2]; $rows += (New-MetricRow 'ConnectionsByType' 'env_ws' $env $ws $null $null $grp.Count $tpe) }
  }
  if($ress.Count -gt 0){
    $g = $ress | Group-Object environmentId,workspaceId
    foreach($grp in $g){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $rows += (New-MetricRow 'Resources' 'env_ws' $env $ws $null $null $grp.Count $null) }
    $byType = $ress | Group-Object environmentId,workspaceId,type
    foreach($grp in $byType){ $parts=$grp.Name -split ','; $env=$parts[0]; $ws=$parts[1]; $tpe=$parts[2]; $rows += (New-MetricRow 'ResourcesByType' 'env_ws' $env $ws $null $null $grp.Count $tpe) }
  }
  if($rows){ $rows|Export-Csv -Path $mCounts -NoTypeInformation -Encoding UTF8 }

  # Environment / global totals
  if(Test-Path $mCounts){
    $tbl=Import-Csv $mCounts; $rows=@()
    $envs = ($tbl | Select-Object -ExpandProperty environmentId | Where-Object { $_ } | Sort-Object -Unique)
    foreach($env in $envs){
      foreach($metric in @('ArtifactsDeployed','Tasks','Plans','Connections','Resources')){
        $val = ($tbl | Where-Object { $_.level -eq 'env_ws' -and $_.environmentId -eq $env -and $_.metric -eq $metric } | Measure-Object -Property value -Sum).Sum
        $rows += (New-MetricRow $metric 'env' $env $null $null $null $val $null)
      }
    }
    foreach($metric in @('ArtifactsDeployed','Tasks','Plans','Connections','Resources')){
      $val = ($tbl | Where-Object { $_.level -eq 'env_ws' -and $_.metric -eq $metric } | Measure-Object -Property value -Sum).Sum
      $rows += (New-MetricRow $metric 'global' $null $null $null $null $val $null)
    }
    if($rows){ $rows|Export-Csv -Path $mCounts -NoTypeInformation -Encoding UTF8 -Append }
  }

  # -------- B) Exec OK/KO daily/hourly --------
  if (-not $DisableExecDailyHourly) {
    if($exec.Count -gt 0){
      $dGlobal=@{}; $hGlobal=@{}; $dByEnv=@{};  $hByEnv=@{}; $dByEnvWs=@{}; $hByEnvWs=@{}
      foreach($r in $exec){
        $end = $null
        foreach($c in @('runEndTime','endTime','finishTimestamp','runStartTime')){ if($r.$c){ $end=_ToUtc $r.$c; if($end){ break } } }
        if(-not $end){ continue }
        $day=$end.ToString('yyyy-MM-dd'); $hour=[int]$end.ToString('HH')
        $isOk = ($r.status -match 'SUCC|OK|SUCCESS')
        if(-not $dGlobal.ContainsKey($day)){ $dGlobal[$day]=@{ ok=0; ko=0 } }
        if($isOk){ $dGlobal[$day].ok++ } else { $dGlobal[$day].ko++ }
        $hk="$day|$hour"; if(-not $hGlobal.ContainsKey($hk)){ $hGlobal[$hk]=@{ ok=0; ko=0 } }
        if($isOk){ $hGlobal[$hk].ok++ } else { $hGlobal[$hk].ko++ }
        $env = $r.environmentId
        if($env){
          $ek="$env|$day"; if(-not $dByEnv.ContainsKey($ek)){ $dByEnv[$ek]=@{ ok=0; ko=0 } }
          if($isOk){ $dByEnv[$ek].ok++ } else { $dByEnv[$ek].ko++ }
          $ehk="$env|$day|$hour"; if(-not $hByEnv.ContainsKey($ehk)){ $hByEnv[$ehk]=@{ ok=0; ko=0 } }
          if($isOk){ $hByEnv[$ehk].ok++ } else { $hByEnv[$ehk].ko++ }
        }
        $ws = $r.workspaceId
        if($env -and $ws){
          $kw="$env|$ws|$day"; if(-not $dByEnvWs.ContainsKey($kw)){ $dByEnvWs[$kw]=@{ ok=0; ko=0 } }
          if($isOk){ $dByEnvWs[$kw].ok++ } else { $dByEnvWs[$kw].ko++ }
          $kh="$env|$ws|$day|$hour"; if(-not $hByEnvWs.ContainsKey($kh)){ $hByEnvWs[$kh]=@{ ok=0; ko=0 } }
          if($isOk){ $hByEnvWs[$kh].ok++ } else { $hByEnvWs[$kh].ko++ }
        }
      }
      $rows=@()
      foreach($k in ($dGlobal.Keys | Sort-Object)){ $rows += (New-MetricRow 'ExecOKDaily' 'global' $null $null $k $null $dGlobal[$k].ok $null); $rows += (New-MetricRow 'ExecKODaily' 'global' $null $null $k $null $dGlobal[$k].ko $null) }
      foreach($k in ($dByEnv.Keys   | Sort-Object)){ $parts=$k -split '\|'; $env=$parts[0]; $day=$parts[1]; $rows += (New-MetricRow 'ExecOKDaily' 'env' $env $null $day $null $dByEnv[$k].ok $null); $rows += (New-MetricRow 'ExecKODaily' 'env' $env $null $day $null $dByEnv[$k].ko $null) }
      foreach($k in ($dByEnvWs.Keys | Sort-Object)){ $parts=$k -split '\|'; $env=$parts[0]; $ws=$parts[1]; $day=$parts[2]; $rows += (New-MetricRow 'ExecOKDaily' 'env_ws' $env $ws $day $null $dByEnvWs[$k].ok $null); $rows += (New-MetricRow 'ExecKODaily' 'env_ws' $env $ws $day $null $dByEnvWs[$k].ko $null) }
      if($rows){ $rows|Export-Csv -Path $mDaily -NoTypeInformation -Encoding UTF8 }
      $rows=@()
      foreach($k in ($hGlobal.Keys | Sort-Object)){ $parts=$k -split '\|'; $day=$parts[0]; $hour=[int]$parts[1]; $rows += (New-MetricRow 'ExecOKHourly' 'global' $null $null $day $hour $hGlobal[$k].ok $null); $rows += (New-MetricRow 'ExecKOHourly' 'global' $null $null $day $hour $hGlobal[$k].ko $null) }
      foreach($k in ($hByEnv.Keys   | Sort-Object)){ $parts=$k -split '\|'; $env=$parts[0]; $day=$parts[1]; $hour=[int]$parts[2]; $rows += (New-MetricRow 'ExecOKHourly' 'env' $env $null $day $hour $hByEnv[$k].ok $null); $rows += (New-MetricRow 'ExecKOHourly' 'env' $env $null $day $hour $hByEnv[$k].ko $null) }
      foreach($k in ($hByEnvWs.Keys | Sort-Object)){ $parts=$k -split '\|'; $env=$parts[0]; $ws=$parts[1]; $day=$parts[2]; $hour=[int]$parts[3]; $rows += (New-MetricRow 'ExecOKHourly' 'env_ws' $env $ws $day $hour $hByEnvWs[$k].ok $null); $rows += (New-MetricRow 'ExecKOHourly' 'env_ws' $env $ws $day $hour $hByEnvWs[$k].ko $null) }
      if($rows){ $rows|Export-Csv -Path $mHourly -NoTypeInformation -Encoding UTF8 }
    }
  } else {
    Write-Host ">> Skipping metrics_exec_daily.csv & metrics_exec_hourly.csv (DisableExecDailyHourly)" -ForegroundColor DarkYellow
  }

  # -------- C) Recurrent KO (6 months window) --------
  if ($exec.Count -gt 0) {
    $from = (NowUtc).AddMonths(-6)
    $norm = $exec | ForEach-Object {
      $status = $_.status
      $eid = Coalesce @($_.taskId, $_.planId, $_.executableId, $_.id)
      $etype = if ($_.taskId) { 'TASK' } elseif ($_.planId) { 'PLAN' } elseif ($_.executableType) { [string]$_.executableType } elseif ($_.type) { [string]$_.type } else { 'UNK' }
      $ename = Coalesce @($_.taskName, $_.planName, $_.executableName, $_.name)
      $ts = Coalesce @($_.runEndTime, $_.endTime, $_.finishTimestamp, $_.runStartTime, $_.startTime, $_.triggerTime)
      $tsUtc = _ToUtc $ts
      [pscustomobject]@{ environmentId=$_.environmentId; workspaceId=$_.workspaceId; status=$status; etype=$etype; eid=$eid; ename=$ename; tsUtc=$tsUtc }
    }
    $fail = $norm | Where-Object { $_.status -and ($_.status -notmatch 'SUCC|OK|SUCCESS') -and $_.tsUtc -and ($_.tsUtc -ge $from) }
    $grp = $fail | Group-Object { "$([string]$_.etype)|$([string]$_.environmentId)|$([string]$_.workspaceId)|$([string]$_.eid)" }

    $rows = @()
    foreach ($g in $grp) {
      $count = $g.Count
      if ($count -ge [int]$RecurrentKOThreshold) {
        $any   = $g.Group | Sort-Object tsUtc | Select-Object -Last 1
        $parts = $g.Name -split '\|', 4
        $type  = $parts[0]; $env=$parts[1]; $ws=$parts[2]; $id=$parts[3]
        $rows += [pscustomobject]@{
          type          = $type
          executableId  = $id
          name          = $any.ename
          environmentId = $env
          workspaceId   = $ws
          failures      = $count
          lastEnd       = $any.tsUtc
        }
      }
    }

    Write-Host ("[METRICS] recent KO={0}, groups={1}, groups>=thr({2})={3}" -f `
      ($fail|Measure-Object).Count, ($grp|Measure-Object).Count, $RecurrentKOThreshold, ($rows|Measure-Object).Count) -ForegroundColor DarkGray

    if ($rows -and $rows.Count -gt 0) {
      $rows | Export-Csv -Path $mRecKO -NoTypeInformation -Encoding UTF8
    } else {
      $header = ([pscustomobject]@{ type=''; executableId=''; name=''; environmentId=''; workspaceId=''; failures=0; lastEnd='' } | ConvertTo-Csv -NoTypeInformation)[0]
      Set-Content -Path $mRecKO -Encoding UTF8 -Value $header
      Write-Host "[METRICS] No groups >= threshold: generated metrics_ko_recurrent_6m.csv with header only." -ForegroundColor DarkYellow
    }
  } else {
    $header = ([pscustomobject]@{ type=''; executableId=''; name=''; environmentId=''; workspaceId=''; failures=0; lastEnd='' } | ConvertTo-Csv -NoTypeInformation)[0]
    Set-Content -Path $mRecKO -Encoding UTF8 -Value $header
    Write-Host "[METRICS] No executions: generated metrics_ko_recurrent_6m.csv with header only." -ForegroundColor DarkYellow
  }

  $generated = @()
  if (Test-Path $mCounts) { $generated += 'metrics_counts.csv' }
  if (-not $DisableExecDailyHourly) {
    if (Test-Path $mDaily)  { $generated += 'metrics_exec_daily.csv' }
    if (Test-Path $mHourly) { $generated += 'metrics_exec_hourly.csv' }
  }
  if (Test-Path $mRecKO) { $generated += 'metrics_ko_recurrent_6m.csv' }

  Write-Host (">> OK metrics: {0}" -f ($generated -join ', ')) -ForegroundColor Green
}

# ================================ MAIN ================================

try{
  if($Mode -contains 'all' -or $Mode -contains 'catalog'){ Export-Catalog }
  if($Mode -contains 'all' -or $Mode -contains 'executions'){ Export-Executions -FullDump:($FullRescan) }
  if($Mode -contains 'all' -or $Mode -contains 'observability'){ Export-Observability }
  if($Mode -contains 'all' -or $Mode -contains 'derived'){ Export-Derived }
  if($Mode -contains 'all' -or $Mode -contains 'metrics'){ Get-Metrics }

  Copy-FinalExports -DestDir $ExportCopyDir

  Write-Host "== Completed. Check $OutDir" -ForegroundColor Cyan
}catch{
  Write-Host "!! ERROR: $($_.Exception.Message)" -ForegroundColor Red
  if($_.ScriptStackTrace){ Write-Host $_.ScriptStackTrace -ForegroundColor DarkRed }
  exit 1
}
