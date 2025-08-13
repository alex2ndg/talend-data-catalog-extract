REM Talend-Qlik launch script
@echo off
setlocal
chcp 65001 >NUL

rem Adjust to your working directory
set "WORKDIR=C:\Path\To\CuadroDeMando"
set "OUTDIR=%WORKDIR%\talend_exports"
cd /d "%WORKDIR%"

rem === Talend PAT (do NOT commit real tokens) ===
set "TALEND_PAT=REPLACE_WITH_TALEND_PAT"

rem === Recommended cleanup for full rescan ===
if exist "%OUTDIR%\executions.csv" del /f /q "%OUTDIR%\executions.csv"
if exist "%OUTDIR%\.state\combine_offsets.json" del /f /q "%OUTDIR%\.state\combine_offsets.json"
for %%F in ("%OUTDIR%\.state\seen_runids_*.txt") do if exist "%%~fF" del /f /q "%%~fF"

powershell.exe -ExecutionPolicy Bypass -NoLogo -NoProfile -File ".\Talend-Qlik-Exports.ps1" ^
  -Pat "%TALEND_PAT%" ^
  -RegionApi "https://api.eu.cloud.talend.com" ^
  -Mode all -FullRescan ^
  -DeltaDays 180 ^
  -ComponentsDays 0 ^
  -DisableExecDailyHourly ^
  -UseSortLineDedup ^
  1>> "%WORKDIR%\run_full.log" 2>&1

if errorlevel 1 (
  echo [FULL] FAIL %date% %time%>> "%WORKDIR%\run_full.log"
) else (
  echo [FULL] OK   %date% %time%>> "%WORKDIR%\run_full.log"
)
endlocal
