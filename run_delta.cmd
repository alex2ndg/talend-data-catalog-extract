rem @echo off
setlocal
chcp 65001 >NUL

rem Adjust to your working directory
set "WORKDIR=C:\Path\To\CuadroDeMando"
cd /d "%WORKDIR%"

rem === Talend PAT (do NOT commit real tokens) ===
set "TALEND_PAT=REPLACE_WITH_TALEND_PAT"

powershell.exe -ExecutionPolicy Bypass -NoLogo -NoProfile -File ".\Talend-Qlik-Exports.ps1" ^
  -Pat "%TALEND_PAT%" ^
  -RegionApi "https://api.eu.cloud.talend.com" ^
  -Mode all ^
  -DeltaDays 2 ^
  -ComponentsDays 0 ^
  -DisableExecDailyHourly ^
  -UseSortLineDedup ^
  1>> "%WORKDIR%\run_delta.log" 2>&1

if errorlevel 1 (
  echo [DELTA] FAIL %date% %time%>> "%WORKDIR%\run_delta.log"
) else (
  echo [DELTA] OK   %date% %time%>> "%WORKDIR%\run_delta.log"
)
endlocal

