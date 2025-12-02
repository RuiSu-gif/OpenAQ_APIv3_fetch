param(
  [Parameter(Mandatory=$true)][string[]]$YearList,
  [switch]$Background,
  [string]$LogDir = ".\logs",
  [int]$MaxRetries = 3
)

# Validate and clean year list
$validYears = @()
foreach ($y in $YearList) {
  # Handle comma-separated string input
  if ($y -match ',') {
    $y.Split(',') | ForEach-Object {
      $cleaned = $_.Trim()
      if ($cleaned -match '^\d{4}$') { $validYears += $cleaned }
      else { Write-Warning "Skipping invalid year: $cleaned" }
    }
  } else {
    $cleaned = $y.Trim()
    if ($cleaned -match '^\d{4}$') { $validYears += $cleaned }
    else { Write-Warning "Skipping invalid year: $cleaned" }
  }
}

if ($validYears.Count -eq 0) {
  Write-Error "No valid years provided. Years must be 4-digit numbers (e.g., 2020)"
  Write-Output "Usage: .\openaq_download_manager.ps1 -YearList 2018,2019,2020 -Background"
  Write-Output "   or: .\openaq_download_manager.ps1 -YearList @(2018,2019,2020) -Background"
  exit 1
}

$YearList = $validYears | Sort-Object -Unique

# Convert LogDir to absolute path and create it
if (-not [System.IO.Path]::IsPathRooted($LogDir)) {
  $LogDir = Join-Path $PSScriptRoot $LogDir
}
New-Item -Path $LogDir -ItemType Directory -Force | Out-Null

$scriptPath = Join-Path $PSScriptRoot "openaq_pull_mulyear_by_loc.ps1"
if (-not (Test-Path $scriptPath)) {
  Write-Error "Cannot find openaq_pull_mulyear_by_loc.ps1 in current directory"
  exit 1
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$masterLog = Join-Path $LogDir "master_$timestamp.log"

function Write-Log {
  param([string]$Message, [string]$LogFile = $masterLog)
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$ts] $Message"
  Add-Content -Path $LogFile -Value $line
  Write-Output $line
}

Write-Log "OpenAQ Download Manager started for years: $($YearList -join ', ')"
Write-Log "Script path: $scriptPath"
Write-Log "Log directory: $LogDir"

if ($Background) {
  Write-Log "Running in background mode (one job per year)"
  
  $jobs = @()
  foreach ($year in $YearList) {
    $yearLog = Join-Path $LogDir "year_${year}_${timestamp}.log"
    Write-Log "Starting background job for year $year (log: $yearLog)"
    
    $jobScript = {
      param($ScriptPath, $Year, $LogFile, $MaxRetries)
      
      function Write-JobLog {
        param([string]$Msg)
        $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        Add-Content -Path $LogFile -Value "[$ts] $Msg"
      }
      
      Write-JobLog "Job started for year $Year"
      
      $attempt = 0
      $success = $false
      
      while ($attempt -lt $MaxRetries -and -not $success) {
        $attempt++
        Write-JobLog "Attempt $attempt of $MaxRetries"
        
        try {
          # Run the download script for single year
          $output = & powershell.exe -ExecutionPolicy Bypass -File $ScriptPath $Year 2>&1
          $output | ForEach-Object { Write-JobLog $_.ToString() }
          
          if ($LASTEXITCODE -eq 0) {
            $success = $true
            Write-JobLog "Year $Year completed successfully"
          } else {
            Write-JobLog "Year $Year failed with exit code $LASTEXITCODE"
            if ($attempt -lt $MaxRetries) {
              $waitSec = [Math]::Min(60, 5 * [Math]::Pow(2, $attempt-1))
              Write-JobLog "Waiting ${waitSec}s before retry..."
              Start-Sleep -Seconds $waitSec
            }
          }
        } catch {
          Write-JobLog "Exception during attempt ${attempt}: $_"
          if ($attempt -lt $MaxRetries) {
            $waitSec = [Math]::Min(60, 5 * [Math]::Pow(2, $attempt-1))
            Write-JobLog "Waiting ${waitSec}s before retry..."
            Start-Sleep -Seconds $waitSec
          }
        }
      }
      
      if (-not $success) {
        Write-JobLog "Year $Year FAILED after $MaxRetries attempts"
        return @{Year=$Year; Success=$false; Attempts=$attempt}
      } else {
        Write-JobLog "Year $Year SUCCESS"
        return @{Year=$Year; Success=$true; Attempts=$attempt}
      }
    }
    
    $job = Start-Job -ScriptBlock $jobScript -ArgumentList $scriptPath, $year, $yearLog, $MaxRetries -Name "OpenAQ_$year"
    $jobs += @{Job=$job; Year=$year; Log=$yearLog}
  }
  
  Write-Log "Started $($jobs.Count) background jobs. Job IDs: $($jobs.Job.Id -join ', ')"
  Write-Log "Monitor with: Get-Job"
  Write-Log "View logs: Get-Content $LogDir\year_*.log -Wait"
  Write-Log "Wait for completion: Wait-Job -Id $($jobs.Job.Id -join ',')"
  
  # Wait for all jobs and collect results
  Write-Log "Waiting for all jobs to complete..."
  $jobs.Job | Wait-Job | Out-Null
  
  Write-Log "\n=== Job Results ==="
  foreach ($jobInfo in $jobs) {
    $result = Receive-Job -Job $jobInfo.Job
    $status = if ($result.Success) { "✓ SUCCESS" } else { "✗ FAILED" }
    Write-Log "Year $($jobInfo.Year): $status (attempts: $($result.Attempts), log: $($jobInfo.Log))"
    Remove-Job -Job $jobInfo.Job
  }
  
  Write-Log "All jobs completed. Check individual logs in: $LogDir"
  
} else {
  Write-Log "Running in foreground mode (sequential)"
  
  foreach ($year in $YearList) {
    $yearLog = Join-Path $LogDir "year_${year}_${timestamp}.log"
    Write-Log "Processing year $year (log: $yearLog)"
    
    $attempt = 0
    $success = $false
    
    while ($attempt -lt $MaxRetries -and -not $success) {
      $attempt++
      Write-Log "Attempt $attempt of $MaxRetries for year $year"
      
      try {
        # Run and tee output to both console and log
        & powershell.exe -ExecutionPolicy Bypass -File $scriptPath $year 2>&1 | Tee-Object -FilePath $yearLog -Append
        
        if ($LASTEXITCODE -eq 0) {
          $success = $true
          Write-Log "Year $year completed successfully"
        } else {
          Write-Log "Year $year failed with exit code $LASTEXITCODE"
          if ($attempt -lt $MaxRetries) {
            $waitSec = [Math]::Min(60, 5 * [Math]::Pow(2, $attempt-1))
            Write-Log "Waiting ${waitSec}s before retry..."
            Start-Sleep -Seconds $waitSec
          }
        }
      } catch {
        Write-Log "Exception for year ${year}: $_"
        if ($attempt -lt $MaxRetries) {
          $waitSec = [Math]::Min(60, 5 * [Math]::Pow(2, $attempt-1))
          Write-Log "Waiting ${waitSec}s before retry..."
          Start-Sleep -Seconds $waitSec
        }
      }
    }
    
    if (-not $success) {
      Write-Log "Year $year FAILED after $MaxRetries attempts" -LogFile $masterLog
    }
  }
  
  Write-Log "All years processed. Logs in: $LogDir"
}

Write-Log "OpenAQ Download Manager finished"
