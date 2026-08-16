# Register Canary Watchdog as a Windows Scheduled Task
# Run this script once from an elevated PowerShell prompt to set up the recurring task.

$taskName = "CanaryWatchdog"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Definition)
$pythonPath = Join-Path $projectRoot ".venv\Scripts\pythonw.exe"
$scriptPath = Join-Path $projectRoot "scripts\canary_watchdog.py"

if (-not (Test-Path $pythonPath)) {
    Write-Warning ".venv not found at $pythonPath -- falling back to system pythonw"
    $pythonPath = "pythonw"
}

$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Task '$taskName' already exists. Removing and re-creating..."
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

$actionParams = @{
    Execute          = $pythonPath
    Argument         = "`"$scriptPath`""
    WorkingDirectory = $projectRoot
}
$action = New-ScheduledTaskAction @actionParams

# run every 5 minutes for the next 10 years (Task Scheduler rejects [TimeSpan]::MaxValue as out of range)
$triggerParams = @{
    Once               = $true
    At                 = (Get-Date)
    RepetitionInterval = (New-TimeSpan -Minutes 5)
    RepetitionDuration = (New-TimeSpan -Days 3650)
}
$trigger = New-ScheduledTaskTrigger @triggerParams

$settingsParams = @{
    AllowStartIfOnBatteries    = $true
    DontStopIfGoingOnBatteries = $true
    StartWhenAvailable         = $true
    ExecutionTimeLimit         = (New-TimeSpan -Minutes 2)
}
$settings = New-ScheduledTaskSettingsSet @settingsParams

$registerParams = @{
    TaskName    = $taskName
    Action      = $action
    Trigger     = $trigger
    Settings    = $settings
    Description = "Canary Watchdog: verifies Loki and Jaeger telemetry pipeline every 5 minutes"
    RunLevel    = "Highest"
}
try {
    Register-ScheduledTask @registerParams -ErrorAction Stop | Out-Null
    Write-Host "Scheduled task '$taskName' registered successfully (every 5 minutes)."
    Write-Host "To verify: Get-ScheduledTask -TaskName $taskName"
    Write-Host "To run now: Start-ScheduledTask -TaskName $taskName"
}
catch {
    Write-Error "Failed to register scheduled task '$taskName': $($_.Exception.Message)"
    exit 1
}