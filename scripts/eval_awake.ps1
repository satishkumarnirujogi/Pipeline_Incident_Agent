# Run the eval while asking Windows not to idle-sleep, for as long as this process lives.
# SetThreadExecutionState is a per-process request: no power settings change, and Windows
# drops it when this script exits. A closed lid or a manual sleep still sleeps.
#
#   powershell -File scripts/eval_awake.ps1 -Split dev [-Ablation no_memory] [-Chaos]
#       [-Baseline <run without chaos>] [-Resume <eval_run_id>]
param(
    [string]$Split = "dev",
    [string]$Ablation = "full",
    [switch]$Chaos,
    [string]$Baseline = "",
    [string]$Resume = ""
)

Add-Type -Namespace Win32 -Name Power -MemberDefinition @'
[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);
'@
$ES_CONTINUOUS = [uint32]"0x80000000"
$ES_SYSTEM_REQUIRED = [uint32]"0x00000001"
[void][Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED)
try {
    $env:PYTHONIOENCODING = "utf-8"
    $evalArgs = @("--split", $Split, "--ablation", $Ablation)
    if ($Chaos) { $evalArgs += "--chaos" }
    if ($Baseline) { $evalArgs += @("--baseline", $Baseline) }
    if ($Resume) { $evalArgs += @("--resume", $Resume) }
    uv run python -m eval.run_eval @evalArgs
    exit $LASTEXITCODE
} finally {
    [void][Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS)
}
