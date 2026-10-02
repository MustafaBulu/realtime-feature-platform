param(
    [string]$FeatureApiUrl = "http://localhost:8080",
    [string]$WorkloadUrl = "http://localhost:8082",
    [string]$WarmupDuration = "PT2S",
    [string]$MeasurementDuration = "PT6S",
    [int]$EventRatePerSecond = 20,
    [int]$ReadRatePerSecond = 5,
    [int]$EntityCardinality = 1,
    [int]$ManualEventCount = 7,
    [int]$PollTimeoutSeconds = 10,
    [int]$PollIntervalMillis = 100
)

$ErrorActionPreference = "Stop"

function Invoke-JsonPost {
    param(
        [string]$Uri,
        [object]$Body
    )

    Invoke-RestMethod `
        -Method Post `
        -Uri $Uri `
        -ContentType "application/json" `
        -Body ($Body | ConvertTo-Json -Depth 10)
}

function Measure-JsonGet {
    param([string]$Uri)

    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-RestMethod -Method Get -Uri $Uri
    $watch.Stop()

    [pscustomobject]@{
        LatencyMillis = $watch.Elapsed.TotalMilliseconds
        Response = $response
    }
}

function Wait-FeatureValueAtLeast {
    param(
        [string]$Uri,
        [double]$MinimumValue,
        [int]$TimeoutSeconds,
        [int]$IntervalMillis
    )

    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    do {
        $read = Invoke-RestMethod -Method Get -Uri $Uri
        if ($null -ne $read.value -and [double]$read.value -ge $MinimumValue) {
            $watch.Stop()
            return [pscustomobject]@{
                LatencyMillis = $watch.Elapsed.TotalMilliseconds
                Response = $read
            }
        }

        Start-Sleep -Milliseconds $IntervalMillis
    } while ($watch.Elapsed.TotalSeconds -lt $TimeoutSeconds)

    throw "Timed out waiting for $Uri to reach at least $MinimumValue"
}

function Wait-FeatureValueEquals {
    param(
        [string]$Uri,
        [double]$ExpectedValue,
        [int]$TimeoutSeconds,
        [int]$IntervalMillis
    )

    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    do {
        $read = Invoke-RestMethod -Method Get -Uri $Uri
        if ($null -ne $read.value -and [double]$read.value -eq $ExpectedValue) {
            $watch.Stop()
            return [pscustomobject]@{
                LatencyMillis = $watch.Elapsed.TotalMilliseconds
                Response = $read
            }
        }

        Start-Sleep -Milliseconds $IntervalMillis
    } while ($watch.Elapsed.TotalSeconds -lt $TimeoutSeconds)

    throw "Timed out waiting for $Uri to equal $ExpectedValue"
}

Write-Host "Running a short benchmark to create comparable realtime and request-time baseline data..."
$benchmark = Invoke-JsonPost `
    -Uri "$WorkloadUrl/benchmarks/request-count" `
    -Body @{
        warmupDuration = $WarmupDuration
        measurementDuration = $MeasurementDuration
        eventRatePerSecond = $EventRatePerSecond
        readRatePerSecond = $ReadRatePerSecond
        entityCardinality = $EntityCardinality
        distribution = "UNIFORM"
        writeHistory = $true
        runSqlBaseline = $true
        correctnessSampleSize = 4
        freshnessProbeCount = 3
        freshnessProbePollInterval = "PT0.1S"
        freshnessProbeTimeout = "PT10S"
        includeRawResults = $false
    }

$runId = $benchmark.runId
$entityId = "bench-$runId-measurement-entity-000000"
$realtimeUri = "$FeatureApiUrl/features/service/$entityId/request_count_total"
$baselineUri = "$FeatureApiUrl/baseline/features/service/$entityId/request_count_total`?benchmarkRunId=$runId"

$baselineRead = Measure-JsonGet -Uri $baselineUri
$currentBaselineValue = if ($null -eq $baselineRead.Response.value) { 0 } else { [double]$baselineRead.Response.value }
$realtimeRead = Wait-FeatureValueEquals `
    -Uri $realtimeUri `
    -ExpectedValue $currentBaselineValue `
    -TimeoutSeconds $PollTimeoutSeconds `
    -IntervalMillis $PollIntervalMillis
$currentRealtimeValue = if ($null -eq $realtimeRead.Response.value) { 0 } else { [double]$realtimeRead.Response.value }

Write-Host ""
Write-Host "Comparable entity"
Write-Host "Run ID:  $runId"
Write-Host "Entity:  service/$entityId"
Write-Host ("Realtime request_count_total: {0} ({1}, {2:N1} ms read)" -f $realtimeRead.Response.value, $realtimeRead.Response.metadata.status, $realtimeRead.LatencyMillis)
Write-Host ("Baseline request_count_total: {0} ({1}, {2:N1} ms read)" -f $baselineRead.Response.value, $baselineRead.Response.metadata.status, $baselineRead.LatencyMillis)
Write-Host ("Parity on this read:        {0}" -f ($currentRealtimeValue -eq $currentBaselineValue))

Write-Host ""
Write-Host "Publishing one new event to the same entity and waiting until Redis-backed realtime read reflects it..."
Invoke-JsonPost `
    -Uri "$WorkloadUrl/workloads/request-count-total" `
    -Body @{
        entityType = "service"
        entityId = $entityId
        samples = @(
            @{
                count = $ManualEventCount
                statusCode = 200
                latencyMs = 80
            }
        )
    } | Out-Null

$expectedRealtimeValue = $currentRealtimeValue + $ManualEventCount
$updated = Wait-FeatureValueAtLeast `
    -Uri $realtimeUri `
    -MinimumValue $expectedRealtimeValue `
    -TimeoutSeconds $PollTimeoutSeconds `
    -IntervalMillis $PollIntervalMillis

$measurement = $benchmark.phases | Where-Object { $_.phase -eq "measurement" } | Select-Object -First 1
$freshness = $benchmark.freshnessProbe
$correctness = $benchmark.correctness

Write-Host ""
Write-Host "Demo summary"
Write-Host ("Manual update-to-availability: {0:N1} ms, new realtime value: {1}" -f $updated.LatencyMillis, $updated.Response.value)
Write-Host ("Benchmark update-to-availability p50/p95/p99: {0}/{1}/{2} ms, successes: {3}/{4}" -f `
    $freshness.updateToAvailabilityLatency.p50Millis, `
    $freshness.updateToAvailabilityLatency.p95Millis, `
    $freshness.updateToAvailabilityLatency.p99Millis, `
    $freshness.successes, `
    $freshness.attempts)
Write-Host ("Benchmark realtime read p50/p95/p99: {0}/{1}/{2} ms" -f `
    $measurement.featureReadLatency.p50Millis, `
    $measurement.featureReadLatency.p95Millis, `
    $measurement.featureReadLatency.p99Millis)
Write-Host ("Single-read latency difference: realtime {0:N1} ms vs baseline {1:N1} ms" -f `
    $realtimeRead.LatencyMillis, `
    $baselineRead.LatencyMillis)
Write-Host ("Correctness sample: {0} matches, {1} mismatches, {2} unavailable" -f `
    $correctness.matches, `
    $correctness.mismatches, `
    $correctness.unavailable)
