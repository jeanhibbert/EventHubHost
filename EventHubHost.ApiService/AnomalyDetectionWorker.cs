using EventHubHost.ApiService.Data;
using EventHubHost.ApiService.Hubs;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.TimeSeries;

namespace EventHubHost.ApiService;

/// <summary>
/// Background worker that periodically bins recent events into a fixed-width time series and runs
/// SR-CNN (Spectral Residual + CNN) anomaly detection from ML.NET. When an anomaly is fired the
/// worker asks the LLM for a one-paragraph plain-language explanation and broadcasts the result
/// over SignalR as "AnomalyDetected" so the Blazor UI can surface it as a notification.
/// </summary>
public sealed class AnomalyDetectionWorker(
    IOptions<AnomalyOptions> anomalyOptions,
    IDbContextFactory<CorrelationDbContext> dbContextFactory,
    OllamaCorrelationClient correlationClient,
    EventRepository eventRepository,
    IHubContext<EventIngestionHub> hubContext,
    ILogger<AnomalyDetectionWorker> logger) : BackgroundService
{
    private sealed class TimeBin
    {
        public float Count { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = anomalyOptions.Value;
        if (!options.Enabled)
        {
            logger.LogInformation("Anomaly detection disabled.");
            return;
        }

        var interval = TimeSpan.FromSeconds(Math.Max(5, options.ScanIntervalSeconds));
        // Initial warm-up so we don't spam alerts before the simulator has produced data.
        await Task.Delay(TimeSpan.FromSeconds(45), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ScanAsync(options, stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Anomaly scan failed.");
            }

            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                return;
            }
        }
    }

    private async Task ScanAsync(AnomalyOptions options, CancellationToken cancellationToken)
    {
        using var activity = CorrelationTelemetry.Source.StartActivity("anomaly.scan");
        var since = DateTimeOffset.UtcNow.AddSeconds(-options.WindowSeconds);

        await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await dbContext.Events
            .AsNoTracking()
            .Where(e => e.OccurredAt >= since)
            .Select(e => new { e.OccurredAt, e.SourceSystem, e.EventType })
            .ToArrayAsync(cancellationToken);

        if (rows.Length < options.MinPointsToScan)
        {
            return;
        }

        // Bin into fixed-width buckets for the entire window.
        var bucketCount = Math.Max(options.MinPointsToScan, options.WindowSeconds / Math.Max(1, options.BinSeconds));
        var bucketDuration = TimeSpan.FromSeconds(options.WindowSeconds / (double)bucketCount);
        var bins = new TimeBin[bucketCount];
        for (var i = 0; i < bucketCount; i++)
        {
            bins[i] = new TimeBin();
        }

        foreach (var row in rows)
        {
            var offset = (row.OccurredAt - since).TotalSeconds;
            var index = (int)(offset / bucketDuration.TotalSeconds);
            if (index >= 0 && index < bucketCount)
            {
                bins[index].Count++;
            }
        }

        var mlContext = new MLContext(seed: 1);
        var dataView = mlContext.Data.LoadFromEnumerable(bins);
        IReadOnlyList<AnomalyDetection.SrCnnAnomalyHit> anomalies;
        try
        {
            var predictions = mlContext.AnomalyDetection.DetectEntireAnomalyBySrCnn(
                dataView,
                outputColumnName: "Prediction",
                inputColumnName: nameof(TimeBin.Count),
                threshold: 0.3,
                batchSize: bucketCount,
                sensitivity: options.Sensitivity,
                detectMode: SrCnnDetectMode.AnomalyAndMargin);

            anomalies = AnomalyDetection.ParseSrCnnPredictions(predictions);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "SR-CNN detection skipped (insufficient signal).");
            return;
        }

        if (anomalies.Count == 0)
        {
            activity?.SetTag("anomalies", 0);
            return;
        }

        activity?.SetTag("anomalies", anomalies.Count);

        // Take the most recent anomaly only, to avoid flooding the UI.
        var latest = anomalies[^1];
        var binStart = since + bucketDuration * latest.Index;
        var binEnd = binStart + bucketDuration;
        var topGroup = rows
            .Where(r => r.OccurredAt >= binStart && r.OccurredAt < binEnd)
            .GroupBy(r => new { r.SourceSystem, r.EventType })
            .OrderByDescending(g => g.Count())
            .FirstOrDefault();

        var sourceSystem = topGroup?.Key.SourceSystem ?? "Unknown";
        int? eventType = topGroup?.Key.EventType;
        var observed = bins[latest.Index].Count;
        var expected = latest.ExpectedValue;

        // Ask the LLM for a one-paragraph explanation, but bound it tightly so we don't block the worker.
        string explanation;
        try
        {
            using var explainCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            explainCts.CancelAfter(TimeSpan.FromMinutes(5));
            var status = await eventRepository.GetStatusAsync(explainCts.Token);
            var recent = await eventRepository.GetRecentAsync(60, explainCts.Token);
            var question = $"At approximately {binStart:O}, the per-{options.BinSeconds}s event rate for {sourceSystem}{(eventType.HasValue ? " type " + eventType.Value : string.Empty)} was {observed} (SR-CNN expected ~{expected:0.##}). Briefly explain what could cause this spike using only the supplied event context.";
            var response = await correlationClient.AskAsync(question, recent, status, explainCts.Token);
            explanation = response.Answer;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "LLM anomaly explanation failed; broadcasting raw alert.");
            explanation = $"Observed event rate {observed} for {sourceSystem} at {binStart:O} exceeded the SR-CNN expected value of {expected:0.##}.";
        }

        var alert = new AnomalyAlert(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow,
            sourceSystem,
            eventType,
            observed,
            expected,
            explanation);

        try
        {
            await hubContext.Clients.All.SendAsync("AnomalyDetected", alert, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Anomaly broadcast failed.");
        }
    }
}

/// <summary>
/// Helpers around ML.NET's SR-CNN anomaly detection output. The DetectEntireAnomalyBySrCnn API
/// returns an IDataView with three columns per row: a 3-element vector { isAnomaly, rawScore, magnitude }
/// and (when sensitivity is supplied) a 7-element vector that includes expectedValue + bounds.
/// </summary>
public static class AnomalyDetection
{
    public sealed record SrCnnAnomalyHit(int Index, double RawScore, double ExpectedValue);

    public static IReadOnlyList<SrCnnAnomalyHit> ParseSrCnnPredictions(IDataView predictions)
    {
        var schema = predictions.Schema;
        if (!schema.GetColumnOrNull("Prediction").HasValue)
        {
            return [];
        }

        var output = new List<SrCnnAnomalyHit>();
        using var cursor = predictions.GetRowCursor(schema);
        var getter = cursor.GetGetter<VBuffer<double>>(schema["Prediction"]);
        var index = 0;
        var buffer = default(VBuffer<double>);
        while (cursor.MoveNext())
        {
            getter(ref buffer);
            var values = buffer.GetValues();
            if (values.Length >= 3 && values[0] >= 0.5)
            {
                var expected = values.Length >= 4 ? values[3] : 0.0;
                output.Add(new SrCnnAnomalyHit(index, values[1], expected));
            }
            index++;
        }
        return output;
    }
}
