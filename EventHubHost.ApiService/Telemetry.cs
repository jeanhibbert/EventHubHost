using System.Diagnostics;

namespace EventHubHost.ApiService;

/// <summary>
/// ActivitySource for the EventHubHost correlation pipeline. Spans are exported via the
/// Aspire OTLP exporter and visible in the Aspire dashboard's Trace view.
/// </summary>
public static class CorrelationTelemetry
{
    public const string SourceName = "EventHubHost.Correlation";

    public static readonly ActivitySource Source = new(SourceName);
}
