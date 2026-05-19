using Microsoft.AspNetCore.SignalR;

namespace EventHubHost.ApiService.Hubs;

/// <summary>
/// SignalR hub used for three broadcast channels:
///  - "EventIngested"            : every newly persisted event
///  - "InsightTokenAppended"     : streamed token chunks for an in-flight LLM answer
///  - "InsightAnswerCompleted"   : final grounded answer for an in-flight LLM answer
///  - "AnomalyDetected"          : SR-CNN anomaly fired by the AnomalyDetectionWorker
/// </summary>
public sealed class EventIngestionHub : Hub
{
}
