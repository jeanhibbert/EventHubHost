using System.Net.Http.Json;

namespace EventHubHost.Web;

public sealed class CorrelationApiClient(HttpClient httpClient)
{
    public async Task<CorrelationStatus?> GetStatusAsync(CancellationToken cancellationToken = default) =>
        await httpClient.GetFromJsonAsync<CorrelationStatus>("/correlations/status", cancellationToken);

    public async Task<CorrelationQueryResponse?> AskAsync(string question, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.PostAsJsonAsync("/correlations/query", new CorrelationQueryRequest(question), cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<CorrelationQueryResponse>(cancellationToken);
    }

    public async Task TriggerType2ScenarioAsync(CancellationToken cancellationToken = default)
    {
        var response = await httpClient.PostAsync("/scenarios/type2", content: null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<InsightRecord>> GetRecentInsightsAsync(int take = 25, CancellationToken cancellationToken = default) =>
        await httpClient.GetFromJsonAsync<IReadOnlyList<InsightRecord>>($"/correlations/insights?take={take}", cancellationToken)
            ?? [];
}

public sealed record CorrelationEvent(
    Guid Id,
    string SourceSystem,
    int EventType,
    string Name,
    string Description,
    DateTimeOffset OccurredAt);

public sealed record CorrelationQueryRequest(string Question);

public sealed record CorrelationQueryResponse(
    string Answer,
    bool UsedLlm,
    IReadOnlyList<CorrelationEvent> ContextEvents);

public sealed record CorrelationStatus(
    int TotalEvents,
    int SystemAEvents,
    int SystemBEvents,
    int SystemAType2Events,
    int SystemBUniqueEvents,
    int TemporalMatches,
    int TemporalWindowSeconds,
    bool SqlStoreAvailable,
    bool VectorStoreAvailable,
    string EmbeddingModel,
    IReadOnlyList<CorrelationEvent> RecentEvents);

public sealed record InsightRecord(
    Guid Id,
    DateTimeOffset AskedAt,
    string Question,
    string Answer,
    bool UsedLlm,
    int VectorMatchCount,
    int RecentEventCount,
    int TemporalMatches,
    int TemporalWindowSeconds);

public sealed class CorrelationApiOptions
{
    public string EventHubUrl { get; set; } = "http://localhost:5562/hubs/events";
}