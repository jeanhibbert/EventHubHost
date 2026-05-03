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
}

public sealed record CorrelationEvent(
    Guid Id,
    string SourceSystem,
    int EventType,
    string Name,
    string Description,
    DateTimeOffset OccurredAt,
    string CorrelationKey);

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
    int MatchedCorrelationKeys,
    bool VectorStoreAvailable,
    IReadOnlyList<CorrelationEvent> RecentEvents);