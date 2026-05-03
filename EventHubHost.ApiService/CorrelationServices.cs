using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace EventHubHost.ApiService;

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

public sealed class QdrantOptions
{
    public string Endpoint { get; set; } = "http://localhost:6333";
    public string Collection { get; set; } = "events";
}

public sealed class OllamaOptions
{
    public string Endpoint { get; set; } = "http://localhost:11434";
    public string Model { get; set; } = "llama3.2:1b";
    public bool AutoPullModel { get; set; } = true;
}

public static class CorrelationEventFactory
{
    public const int SystemATypeThatCausesSystemB = 2;
    public const int SystemBUniqueEventType = 9002;

    public static CorrelationEvent CreateSystemAEvent(int eventType, string? correlationKey = null)
    {
        var key = correlationKey ?? $"A-{Guid.NewGuid():N}";
        return new CorrelationEvent(
            Guid.NewGuid(),
            "System A",
            eventType,
            $"System A type {eventType}",
            eventType == SystemATypeThatCausesSystemB
                ? "System A emitted the enforced type 2 event."
                : "System A emitted a random business event.",
            DateTimeOffset.UtcNow,
            key);
    }

    public static CorrelationEvent CreateSystemBEvent(int eventType, string? correlationKey = null)
    {
        var unique = eventType == SystemBUniqueEventType;
        return new CorrelationEvent(
            Guid.NewGuid(),
            "System B",
            eventType,
            unique ? "System B unique response" : $"System B type {eventType}",
            unique
                ? "System B emitted the unique event that only follows a System A type 2 event."
                : "System B emitted a random business event.",
            DateTimeOffset.UtcNow,
            correlationKey ?? $"B-{Guid.NewGuid():N}");
    }

    public static IReadOnlyList<CorrelationEvent> CreateEnforcedScenario()
    {
        var key = $"scenario-{Guid.NewGuid():N}";
        return
        [
            CreateSystemAEvent(SystemATypeThatCausesSystemB, key),
            CreateSystemBEvent(SystemBUniqueEventType, key)
        ];
    }
}

public sealed class EventSimulationWorker(EventRepository repository, ILogger<EventSimulationWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(4));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var type = Random.Shared.Next(1, 5);
                var systemAEvent = CorrelationEventFactory.CreateSystemAEvent(type);
                await repository.AddAsync(systemAEvent, stoppingToken);

                if (type == CorrelationEventFactory.SystemATypeThatCausesSystemB)
                {
                    await repository.AddAsync(
                        CorrelationEventFactory.CreateSystemBEvent(CorrelationEventFactory.SystemBUniqueEventType, systemAEvent.CorrelationKey),
                        stoppingToken);
                }
                else if (Random.Shared.NextDouble() > 0.35)
                {
                    await repository.AddAsync(
                        CorrelationEventFactory.CreateSystemBEvent(Random.Shared.Next(1, 5)),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Event simulation tick failed.");
            }
        }
    }
}

public sealed class EventRepository(QdrantEventVectorStore vectorStore, ILogger<EventRepository> logger)
{
    private readonly List<CorrelationEvent> events = [];
    private readonly Lock gate = new();

    public async Task AddAsync(CorrelationEvent eventItem, CancellationToken cancellationToken)
    {
        lock (gate)
        {
            events.Add(eventItem);
            if (events.Count > 500)
            {
                events.RemoveRange(0, events.Count - 500);
            }
        }

        try
        {
            await vectorStore.UpsertAsync(eventItem, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Vector store write failed; event remains available in memory.");
        }
    }

    public async Task<IReadOnlyList<CorrelationEvent>> GetRecentAsync(int take, CancellationToken cancellationToken)
    {
        try
        {
            var storedEvents = await vectorStore.GetRecentAsync(take, cancellationToken);
            if (storedEvents.Count > 0)
            {
                return storedEvents;
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Vector store read failed; using in-memory events.");
        }

        lock (gate)
        {
            return events
                .OrderByDescending(eventItem => eventItem.OccurredAt)
                .Take(take)
                .ToArray();
        }
    }

    public async Task<CorrelationStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        var recentEvents = await GetRecentAsync(100, cancellationToken);
        var systemAType2Keys = recentEvents
            .Where(eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == CorrelationEventFactory.SystemATypeThatCausesSystemB)
            .Select(eventItem => eventItem.CorrelationKey)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var systemBUniqueKeys = recentEvents
            .Where(eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == CorrelationEventFactory.SystemBUniqueEventType)
            .Select(eventItem => eventItem.CorrelationKey)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return new CorrelationStatus(
            recentEvents.Count,
            recentEvents.Count(eventItem => eventItem.SourceSystem == "System A"),
            recentEvents.Count(eventItem => eventItem.SourceSystem == "System B"),
            systemAType2Keys.Count,
            systemBUniqueKeys.Count,
            systemAType2Keys.Intersect(systemBUniqueKeys, StringComparer.OrdinalIgnoreCase).Count(),
            await vectorStore.IsAvailableAsync(cancellationToken),
            recentEvents.Take(20).ToArray());
    }
}

public sealed class QdrantEventVectorStore(HttpClient httpClient, IOptions<QdrantOptions> options)
{
    private const int VectorSize = 16;
    private readonly SemaphoreSlim initializationLock = new(1, 1);
    private bool initialized;

    public async Task UpsertAsync(CorrelationEvent eventItem, CancellationToken cancellationToken)
    {
        ConfigureClient();
        await EnsureCollectionAsync(cancellationToken);

        var payload = new
        {
            points = new[]
            {
                new
                {
                    id = eventItem.Id,
                    vector = CreateEmbedding(eventItem),
                    payload = new
                    {
                        eventItem.SourceSystem,
                        eventItem.EventType,
                        eventItem.Name,
                        eventItem.Description,
                        OccurredAt = eventItem.OccurredAt,
                        eventItem.CorrelationKey
                    }
                }
            }
        };

        var response = await httpClient.PutAsJsonAsync($"/collections/{options.Value.Collection}/points?wait=true", payload, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<CorrelationEvent>> GetRecentAsync(int take, CancellationToken cancellationToken)
    {
        ConfigureClient();
        await EnsureCollectionAsync(cancellationToken);

        var response = await httpClient.PostAsJsonAsync($"/collections/{options.Value.Collection}/points/scroll", new
        {
            limit = take,
            with_payload = true,
            with_vector = false
        }, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        if (!document.RootElement.TryGetProperty("result", out var result)
            || !result.TryGetProperty("points", out var points))
        {
            return [];
        }

        return points.EnumerateArray()
            .Select(TryReadEvent)
            .OfType<CorrelationEvent>()
            .OrderByDescending(eventItem => eventItem.OccurredAt)
            .Take(take)
            .ToArray();
    }

    public async Task<bool> IsAvailableAsync(CancellationToken cancellationToken)
    {
        try
        {
            ConfigureClient();
            var response = await httpClient.GetAsync("/collections", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private async Task EnsureCollectionAsync(CancellationToken cancellationToken)
    {
        if (initialized)
        {
            return;
        }

        await initializationLock.WaitAsync(cancellationToken);
        try
        {
            if (initialized)
            {
                return;
            }

            var getResponse = await httpClient.GetAsync($"/collections/{options.Value.Collection}", cancellationToken);
            if (getResponse.IsSuccessStatusCode)
            {
                initialized = true;
                return;
            }

            var createResponse = await httpClient.PutAsJsonAsync($"/collections/{options.Value.Collection}", new
            {
                vectors = new
                {
                    size = VectorSize,
                    distance = "Cosine"
                }
            }, cancellationToken);

            createResponse.EnsureSuccessStatusCode();
            initialized = true;
        }
        finally
        {
            initializationLock.Release();
        }
    }

    private void ConfigureClient()
    {
        if (httpClient.BaseAddress is not null)
        {
            return;
        }

        httpClient.BaseAddress = new Uri(options.Value.Endpoint.TrimEnd('/'));
    }

    private static CorrelationEvent? TryReadEvent(JsonElement point)
    {
        try
        {
            var payload = point.GetProperty("payload");
            return new CorrelationEvent(
                point.GetProperty("id").GetGuid(),
                GetString(payload, "sourceSystem", "SourceSystem") ?? "Unknown",
                GetInt32(payload, "eventType", "EventType"),
                GetString(payload, "name", "Name") ?? "Unknown event",
                GetString(payload, "description", "Description") ?? string.Empty,
                GetDateTimeOffset(payload, "occurredAt", "OccurredAt"),
                GetString(payload, "correlationKey", "CorrelationKey") ?? string.Empty);
        }
        catch
        {
            return null;
        }
    }

    private static string? GetString(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value))
            {
                return value.GetString();
            }
        }

        return null;
    }

    private static int GetInt32(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value))
            {
                return value.GetInt32();
            }
        }

        return 0;
    }

    private static DateTimeOffset GetDateTimeOffset(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value))
            {
                return value.GetDateTimeOffset();
            }
        }

        return DateTimeOffset.MinValue;
    }

    private static double[] CreateEmbedding(CorrelationEvent eventItem)
    {
        var vector = new double[VectorSize];
        var text = $"{eventItem.SourceSystem}|{eventItem.EventType}|{eventItem.Name}|{eventItem.Description}|{eventItem.CorrelationKey}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));

        for (var index = 0; index < VectorSize; index++)
        {
            vector[index] = (hash[index] / 255d * 2d) - 1d;
        }

        vector[0] = eventItem.SourceSystem == "System A" ? 1d : -1d;
        vector[1] = eventItem.EventType / 10000d;
        vector[2] = eventItem.EventType == CorrelationEventFactory.SystemBUniqueEventType ? 1d : 0d;
        return vector;
    }
}

public sealed class OllamaCorrelationClient(HttpClient httpClient, IOptions<OllamaOptions> options, ILogger<OllamaCorrelationClient> logger)
{
    private readonly SemaphoreSlim modelPullLock = new(1, 1);

    public async Task<CorrelationQueryResponse> AskAsync(
        string question,
        IReadOnlyList<CorrelationEvent> events,
        CorrelationStatus status,
        CancellationToken cancellationToken)
    {
        ConfigureClient();
        var prompt = CreatePrompt(question, events, status);

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var answer = await GenerateAsync(prompt, allowModelPull: true, cancellationToken);
                return new CorrelationQueryResponse(answer, true, events.Take(30).ToArray());
            }
            catch (Exception ex) when (attempt < 3)
            {
                logger.LogInformation(ex, "Ollama generation attempt {Attempt} failed; retrying.", attempt);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Ollama generation failed; returning local correlation summary.");
                return new CorrelationQueryResponse(CreateFallbackAnswer(question, status), false, events.Take(30).ToArray());
            }
        }

        return new CorrelationQueryResponse(CreateFallbackAnswer(question, status), false, events.Take(30).ToArray());
    }

    private async Task<string> GenerateAsync(string prompt, bool allowModelPull, CancellationToken cancellationToken)
    {
        var response = await httpClient.PostAsJsonAsync("/api/generate", new
        {
            model = options.Value.Model,
            prompt,
            stream = false,
            options = new
            {
                temperature = 0.1
            }
        }, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(cancellationToken);
            if (allowModelPull
                && options.Value.AutoPullModel
                && (response.StatusCode == HttpStatusCode.NotFound
                    || error.Contains("not found", StringComparison.OrdinalIgnoreCase)
                    || error.Contains("pull", StringComparison.OrdinalIgnoreCase)))
            {
                await PullModelAsync(cancellationToken);
                return await GenerateAsync(prompt, allowModelPull: false, cancellationToken);
            }

            throw new InvalidOperationException($"Ollama returned {(int)response.StatusCode}: {error}");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        return document.RootElement.TryGetProperty("response", out var answer)
            ? answer.GetString() ?? "The LLM returned an empty response."
            : "The LLM response did not include an answer.";
    }

    private async Task PullModelAsync(CancellationToken cancellationToken)
    {
        await modelPullLock.WaitAsync(cancellationToken);
        try
        {
            var response = await httpClient.PostAsJsonAsync("/api/pull", new
            {
                name = options.Value.Model,
                stream = false
            }, cancellationToken);
            response.EnsureSuccessStatusCode();
        }
        finally
        {
            modelPullLock.Release();
        }
    }

    private void ConfigureClient()
    {
        if (httpClient.BaseAddress is null)
        {
            httpClient.BaseAddress = new Uri(options.Value.Endpoint.TrimEnd('/'));
        }

        httpClient.Timeout = TimeSpan.FromMinutes(10);
    }

    private static string CreatePrompt(string question, IReadOnlyList<CorrelationEvent> events, CorrelationStatus status)
    {
        var eventLines = events.Take(40).Select(eventItem =>
            $"- {eventItem.OccurredAt:O} | {eventItem.SourceSystem} | type {eventItem.EventType} | key {eventItem.CorrelationKey} | {eventItem.Description}");

        return $$"""
You are a centralized event correlation engine. Use only the supplied event context.

Known enforced scenario for validation: when System A emits event type 2, System B emits unique event type 9002 with the same correlation key.

Current counts:
- Total events: {{status.TotalEvents}}
- System A type 2 events: {{status.SystemAType2Events}}
- System B unique events: {{status.SystemBUniqueEvents}}
- Matched correlation keys: {{status.MatchedCorrelationKeys}}

Recent event context:
{{string.Join(Environment.NewLine, eventLines)}}

Question: {{question}}

Answer concisely. Say whether the System A type 2 to System B unique event correlation is present, and cite the observed evidence from the context.
""";
    }

    private static string CreateFallbackAnswer(string question, CorrelationStatus status)
    {
        var detected = status.MatchedCorrelationKeys > 0;
        return detected
            ? $"Local fallback summary because the LLM is not ready: yes, the correlation is present. I found {status.SystemAType2Events} System A type 2 event(s), {status.SystemBUniqueEvents} System B unique event(s), and {status.MatchedCorrelationKeys} shared correlation key match(es). Question: {question}"
            : $"Local fallback summary because the LLM is not ready: the enforced correlation has not been observed yet. System A type 2 events: {status.SystemAType2Events}; System B unique events: {status.SystemBUniqueEvents}; shared keys: {status.MatchedCorrelationKeys}. Question: {question}";
    }
}