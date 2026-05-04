using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using EventHubHost.ApiService.Data;
using EventHubHost.ApiService.Hubs;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace EventHubHost.ApiService;

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
    IReadOnlyList<CorrelationEvent> RecentEvents);

public sealed class OllamaOptions
{
    public string Endpoint { get; set; } = "http://localhost:11434";
    public string Model { get; set; } = "llama3.2:1b";
    public bool AutoPullModel { get; set; } = true;
}

public sealed class CorrelationOptions
{
    public int TemporalWindowSeconds { get; set; } = 15;
}

public static class CorrelationEventFactory
{
    public const int SystemATypeThatCausesSystemB = 2;
    public const int SystemBUniqueEventType = 9002;

    public static CorrelationEvent CreateSystemAEvent(int eventType, DateTimeOffset? occurredAt = null) =>
        new(
            Guid.NewGuid(),
            "System A",
            eventType,
            $"System A type {eventType}",
            eventType == SystemATypeThatCausesSystemB
                ? "System A emitted an independent type 2 event."
                : "System A emitted a random business event.",
            occurredAt ?? DateTimeOffset.UtcNow);

    public static CorrelationEvent CreateSystemBEvent(int eventType, DateTimeOffset? occurredAt = null)
    {
        var unique = eventType == SystemBUniqueEventType;
        return new CorrelationEvent(
            Guid.NewGuid(),
            "System B",
            eventType,
            unique ? "System B unique response" : $"System B type {eventType}",
            unique
                ? "System B emitted a unique event that may be temporally related to recent System A activity."
                : "System B emitted a random business event.",
            occurredAt ?? DateTimeOffset.UtcNow);
    }

    public static IReadOnlyList<CorrelationEvent> CreateEnforcedScenario()
    {
        var scenarioStart = DateTimeOffset.UtcNow;
        return
        [
            CreateSystemAEvent(SystemATypeThatCausesSystemB, scenarioStart),
            CreateSystemBEvent(SystemBUniqueEventType, scenarioStart.AddSeconds(2))
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
                var occurredAt = DateTimeOffset.UtcNow;
                await repository.AddAsync(CorrelationEventFactory.CreateSystemAEvent(type, occurredAt), stoppingToken);

                if (type == CorrelationEventFactory.SystemATypeThatCausesSystemB)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    await repository.AddAsync(
                        CorrelationEventFactory.CreateSystemBEvent(CorrelationEventFactory.SystemBUniqueEventType, occurredAt.AddSeconds(1)),
                        stoppingToken);
                }
                else if (Random.Shared.NextDouble() > 0.35)
                {
                    await repository.AddAsync(
                        CorrelationEventFactory.CreateSystemBEvent(Random.Shared.Next(1, 5), DateTimeOffset.UtcNow),
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

public sealed class EventRepository(
    IDbContextFactory<CorrelationDbContext> dbContextFactory,
    IHubContext<EventIngestionHub> hubContext,
    IOptions<CorrelationOptions> options,
    ILogger<EventRepository> logger)
{
    public async Task AddAsync(CorrelationEvent eventItem, CancellationToken cancellationToken)
    {
        try
        {
            await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
            dbContext.Events.Add(ToEntity(eventItem));
            await dbContext.SaveChangesAsync(cancellationToken);
            await hubContext.Clients.All.SendAsync("EventIngested", eventItem, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "SQL event persistence failed.");
            throw;
        }
    }

    public async Task<IReadOnlyList<CorrelationEvent>> GetRecentAsync(int take, CancellationToken cancellationToken)
    {
        await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var events = await dbContext.Events
            .AsNoTracking()
            .OrderByDescending(eventItem => eventItem.OccurredAt)
            .Take(take)
            .ToArrayAsync(cancellationToken);

        return events.Select(ToModel).ToArray();
    }

    public async Task<CorrelationStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
            var recentEntities = await dbContext.Events
                .AsNoTracking()
                .OrderByDescending(eventItem => eventItem.OccurredAt)
                .Take(250)
                .ToArrayAsync(cancellationToken);
            var recentEvents = recentEntities.Select(ToModel).ToArray();
            var temporalMatches = CountTemporalMatches(recentEvents, options.Value.TemporalWindowSeconds);

            return new CorrelationStatus(
                await dbContext.Events.CountAsync(cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System A", cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System B", cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == CorrelationEventFactory.SystemATypeThatCausesSystemB, cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == CorrelationEventFactory.SystemBUniqueEventType, cancellationToken),
                temporalMatches,
                options.Value.TemporalWindowSeconds,
                true,
                recentEvents.Take(20).ToArray());
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "SQL status query failed.");
            return new CorrelationStatus(0, 0, 0, 0, 0, 0, options.Value.TemporalWindowSeconds, false, []);
        }
    }

    public static int CountTemporalMatches(IReadOnlyList<CorrelationEvent> events, int temporalWindowSeconds)
    {
        var orderedEvents = events.OrderBy(eventItem => eventItem.OccurredAt).ToArray();
        var window = TimeSpan.FromSeconds(temporalWindowSeconds);

        return orderedEvents.Count(systemAEvent =>
            systemAEvent.SourceSystem == "System A"
            && systemAEvent.EventType == CorrelationEventFactory.SystemATypeThatCausesSystemB
            && orderedEvents.Any(systemBEvent =>
                systemBEvent.SourceSystem == "System B"
                && systemBEvent.EventType == CorrelationEventFactory.SystemBUniqueEventType
                && systemBEvent.OccurredAt >= systemAEvent.OccurredAt
                && systemBEvent.OccurredAt - systemAEvent.OccurredAt <= window));
    }

    private static CorrelationEventEntity ToEntity(CorrelationEvent eventItem) =>
        new()
        {
            Id = eventItem.Id,
            SourceSystem = eventItem.SourceSystem,
            EventType = eventItem.EventType,
            Name = eventItem.Name,
            Description = eventItem.Description,
            OccurredAt = eventItem.OccurredAt
        };

    private static CorrelationEvent ToModel(CorrelationEventEntity eventItem) =>
        new(
            eventItem.Id,
            eventItem.SourceSystem,
            eventItem.EventType,
            eventItem.Name,
            eventItem.Description,
            eventItem.OccurredAt);
}

public sealed class OllamaCorrelationClient(IOptions<OllamaOptions> options, ILogger<OllamaCorrelationClient> logger) : IDisposable
{
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromMinutes(10)
    };
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
                if (!IsGroundedAnswer(answer, status))
                {
                    var correctivePrompt = CreateCorrectivePrompt(prompt, answer, status);
                    answer = await GenerateAsync(correctivePrompt, allowModelPull: false, cancellationToken);
                }

                if (!IsGroundedAnswer(answer, status))
                {
                    answer = CreateVerifiedSqlAnswer(events, status);
                }

                return new CorrelationQueryResponse(answer, true, events.Take(30).ToArray());
            }
            catch (Exception ex) when (attempt < 3)
            {
                logger.LogInformation(ex, "Ollama generation attempt {Attempt} failed; retrying.", attempt);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Ollama generation failed; returning local temporal correlation summary.");
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

    }

    public void Dispose()
    {
        httpClient.Dispose();
        modelPullLock.Dispose();
    }

    private static string CreatePrompt(string question, IReadOnlyList<CorrelationEvent> events, CorrelationStatus status)
    {
        var eventLines = events
            .OrderBy(eventItem => eventItem.OccurredAt)
            .TakeLast(80)
            .Select(eventItem =>
                $"- {eventItem.OccurredAt:O} | {eventItem.SourceSystem} | type {eventItem.EventType} | {eventItem.Description}");
        var matchedPairLines = CreateMatchedPairLines(events, status.TemporalWindowSeconds);
        var computedFinding = status.TemporalMatches > 0
            ? $"SUPPORTED: the SQL analysis found {status.TemporalMatches} time-window match(es). A time-window match means a System A type 2 event was followed by System B type 9002 within {status.TemporalWindowSeconds} seconds."
            : "NOT YET SUPPORTED: the SQL analysis found zero time-window matches.";

        return $$"""
You are a centralized event correlation analyst. Use only the supplied SQL event dataset context and temporal summary.

The source systems are independent. There is no shared correlation ID, key, trace ID, or transaction ID. Do not claim that events are linked by identity. Reason only from event timing, event type, and observed counts.

Correlation hypothesis being tested:
When System A emits event type 2, System B tends to emit its unique event type 9002 shortly afterward.

Temporal summary computed from the SQL dataset:
- Total events: {{status.TotalEvents}}
- System A events: {{status.SystemAEvents}}
- System B events: {{status.SystemBEvents}}
- System A type 2 events: {{status.SystemAType2Events}}
- System B unique type 9002 events: {{status.SystemBUniqueEvents}}
- Time-window matches: {{status.TemporalMatches}} System A type 2 event(s) were followed by a System B type 9002 event within {{status.TemporalWindowSeconds}} seconds.

Deterministic SQL-computed finding:
{{computedFinding}}

Exact matched event examples computed from the SQL event rows supplied to this prompt:
{{string.Join(Environment.NewLine, matchedPairLines)}}

Recent persisted events from SQL, ordered by occurrence time:
{{string.Join(Environment.NewLine, eventLines)}}

Question: {{question}}

Answer concisely. Explain the deterministic SQL-computed finding in plain language and cite only the time-window evidence supplied above. Do not imply a shared correlation key exists. Do not claim statistical significance, causation, proof, or confidence beyond the supplied time-window counts. Do not invent timestamps, counts, or unmatched-event claims. If you cite timestamps, copy them only from the exact matched event examples.
""";
    }

    private static string CreateCorrectivePrompt(string originalPrompt, string previousAnswer, CorrelationStatus status) =>
        $$"""
{{originalPrompt}}

Your previous answer was not sufficiently grounded:
{{previousAnswer}}

Rewrite the answer using exactly this format and no other claims:
- Finding: The SQL event dataset {{(status.TemporalMatches > 0 ? "supports" : "does not yet support")}} the temporal hypothesis.
- Evidence: {{status.TemporalMatches}} System A type 2 event(s) were followed by System B type 9002 within {{status.TemporalWindowSeconds}} seconds.
- Example: Cite one exact matched event example from the supplied examples, or say no example was supplied.
- Limit: This is time-window evidence only; it does not prove causation or statistical significance.

Rules: do not use the words statistically, significant, probability, proves, confidence, or correlation key. Do not add counts other than {{status.TemporalMatches}} and {{status.TemporalWindowSeconds}}.
""";

    private static bool IsGroundedAnswer(string answer, CorrelationStatus status)
    {
        string[] forbiddenTerms = ["statistically", "probability", "proves", "proof", "confidence", "correlation key"];
        var requiredEvidence = $"Evidence: {status.TemporalMatches} System A type 2 event(s) were followed by System B type 9002 within {status.TemporalWindowSeconds} seconds";
        return answer.Contains(requiredEvidence, StringComparison.OrdinalIgnoreCase)
            && !forbiddenTerms.Any(term => answer.Contains(term, StringComparison.OrdinalIgnoreCase));
    }

    private static string CreateVerifiedSqlAnswer(IReadOnlyList<CorrelationEvent> events, CorrelationStatus status)
    {
        var matchedPairLines = CreateMatchedPairLines(events, status.TemporalWindowSeconds);
        var example = matchedPairLines.Count > 0
            ? matchedPairLines[0]
            : "No exact matched example was supplied in the current SQL event context.";

        return $$"""
Finding: The persisted SQL event dataset {{(status.TemporalMatches > 0 ? "supports" : "does not yet support")}} the temporal hypothesis.
Evidence: {{status.TemporalMatches}} System A type 2 event(s) were followed by System B type 9002 within {{status.TemporalWindowSeconds}} seconds.
Example: {{example}}
Limit: This is time-window evidence only; it does not prove causation or statistical significance.
""";
    }

    private static IReadOnlyList<string> CreateMatchedPairLines(IReadOnlyList<CorrelationEvent> events, int temporalWindowSeconds)
    {
        var orderedEvents = events.OrderBy(eventItem => eventItem.OccurredAt).ToArray();
        var window = TimeSpan.FromSeconds(temporalWindowSeconds);
        var pairs = new List<string>();

        foreach (var systemAEvent in orderedEvents.Where(eventItem =>
            eventItem.SourceSystem == "System A"
            && eventItem.EventType == CorrelationEventFactory.SystemATypeThatCausesSystemB))
        {
            var systemBEvent = orderedEvents.FirstOrDefault(eventItem =>
                eventItem.SourceSystem == "System B"
                && eventItem.EventType == CorrelationEventFactory.SystemBUniqueEventType
                && eventItem.OccurredAt >= systemAEvent.OccurredAt
                && eventItem.OccurredAt - systemAEvent.OccurredAt <= window);

            if (systemBEvent is not null)
            {
                pairs.Add($"- System A type 2 at {systemAEvent.OccurredAt:O}; System B type 9002 at {systemBEvent.OccurredAt:O}; elapsed {(systemBEvent.OccurredAt - systemAEvent.OccurredAt).TotalSeconds:0.###} seconds.");
            }
        }

        return pairs.Count > 0
            ? pairs.Take(8).ToArray()
            : ["- No matched event examples were present in the supplied recent rows."];
    }

    private static string CreateFallbackAnswer(string question, CorrelationStatus status)
    {
        var detected = status.TemporalMatches > 0;
        return detected
            ? $"Local fallback summary because the LLM is not ready: the SQL event dataset supports the temporal correlation hypothesis. {status.TemporalMatches} System A type 2 event(s) were followed by System B type 9002 within {status.TemporalWindowSeconds} seconds. Question: {question}"
            : $"Local fallback summary because the LLM is not ready: the SQL event dataset does not yet show the temporal correlation. System A type 2 events: {status.SystemAType2Events}; System B type 9002 events: {status.SystemBUniqueEvents}; time-window matches: {status.TemporalMatches}. Question: {question}";
    }
}
