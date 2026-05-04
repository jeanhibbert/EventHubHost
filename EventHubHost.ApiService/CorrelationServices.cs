using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using System.Text;
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

/// <summary>
/// Initial response to a streaming Ask: contains the deterministic SQL-only answer (returned
/// immediately so the user always sees a correct answer with no waiting) and a streamId the
/// client subscribes to over SignalR to receive token deltas + the final LLM-elaborated answer.
/// </summary>
public sealed record CorrelationStreamResponse(
    Guid StreamId,
    string DeterministicAnswer,
    int RecentEventCount,
    int VectorMatchCount);

public sealed record InsightTokenMessage(Guid StreamId, string TokenDelta);

public sealed record InsightCompletedMessage(
    Guid StreamId,
    string FinalAnswer,
    bool UsedLlm,
    int VectorMatchCount,
    int RecentEventCount);

public sealed record AnomalyAlert(
    Guid Id,
    DateTimeOffset DetectedAt,
    string SourceSystem,
    int? EventType,
    double ObservedRate,
    double ExpectedRate,
    string Explanation);

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

public sealed class OllamaOptions
{
    public string Endpoint { get; set; } = "http://localhost:11434";
    public string Model { get; set; } = "llama3.2:1b";
    public string EmbeddingModel { get; set; } = "nomic-embed-text";
    public bool AutoPullModel { get; set; } = true;
}

public sealed class QdrantOptions
{
    public string Endpoint { get; set; } = "http://localhost:6333";
    public string Collection { get; set; } = "events";
    public string InsightCollection { get; set; } = "insights";
    public int VectorSize { get; set; } = 768;
}

public sealed class CorrelationOptions
{
    public int TemporalWindowSeconds { get; set; } = 15;
    public int VectorSearchTopK { get; set; } = 12;
    public int MaxRecentEventsForPrompt { get; set; } = 30;
    public int InsightMemoryTopK { get; set; } = 3;
}

public sealed class AnomalyOptions
{
    public bool Enabled { get; set; } = true;
    public int ScanIntervalSeconds { get; set; } = 30;
    public int WindowSeconds { get; set; } = 300;
    public int BinSeconds { get; set; } = 5;
    public int MinPointsToScan { get; set; } = 24;
    public double Sensitivity { get; set; } = 90.0;
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
    IOptions<OllamaOptions> ollamaOptions,
    OllamaEmbeddingClient embeddingClient,
    QdrantEventVectorStore vectorStore,
    ILogger<EventRepository> logger)
{
    public async Task AddAsync(CorrelationEvent eventItem, CancellationToken cancellationToken)
    {
        using var activity = CorrelationTelemetry.Source.StartActivity("ingest.event", ActivityKind.Internal);
        activity?.SetTag("event.source_system", eventItem.SourceSystem);
        activity?.SetTag("event.type", eventItem.EventType);

        try
        {
            await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
            dbContext.Events.Add(ToEntity(eventItem));
            await dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            logger.LogWarning(ex, "SQL event persistence failed.");
            throw;
        }

        try
        {
            var embedding = await embeddingClient.EmbedAsync(EventToEmbeddingText(eventItem), cancellationToken);
            await vectorStore.UpsertAsync(eventItem, embedding, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Vector store upsert failed; SQL row remains the source of truth.");
        }

        try
        {
            await hubContext.Clients.All.SendAsync("EventIngested", eventItem, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "SignalR broadcast failed.");
        }
    }

    public static string EventToEmbeddingText(CorrelationEvent eventItem) =>
        $"{eventItem.SourceSystem} type {eventItem.EventType} | {eventItem.Name} | {eventItem.Description} | occurred at {eventItem.OccurredAt:O}";

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

            var vectorOnline = await vectorStore.IsAvailableAsync(cancellationToken);

            return new CorrelationStatus(
                await dbContext.Events.CountAsync(cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System A", cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System B", cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == CorrelationEventFactory.SystemATypeThatCausesSystemB, cancellationToken),
                await dbContext.Events.CountAsync(eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == CorrelationEventFactory.SystemBUniqueEventType, cancellationToken),
                temporalMatches,
                options.Value.TemporalWindowSeconds,
                true,
                vectorOnline,
                ollamaOptions.Value.EmbeddingModel,
                recentEvents.Take(20).ToArray());
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "SQL status query failed.");
            return new CorrelationStatus(0, 0, 0, 0, 0, 0, options.Value.TemporalWindowSeconds, false, false, ollamaOptions.Value.EmbeddingModel, []);
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

public sealed class OllamaCorrelationClient(
    IOptions<OllamaOptions> options,
    IOptions<CorrelationOptions> correlationOptions,
    OllamaEmbeddingClient embeddingClient,
    QdrantEventVectorStore vectorStore,
    QdrantInsightVectorStore insightVectorStore,
    InsightRepository insightRepository,
    IHubContext<EventIngestionHub> hubContext,
    ILogger<OllamaCorrelationClient> logger) : IDisposable
{
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromMinutes(30)
    };
    private readonly SemaphoreSlim modelPullLock = new(1, 1);

    public async Task<CorrelationQueryResponse> AskAsync(
        string question,
        IReadOnlyList<CorrelationEvent> events,
        CorrelationStatus status,
        CancellationToken cancellationToken)
    {
        using var activity = CorrelationTelemetry.Source.StartActivity("llm.ask", ActivityKind.Internal);
        activity?.SetTag("question.length", question.Length);

        ConfigureClient();

        var (vectorMatches, pastInsights) = await RetrieveContextAsync(question, cancellationToken);
        activity?.SetTag("retrieval.vector_matches", vectorMatches.Count);
        activity?.SetTag("retrieval.past_insights", pastInsights.Count);

        var prompt = CreatePrompt(question, events, vectorMatches, pastInsights, status, correlationOptions.Value);

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

                var response = new CorrelationQueryResponse(answer, true, events.Take(30).ToArray());
                await RecordInsightAsync(question, response, vectorMatches.Count, events.Count, status, cancellationToken);
                return response;
            }
            catch (Exception ex) when (attempt < 3)
            {
                logger.LogInformation(ex, "Ollama generation attempt {Attempt} failed; retrying.", attempt);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Ollama generation failed; returning local temporal correlation summary.");
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                var fallback = new CorrelationQueryResponse(CreateFallbackAnswer(question, status), false, events.Take(30).ToArray());
                await RecordInsightAsync(question, fallback, vectorMatches.Count, events.Count, status, cancellationToken);
                return fallback;
            }
        }

        var failed = new CorrelationQueryResponse(CreateFallbackAnswer(question, status), false, events.Take(30).ToArray());
        await RecordInsightAsync(question, failed, vectorMatches.Count, events.Count, status, cancellationToken);
        return failed;
    }

    /// <summary>
    /// Begins a streaming Ask. Returns immediately with a deterministic SQL-only answer and a
    /// streamId; the LLM elaboration is generated in the background and broadcast token-by-token
    /// over SignalR ("InsightTokenAppended"), with a final "InsightAnswerCompleted" event when
    /// the grounded answer is verified and persisted.
    /// </summary>
    public async Task<CorrelationStreamResponse> BeginStreamingAskAsync(
        string question,
        IReadOnlyList<CorrelationEvent> events,
        CorrelationStatus status,
        CancellationToken cancellationToken)
    {
        ConfigureClient();
        var streamId = Guid.NewGuid();
        var deterministicAnswer = CreateVerifiedSqlAnswer(events, status);

        var (vectorMatches, pastInsights) = await RetrieveContextAsync(question, cancellationToken);

        // Run the long LLM call on the background scheduler so the HTTP caller returns now.
        _ = Task.Run(() => StreamLlmElaborationAsync(streamId, question, events, vectorMatches, pastInsights, status, deterministicAnswer), CancellationToken.None);

        return new CorrelationStreamResponse(streamId, deterministicAnswer, events.Count, vectorMatches.Count);
    }

    private async Task StreamLlmElaborationAsync(
        Guid streamId,
        string question,
        IReadOnlyList<CorrelationEvent> events,
        IReadOnlyList<CorrelationEvent> vectorMatches,
        IReadOnlyList<InsightRecord> pastInsights,
        CorrelationStatus status,
        string deterministicAnswer)
    {
        using var activity = CorrelationTelemetry.Source.StartActivity("llm.ask.streaming", ActivityKind.Internal);
        activity?.SetTag("stream.id", streamId);

        var prompt = CreatePrompt(question, events, vectorMatches, pastInsights, status, correlationOptions.Value);
        var collected = new StringBuilder();
        var usedLlm = true;
        try
        {
            await foreach (var token in GenerateStreamAsync(prompt, CancellationToken.None))
            {
                collected.Append(token);
                try
                {
                    await hubContext.Clients.All.SendAsync("InsightTokenAppended", new InsightTokenMessage(streamId, token));
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Token broadcast failed.");
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Streaming LLM call failed; falling back to deterministic answer.");
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            usedLlm = false;
        }

        var rawAnswer = collected.Length > 0 ? collected.ToString() : deterministicAnswer;
        var finalAnswer = IsGroundedAnswer(rawAnswer, status) ? rawAnswer : deterministicAnswer;
        if (!IsGroundedAnswer(rawAnswer, status))
        {
            usedLlm = false;
        }

        try
        {
            await hubContext.Clients.All.SendAsync(
                "InsightAnswerCompleted",
                new InsightCompletedMessage(streamId, finalAnswer, usedLlm, vectorMatches.Count, events.Count));
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Completion broadcast failed.");
        }

        await RecordInsightAsync(question, new CorrelationQueryResponse(finalAnswer, usedLlm, events.Take(30).ToArray()), vectorMatches.Count, events.Count, status, CancellationToken.None);
    }

    private async Task<(IReadOnlyList<CorrelationEvent> VectorMatches, IReadOnlyList<InsightRecord> PastInsights)> RetrieveContextAsync(string question, CancellationToken cancellationToken)
    {
        IReadOnlyList<CorrelationEvent> vectorMatches = [];
        IReadOnlyList<InsightRecord> pastInsights = [];
        try
        {
            using var embedSpan = CorrelationTelemetry.Source.StartActivity("llm.embed.question");
            var questionEmbedding = await embeddingClient.EmbedAsync(question, cancellationToken);
            embedSpan?.SetTag("embedding.length", questionEmbedding.Length);

            var entities = EntityExtractor.Extract(question);
            using (var searchSpan = CorrelationTelemetry.Source.StartActivity("vector.search.events"))
            {
                searchSpan?.SetTag("filter.systems", string.Join(",", entities.SourceSystems));
                searchSpan?.SetTag("filter.types", string.Join(",", entities.EventTypes));
                vectorMatches = await vectorStore.SearchAsync(questionEmbedding, correlationOptions.Value.VectorSearchTopK, entities, cancellationToken);
            }

            using (var insightSpan = CorrelationTelemetry.Source.StartActivity("vector.search.insights"))
            {
                pastInsights = await insightVectorStore.SearchAsync(questionEmbedding, correlationOptions.Value.InsightMemoryTopK, cancellationToken);
                insightSpan?.SetTag("matches", pastInsights.Count);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Retrieval failed; proceeding with SQL temporal context only.");
        }

        return (vectorMatches, pastInsights);
    }

    private async Task RecordInsightAsync(
        string question,
        CorrelationQueryResponse response,
        int vectorMatchCount,
        int recentEventCount,
        CorrelationStatus status,
        CancellationToken cancellationToken)
    {
        try
        {
            await insightRepository.AddAsync(new InsightRecord(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                question,
                response.Answer,
                response.UsedLlm,
                vectorMatchCount,
                recentEventCount,
                status.TemporalMatches,
                status.TemporalWindowSeconds), cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to record insight history row.");
        }
    }

    private async Task<string> GenerateAsync(string prompt, bool allowModelPull, CancellationToken cancellationToken)
    {
        using var span = CorrelationTelemetry.Source.StartActivity("llm.generate");
        span?.SetTag("llm.model", options.Value.Model);
        span?.SetTag("prompt.length", prompt.Length);

        var response = await httpClient.PostAsJsonAsync("/api/generate", new
        {
            model = options.Value.Model,
            prompt,
            stream = false,
            keep_alive = "30m",
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

    /// <summary>
    /// Streaming variant: yields incremental token chunks from Ollama's /api/generate (NDJSON when
    /// stream=true). Each line of the response body is a JSON object with a "response" property
    /// containing the next token chunk; the final line carries "done":true.
    /// </summary>
    private async IAsyncEnumerable<string> GenerateStreamAsync(string prompt, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var span = CorrelationTelemetry.Source.StartActivity("llm.generate.stream");
        span?.SetTag("llm.model", options.Value.Model);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/generate")
        {
            Content = JsonContent.Create(new
            {
                model = options.Value.Model,
                prompt,
                stream = true,
                keep_alive = "30m",
                options = new { temperature = 0.1 }
            })
        };

        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            string? token = null;
            try
            {
                using var doc = JsonDocument.Parse(line);
                if (doc.RootElement.TryGetProperty("response", out var part))
                {
                    token = part.GetString();
                }
            }
            catch (JsonException)
            {
                // skip malformed line
            }

            if (!string.IsNullOrEmpty(token))
            {
                yield return token;
            }
        }
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

    private static string CreatePrompt(
        string question,
        IReadOnlyList<CorrelationEvent> events,
        IReadOnlyList<CorrelationEvent> vectorMatches,
        IReadOnlyList<InsightRecord> pastInsights,
        CorrelationStatus status,
        CorrelationOptions correlationOptions)
    {
        var eventLines = events
            .OrderBy(eventItem => eventItem.OccurredAt)
            .TakeLast(correlationOptions.MaxRecentEventsForPrompt)
            .Select(eventItem =>
                $"- {eventItem.OccurredAt:O} | {eventItem.SourceSystem} | type {eventItem.EventType} | {eventItem.Description}");
        var vectorLines = vectorMatches.Count > 0
            ? vectorMatches.Select(eventItem =>
                $"- {eventItem.OccurredAt:O} | {eventItem.SourceSystem} | type {eventItem.EventType} | {eventItem.Description}")
            : ["- No semantically similar events were retrieved from the vector store."];
        var insightLines = pastInsights.Count > 0
            ? pastInsights.Select(insight =>
                $"- ({insight.AskedAt:O}) Q: {Truncate(insight.Question, 120)} | A: {Truncate(insight.Answer, 200)}")
            : ["- No prior related questions were found in the insight memory."];
        var matchedPairLines = CreateMatchedPairLines(events, status.TemporalWindowSeconds);
        var computedFinding = status.TemporalMatches > 0
            ? $"SUPPORTED: the SQL analysis found {status.TemporalMatches} time-window match(es). A time-window match means a System A type 2 event was followed by System B type 9002 within {status.TemporalWindowSeconds} seconds."
            : "NOT YET SUPPORTED: the SQL analysis found zero time-window matches.";

        return $$"""
You are a centralized event correlation analyst. Use only the supplied SQL event dataset context, vector-retrieved similar events, and temporal summary.

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

Recent SQL events (temporal context), ordered by occurrence time:
{{string.Join(Environment.NewLine, eventLines)}}

Vector-retrieved similar events (semantic context from Qdrant):
{{string.Join(Environment.NewLine, vectorLines)}}

Past related questions (from insight memory):
{{string.Join(Environment.NewLine, insightLines)}}

Question: {{question}}

Answer concisely. Explain the deterministic SQL-computed finding in plain language and cite only the time-window evidence supplied above. Do not imply a shared correlation key exists. Do not claim statistical significance, causation, proof, or confidence beyond the supplied time-window counts. Do not invent timestamps, counts, or unmatched-event claims. If you cite timestamps, copy them only from the exact matched event examples.
""";
    }

    private static string Truncate(string value, int maxLength)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maxLength)
        {
            return value ?? string.Empty;
        }
        return value[..maxLength] + "…";
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

public sealed class OllamaEmbeddingClient(IOptions<OllamaOptions> options, ILogger<OllamaEmbeddingClient> logger) : IDisposable
{
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromMinutes(30)
    };
    private readonly SemaphoreSlim modelPullLock = new(1, 1);
    private bool modelPulled;

    public async Task<float[]> EmbedAsync(string text, CancellationToken cancellationToken)
    {
        ConfigureClient();
        return await EmbedInternalAsync(text, allowModelPull: options.Value.AutoPullModel, cancellationToken);
    }

    private async Task<float[]> EmbedInternalAsync(string text, bool allowModelPull, CancellationToken cancellationToken)
    {
        var response = await httpClient.PostAsJsonAsync("/api/embeddings", new
        {
            model = options.Value.EmbeddingModel,
            prompt = text,
            keep_alive = "30m"
        }, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(cancellationToken);
            if (allowModelPull
                && (response.StatusCode == HttpStatusCode.NotFound
                    || error.Contains("not found", StringComparison.OrdinalIgnoreCase)
                    || error.Contains("pull", StringComparison.OrdinalIgnoreCase)))
            {
                await PullEmbeddingModelAsync(cancellationToken);
                return await EmbedInternalAsync(text, allowModelPull: false, cancellationToken);
            }

            throw new InvalidOperationException($"Ollama embedding returned {(int)response.StatusCode}: {error}");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        if (!document.RootElement.TryGetProperty("embedding", out var embeddingElement)
            || embeddingElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Ollama embedding response did not include an 'embedding' array.");
        }

        var values = new float[embeddingElement.GetArrayLength()];
        var index = 0;
        foreach (var component in embeddingElement.EnumerateArray())
        {
            values[index++] = component.GetSingle();
        }

        return values;
    }

    private async Task PullEmbeddingModelAsync(CancellationToken cancellationToken)
    {
        await modelPullLock.WaitAsync(cancellationToken);
        try
        {
            if (modelPulled)
            {
                return;
            }

            logger.LogInformation("Pulling Ollama embedding model {Model}.", options.Value.EmbeddingModel);
            var response = await httpClient.PostAsJsonAsync("/api/pull", new
            {
                name = options.Value.EmbeddingModel,
                stream = false
            }, cancellationToken);
            response.EnsureSuccessStatusCode();
            modelPulled = true;
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
}

public sealed class QdrantEventVectorStore(IOptions<QdrantOptions> options, ILogger<QdrantEventVectorStore> logger) : IDisposable
{
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromMinutes(2)
    };
    private readonly SemaphoreSlim initLock = new(1, 1);
    private bool initialized;

    public async Task UpsertAsync(CorrelationEvent eventItem, float[] embedding, CancellationToken cancellationToken)
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
                    vector = embedding,
                    payload = new
                    {
                        eventItem.SourceSystem,
                        eventItem.EventType,
                        eventItem.Name,
                        eventItem.Description,
                        OccurredAt = eventItem.OccurredAt
                    }
                }
            }
        };

        var response = await httpClient.PutAsJsonAsync($"/collections/{options.Value.Collection}/points?wait=true", payload, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<CorrelationEvent>> SearchAsync(float[] embedding, int topK, CancellationToken cancellationToken) =>
        await SearchAsync(embedding, topK, entities: null, cancellationToken);

    /// <summary>
    /// Hybrid retrieval: combines vector similarity with a Qdrant payload filter built from
    /// regex-extracted entities (source systems, event-type numbers). When entities is null or
    /// empty, behaves as a pure vector search.
    /// </summary>
    public async Task<IReadOnlyList<CorrelationEvent>> SearchAsync(float[] embedding, int topK, QuestionEntities? entities, CancellationToken cancellationToken)
    {
        ConfigureClient();
        await EnsureCollectionAsync(cancellationToken);

        object? filter = null;
        if (entities is not null && entities.HasFilters)
        {
            var must = new List<object>();
            foreach (var system in entities.SourceSystems)
            {
                must.Add(new { key = "sourceSystem", match = new { value = system } });
            }
            foreach (var type in entities.EventTypes)
            {
                must.Add(new { key = "eventType", match = new { value = type } });
            }
            if (must.Count > 0)
            {
                filter = new { must };
            }
        }

        var response = await httpClient.PostAsJsonAsync($"/collections/{options.Value.Collection}/points/search", new
        {
            vector = embedding,
            limit = topK,
            filter,
            with_payload = true,
            with_vector = false
        }, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        if (!document.RootElement.TryGetProperty("result", out var result) || result.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var matches = new List<CorrelationEvent>();
        foreach (var hit in result.EnumerateArray())
        {
            var parsed = TryReadEvent(hit);
            if (parsed is not null)
            {
                matches.Add(parsed);
            }
        }

        return matches;
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

        await initLock.WaitAsync(cancellationToken);
        try
        {
            if (initialized)
            {
                return;
            }

            var existing = await httpClient.GetAsync($"/collections/{options.Value.Collection}", cancellationToken);
            if (existing.IsSuccessStatusCode)
            {
                initialized = true;
                return;
            }

            var createResponse = await httpClient.PutAsJsonAsync($"/collections/{options.Value.Collection}", new
            {
                vectors = new
                {
                    size = options.Value.VectorSize,
                    distance = "Cosine"
                }
            }, cancellationToken);
            createResponse.EnsureSuccessStatusCode();
            logger.LogInformation("Qdrant collection {Collection} created.", options.Value.Collection);
            initialized = true;
        }
        finally
        {
            initLock.Release();
        }
    }

    private void ConfigureClient()
    {
        if (httpClient.BaseAddress is null)
        {
            httpClient.BaseAddress = new Uri(options.Value.Endpoint.TrimEnd('/'));
        }
    }

    private static CorrelationEvent? TryReadEvent(JsonElement hit)
    {
        try
        {
            if (!hit.TryGetProperty("payload", out var payload) || payload.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var idText = hit.TryGetProperty("id", out var idElement)
                ? idElement.ValueKind == JsonValueKind.String ? idElement.GetString() : idElement.GetRawText()
                : null;
            if (!Guid.TryParse(idText, out var id))
            {
                return null;
            }

            return new CorrelationEvent(
                id,
                GetString(payload, "sourceSystem", "SourceSystem") ?? "Unknown",
                GetInt32(payload, "eventType", "EventType"),
                GetString(payload, "name", "Name") ?? "Unknown event",
                GetString(payload, "description", "Description") ?? string.Empty,
                GetDateTimeOffset(payload, "occurredAt", "OccurredAt"));
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
            if (payload.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String)
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
            if (payload.TryGetProperty(name, out var value) && value.TryGetInt32(out var number))
            {
                return number;
            }
        }
        return 0;
    }

    private static DateTimeOffset GetDateTimeOffset(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value)
                && value.ValueKind == JsonValueKind.String
                && DateTimeOffset.TryParse(value.GetString(), out var parsed))
            {
                return parsed;
            }
        }
        return DateTimeOffset.MinValue;
    }

    public void Dispose()
    {
        httpClient.Dispose();
        initLock.Dispose();
    }
}

public sealed class InsightRepository(
    IDbContextFactory<CorrelationDbContext> dbContextFactory,
    OllamaEmbeddingClient embeddingClient,
    QdrantInsightVectorStore insightVectorStore,
    ILogger<InsightRepository> logger)
{
    public async Task AddAsync(InsightRecord record, CancellationToken cancellationToken)
    {
        try
        {
            await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
            dbContext.Insights.Add(new InsightRecordEntity
            {
                Id = record.Id,
                AskedAt = record.AskedAt,
                Question = record.Question,
                Answer = record.Answer,
                UsedLlm = record.UsedLlm,
                VectorMatchCount = record.VectorMatchCount,
                RecentEventCount = record.RecentEventCount,
                TemporalMatches = record.TemporalMatches,
                TemporalWindowSeconds = record.TemporalWindowSeconds
            });
            await dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist insight record.");
        }

        // Insight memory: best-effort embed + upsert into the dedicated Qdrant collection so that
        // future Ask calls can surface semantically related past Q/A pairs.
        try
        {
            var embedding = await embeddingClient.EmbedAsync(record.Question + " | " + record.Answer, cancellationToken);
            await insightVectorStore.UpsertAsync(record, embedding, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Insight vector upsert failed (non-fatal).");
        }
    }

    public async Task<IReadOnlyList<InsightRecord>> GetRecentAsync(int take, CancellationToken cancellationToken)
    {
        try
        {
            await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
            var rows = await dbContext.Insights
                .AsNoTracking()
                .OrderByDescending(insight => insight.AskedAt)
                .Take(take)
                .ToArrayAsync(cancellationToken);

            return rows.Select(row => new InsightRecord(
                row.Id,
                row.AskedAt,
                row.Question,
                row.Answer,
                row.UsedLlm,
                row.VectorMatchCount,
                row.RecentEventCount,
                row.TemporalMatches,
                row.TemporalWindowSeconds)).ToArray();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to read insight history.");
            return [];
        }
    }
}
