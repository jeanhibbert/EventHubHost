using EventHubHost.ApiService;
using EventHubHost.ApiService.Data;
using EventHubHost.ApiService.Hubs;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add the EventHubHost ActivitySource to the OpenTelemetry tracer so spans flow to the Aspire dashboard.
builder.Services.ConfigureOpenTelemetryTracerProvider(tracing => tracing.AddSource(CorrelationTelemetry.SourceName));

// Add services to the container.
builder.Services.AddProblemDetails();
builder.Services.Configure<OllamaOptions>(builder.Configuration.GetSection("Ollama"));
builder.Services.Configure<QdrantOptions>(builder.Configuration.GetSection("Qdrant"));
builder.Services.Configure<CorrelationOptions>(builder.Configuration.GetSection("Correlation"));
builder.Services.Configure<AnomalyOptions>(builder.Configuration.GetSection("Anomaly"));
builder.Services.AddPooledDbContextFactory<CorrelationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("eventdb")));
builder.Services.AddSingleton<OllamaEmbeddingClient>();
builder.Services.AddSingleton<QdrantEventVectorStore>();
builder.Services.AddSingleton<QdrantInsightVectorStore>();
builder.Services.AddSingleton<InsightRepository>();
builder.Services.AddSingleton<AnomalyRepository>();
builder.Services.AddSingleton<EventRepository>();
builder.Services.AddSingleton<OllamaCorrelationClient>();
builder.Services.AddSignalR();
builder.Services.AddHostedService<CorrelationDatabaseInitializer>();
builder.Services.AddHostedService<EventSimulationWorker>();
builder.Services.AddHostedService<AnomalyDetectionWorker>();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGet("/events/recent", async (EventRepository repository, int take, CancellationToken cancellationToken) =>
    await repository.GetRecentAsync(take <= 0 ? 40 : take, cancellationToken))
    .WithName("GetRecentEvents");

app.MapGet("/correlations/status", async (EventRepository repository, CancellationToken cancellationToken) =>
    await repository.GetStatusAsync(cancellationToken))
    .WithName("GetCorrelationStatus");

app.MapPost("/correlations/query", async (CorrelationQueryRequest request, EventRepository repository, OllamaCorrelationClient llm, CancellationToken cancellationToken) =>
{
    var events = await repository.GetRecentAsync(80, cancellationToken);
    var status = await repository.GetStatusAsync(cancellationToken);
    return await llm.AskAsync(request.Question, events, status, cancellationToken);
})
.WithName("QueryCorrelationInsights");

// Streaming endpoint: returns the deterministic SQL-derived answer immediately and a streamId the
// Blazor UI subscribes to over SignalR for token-by-token LLM elaboration.
app.MapPost("/correlations/query/stream", async (CorrelationQueryRequest request, EventRepository repository, OllamaCorrelationClient llm, CancellationToken cancellationToken) =>
{
    var events = await repository.GetRecentAsync(80, cancellationToken);
    var status = await repository.GetStatusAsync(cancellationToken);
    return await llm.BeginStreamingAskAsync(request.Question, events, status, cancellationToken);
})
.WithName("QueryCorrelationInsightsStreaming");

app.MapGet("/correlations/insights", async (InsightRepository insights, int? take, CancellationToken cancellationToken) =>
    await insights.GetRecentAsync(take is > 0 ? take.Value : 25, cancellationToken))
    .WithName("GetRecentInsights");

app.MapGet("/anomalies/recent", async (AnomalyRepository anomalies, int? take, CancellationToken cancellationToken) =>
    await anomalies.GetRecentAsync(take is > 0 ? take.Value : 25, cancellationToken))
    .WithName("GetRecentAnomalies");

app.MapPost("/scenarios/type2", async (EventRepository repository, CancellationToken cancellationToken) =>
{
    var events = CorrelationEventFactory.CreateEnforcedScenario();
    foreach (var eventItem in events)
    {
        await repository.AddAsync(eventItem, cancellationToken);
    }

    return Results.Ok(events);
})
.WithName("TriggerType2CorrelationScenario");

app.MapHub<EventIngestionHub>("/hubs/events");

app.MapDefaultEndpoints();

app.Run();
