using EventHubHost.ApiService;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddProblemDetails();
builder.Services.Configure<QdrantOptions>(builder.Configuration.GetSection("Qdrant"));
builder.Services.Configure<OllamaOptions>(builder.Configuration.GetSection("Ollama"));
builder.Services.AddSingleton<EventRepository>();
builder.Services.AddHttpClient<QdrantEventVectorStore>();
builder.Services.AddHttpClient<OllamaCorrelationClient>();
builder.Services.AddHostedService<EventSimulationWorker>();

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

app.MapDefaultEndpoints();

app.Run();
