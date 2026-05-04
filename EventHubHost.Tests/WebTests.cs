using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR.Client;
using System.Net.Http.Json;

namespace EventHubHost.Tests;

public class WebTests
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ContainerStartupTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan LlmQueryTimeout = TimeSpan.FromMinutes(12);

    [Fact]
    public async Task GetWebResourceRootReturnsOkStatusCode()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.EventHubHost_AppHost>(cancellationToken);
        appHost.Services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Debug);
            // Override the logging filters from the app's configuration
            logging.AddFilter(appHost.Environment.ApplicationName, LogLevel.Debug);
            logging.AddFilter("Aspire.", LogLevel.Debug);
            // To output logs to the xUnit.net ITestOutputHelper, consider adding a package from https://www.nuget.org/packages?q=xunit+logging
        });
        appHost.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
            clientBuilder.AddStandardResilienceHandler();
        });

        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);

        // Act
        var httpClient = app.CreateHttpClient("webfrontend");
        await app.ResourceNotifications.WaitForResourceHealthyAsync("webfrontend", cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);
        var response = await httpClient.GetAsync("/", cancellationToken);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task CorrelationScenarioPersistsToSqlAndAnswersWithTemporalInsight()
    {
        // Arrange
        var cancellationToken = TestContext.Current.CancellationToken;

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.EventHubHost_AppHost>(cancellationToken);
        appHost.Services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Debug);
            logging.AddFilter(appHost.Environment.ApplicationName, LogLevel.Debug);
            logging.AddFilter("Aspire.", LogLevel.Debug);
        });

        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);

        var apiClient = app.CreateHttpClient("apiservice");
        apiClient.Timeout = LlmQueryTimeout;
        await app.ResourceNotifications.WaitForResourceHealthyAsync("apiservice", cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);

        var receivedEvents = new List<CorrelationEvent>();
        var eventSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var hubConnection = new HubConnectionBuilder()
            .WithUrl(new Uri(apiClient.BaseAddress!, "/hubs/events"))
            .Build();

        hubConnection.On<CorrelationEvent>("EventIngested", eventItem =>
        {
            receivedEvents.Add(eventItem);
            if (receivedEvents.Count >= 2)
            {
                eventSignal.TrySetResult();
            }
        });

        await hubConnection.StartAsync(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        // Act
        var scenarioResponse = await apiClient.PostAsync("/scenarios/type2", content: null, cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        scenarioResponse.EnsureSuccessStatusCode();
        await eventSignal.Task.WaitAsync(DefaultTimeout, cancellationToken);

        var status = await apiClient.GetFromJsonAsync<CorrelationStatus>("/correlations/status", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        var queryResponse = await apiClient.PostAsJsonAsync(
            "/correlations/query",
            new CorrelationQueryRequest("Based on the SQL event history, does System B type 9002 tend to happen within the time window after System A type 2?"),
            cancellationToken).WaitAsync(LlmQueryTimeout, cancellationToken);
        queryResponse.EnsureSuccessStatusCode();

        var insight = await queryResponse.Content.ReadFromJsonAsync<CorrelationQueryResponse>(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        // Assert
        Assert.NotNull(status);
        Assert.True(status.SqlStoreAvailable);
        Assert.True(status.SystemAType2Events >= 1);
        Assert.True(status.SystemBUniqueEvents >= 1);
        Assert.True(status.TemporalMatches >= 1);
        Assert.Contains(status.RecentEvents, eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == 2);
        Assert.Contains(status.RecentEvents, eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == 9002);
        Assert.Contains(receivedEvents, eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == 2);
        Assert.Contains(receivedEvents, eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == 9002);

        Assert.NotNull(insight);
        Assert.True(insight.UsedLlm);
        Assert.False(string.IsNullOrWhiteSpace(insight.Answer));
        Assert.True(
            insight.Answer.Contains("temporal", StringComparison.OrdinalIgnoreCase)
            || insight.Answer.Contains("time", StringComparison.OrdinalIgnoreCase)
            || insight.Answer.Contains("window", StringComparison.OrdinalIgnoreCase)
            || insight.Answer.Contains("after", StringComparison.OrdinalIgnoreCase));
    }

    private sealed record CorrelationQueryRequest(string Question);

    private sealed record CorrelationQueryResponse(
        string Answer,
        bool UsedLlm,
        IReadOnlyList<CorrelationEvent> ContextEvents);

    private sealed record CorrelationStatus(
        int TotalEvents,
        int SystemAEvents,
        int SystemBEvents,
        int SystemAType2Events,
        int SystemBUniqueEvents,
        int TemporalMatches,
        int TemporalWindowSeconds,
        bool SqlStoreAvailable,
        IReadOnlyList<CorrelationEvent> RecentEvents);

    private sealed record CorrelationEvent(
        Guid Id,
        string SourceSystem,
        int EventType,
        string Name,
        string Description,
        DateTimeOffset OccurredAt);
}
