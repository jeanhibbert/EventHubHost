using Microsoft.Extensions.Logging;
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

        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        // Act
        var httpClient = app.CreateHttpClient("webfrontend");
        await app.ResourceNotifications.WaitForResourceHealthyAsync("webfrontend", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        var response = await httpClient.GetAsync("/", cancellationToken);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task CorrelationScenarioPersistsToVectorStoreAndAnswersQuestion()
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

        // Act
        var scenarioResponse = await apiClient.PostAsync("/scenarios/type2", content: null, cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        scenarioResponse.EnsureSuccessStatusCode();

        var status = await apiClient.GetFromJsonAsync<CorrelationStatus>("/correlations/status", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        var queryResponse = await apiClient.PostAsJsonAsync(
            "/correlations/query",
            new CorrelationQueryRequest("Has System A event type 2 caused a unique event in System B?"),
            cancellationToken).WaitAsync(LlmQueryTimeout, cancellationToken);
        queryResponse.EnsureSuccessStatusCode();

        var insight = await queryResponse.Content.ReadFromJsonAsync<CorrelationQueryResponse>(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        // Assert
        Assert.NotNull(status);
        Assert.True(status.VectorStoreAvailable);
        Assert.True(status.SystemAType2Events >= 1);
        Assert.True(status.SystemBUniqueEvents >= 1);
        Assert.True(status.MatchedCorrelationKeys >= 1);
        Assert.Contains(status.RecentEvents, eventItem => eventItem.SourceSystem == "System A" && eventItem.EventType == 2);
        Assert.Contains(status.RecentEvents, eventItem => eventItem.SourceSystem == "System B" && eventItem.EventType == 9002);

        Assert.NotNull(insight);
        Assert.True(insight.UsedLlm);
        Assert.False(string.IsNullOrWhiteSpace(insight.Answer));
        Assert.Contains("correlation", insight.Answer, StringComparison.OrdinalIgnoreCase);
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
        int MatchedCorrelationKeys,
        bool VectorStoreAvailable,
        IReadOnlyList<CorrelationEvent> RecentEvents);

    private sealed record CorrelationEvent(
        Guid Id,
        string SourceSystem,
        int EventType,
        string Name,
        string Description,
        DateTimeOffset OccurredAt,
        string CorrelationKey);
}
