using EventHubHost.ApiService;
using Microsoft.Extensions.Logging;
using System.Net.Http.Json;

namespace EventHubHost.Tests;

/// <summary>
/// Golden-question evaluation harness. For each parameterized question, asserts that the
/// grounded-answer guard's evidence sentence is present and that the LLM has not regressed into
/// using forbidden words like "statistically significant" or "correlation key". This pins prompt
/// behaviour so future prompt edits cannot silently break the SQL-grounding contract.
///
/// Live test — runs the same Aspire orchestration as <see cref="WebTests"/> and is therefore
/// gated under the "Live" trait so CI can opt out by category.
/// </summary>
public class GoldenQuestionTests
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ContainerStartupTimeout = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan LlmQueryTimeout = TimeSpan.FromMinutes(30);

    public static TheoryData<string> GoldenQuestions => new()
    {
        "Based on the SQL event history, does System B type 9002 tend to happen within the time window after System A type 2?",
        "Is there evidence that System A type 2 events are correlated in time with System B unique events?",
        "Show whether System B follows System A within the configured window."
    };

    private static readonly string[] ForbiddenTerms =
        ["statistically", "probability", "proves", "proof", "confidence", "correlation key"];

    [Fact]
    public void GroundingGuardAcceptsCorrectParaphrase()
    {
        var status = CreateStatus(temporalMatches: 4, temporalWindowSeconds: 15);

        var answer = "Finding: the SQL data supports the temporal hypothesis. Evidence: 4 matches show System A type 2 is followed after by System B type 9002 within a 15 second window. Limit: timing evidence only.";

        Assert.True(OllamaCorrelationClient.IsGroundedAnswer(answer, status));
    }

    [Fact]
    public void GroundingGuardRejectsForbiddenClaims()
    {
        var status = CreateStatus(temporalMatches: 4, temporalWindowSeconds: 15);

        var answer = "Evidence: 4 System A type 2 event(s) were followed by System B type 9002 within 15 seconds. This proves the systems are linked.";

        Assert.False(OllamaCorrelationClient.IsGroundedAnswer(answer, status));
    }

    [Fact]
    public void GroundingGuardAcceptsZeroMatchParaphrase()
    {
        var status = CreateStatus(temporalMatches: 0, temporalWindowSeconds: 15);

        var answer = "Finding: the SQL data does not yet support the hypothesis. Evidence: zero matches show System A type 2 followed by System B type 9002 within the 15 second window. Limit: timing evidence only.";

        Assert.True(OllamaCorrelationClient.IsGroundedAnswer(answer, status));
    }

    [Theory]
    [Trait("Category", "Live")]
    [MemberData(nameof(GoldenQuestions))]
    public async Task GoldenQuestionProducesGroundedEvidenceSentence(string question)
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.EventHubHost_AppHost>(cancellationToken);
        appHost.Services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Information);
            logging.AddFilter("Aspire.", LogLevel.Warning);
        });
        appHost.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
            clientBuilder.AddStandardResilienceHandler(options =>
            {
                options.AttemptTimeout.Timeout = LlmQueryTimeout;
                options.TotalRequestTimeout.Timeout = LlmQueryTimeout;
                options.CircuitBreaker.SamplingDuration = LlmQueryTimeout * 2;
            });
        });

        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);

        var apiClient = app.CreateHttpClient("apiservice");
        apiClient.Timeout = LlmQueryTimeout;
        await app.ResourceNotifications.WaitForResourceHealthyAsync("apiservice", cancellationToken).WaitAsync(ContainerStartupTimeout, cancellationToken);

        // Guarantee a grounded matched-event row exists so the deterministic evidence is non-trivial.
        var scenarioResponse = await apiClient.PostAsync("/scenarios/type2", content: null, cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        scenarioResponse.EnsureSuccessStatusCode();

        var status = await apiClient.GetFromJsonAsync<GoldenStatus>("/correlations/status", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        Assert.NotNull(status);

        var queryResponse = await apiClient.PostAsJsonAsync(
            "/correlations/query",
            new GoldenRequest(question),
            cancellationToken).WaitAsync(LlmQueryTimeout, cancellationToken);
        queryResponse.EnsureSuccessStatusCode();
        var insight = await queryResponse.Content.ReadFromJsonAsync<GoldenResponse>(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        Assert.NotNull(insight);
        Assert.False(string.IsNullOrWhiteSpace(insight.Answer));

        // The grounded-answer guard requires this exact evidence sentence (or the verified-SQL
        // fallback, which contains the same sentence). Either way it must appear in the answer.
        var requiredEvidence = $"Evidence: {status.TemporalMatches} System A type 2 event(s) were followed by System B type 9002 within {status.TemporalWindowSeconds} seconds";
        Assert.Contains(requiredEvidence, insight.Answer, StringComparison.OrdinalIgnoreCase);

        foreach (var term in ForbiddenTerms)
        {
            Assert.DoesNotContain(term, insight.Answer, StringComparison.OrdinalIgnoreCase);
        }
    }

    private sealed record GoldenRequest(string Question);
    private sealed record GoldenResponse(string Answer, bool UsedLlm);
    private sealed record GoldenStatus(int TemporalMatches, int TemporalWindowSeconds);

    private static CorrelationStatus CreateStatus(int temporalMatches, int temporalWindowSeconds) =>
        new(
            TotalEvents: 0,
            SystemAEvents: 0,
            SystemBEvents: 0,
            SystemAType2Events: 0,
            SystemBUniqueEvents: 0,
            TemporalMatches: temporalMatches,
            TemporalWindowSeconds: temporalWindowSeconds,
            SqlStoreAvailable: true,
            VectorStoreAvailable: true,
            EmbeddingModel: "nomic-embed-text",
            RecentEvents: []);
}
