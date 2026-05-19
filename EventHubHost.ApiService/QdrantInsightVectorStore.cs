using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace EventHubHost.ApiService;

/// <summary>
/// Insight memory: a dedicated Qdrant collection that stores embeddings of prior Question + Answer
/// pairs. During Ask, we vector-search this collection and feed the top-K matches into the prompt
/// under "Past related questions" so the LLM can build on prior reasoning.
/// </summary>
public sealed class QdrantInsightVectorStore(IOptions<QdrantOptions> options, ILogger<QdrantInsightVectorStore> logger) : IDisposable
{
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromMinutes(2)
    };
    private readonly SemaphoreSlim initLock = new(1, 1);
    private bool initialized;

    public async Task UpsertAsync(InsightRecord record, float[] embedding, CancellationToken cancellationToken)
    {
        ConfigureClient();
        await EnsureCollectionAsync(cancellationToken);

        var payload = new
        {
            points = new[]
            {
                new
                {
                    id = record.Id,
                    vector = embedding,
                    payload = new
                    {
                        record.Question,
                        record.Answer,
                        record.AskedAt,
                        record.UsedLlm,
                        record.VectorMatchCount,
                        record.RecentEventCount,
                        record.TemporalMatches,
                        record.TemporalWindowSeconds
                    }
                }
            }
        };

        var response = await httpClient.PutAsJsonAsync($"/collections/{options.Value.InsightCollection}/points?wait=true", payload, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<InsightRecord>> SearchAsync(float[] embedding, int topK, CancellationToken cancellationToken)
    {
        ConfigureClient();
        await EnsureCollectionAsync(cancellationToken);

        try
        {
            var response = await httpClient.PostAsJsonAsync($"/collections/{options.Value.InsightCollection}/points/search", new
            {
                vector = embedding,
                limit = topK,
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

            var matches = new List<InsightRecord>();
            foreach (var hit in result.EnumerateArray())
            {
                if (!hit.TryGetProperty("payload", out var p) || p.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }
                var idText = hit.TryGetProperty("id", out var idEl)
                    ? (idEl.ValueKind == JsonValueKind.String ? idEl.GetString() : idEl.GetRawText())
                    : null;
                if (!Guid.TryParse(idText, out var id))
                {
                    continue;
                }

                matches.Add(new InsightRecord(
                    id,
                    GetDateTimeOffset(p, "askedAt", "AskedAt"),
                    GetString(p, "question", "Question") ?? string.Empty,
                    GetString(p, "answer", "Answer") ?? string.Empty,
                    GetBool(p, "usedLlm", "UsedLlm"),
                    GetInt(p, "vectorMatchCount", "VectorMatchCount"),
                    GetInt(p, "recentEventCount", "RecentEventCount"),
                    GetInt(p, "temporalMatches", "TemporalMatches"),
                    GetInt(p, "temporalWindowSeconds", "TemporalWindowSeconds")));
            }
            return matches;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Insight vector search failed.");
            return [];
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
            var existing = await httpClient.GetAsync($"/collections/{options.Value.InsightCollection}", cancellationToken);
            if (!existing.IsSuccessStatusCode)
            {
                var createResponse = await httpClient.PutAsJsonAsync($"/collections/{options.Value.InsightCollection}", new
                {
                    vectors = new
                    {
                        size = options.Value.VectorSize,
                        distance = "Cosine"
                    }
                }, cancellationToken);
                createResponse.EnsureSuccessStatusCode();
                logger.LogInformation("Qdrant insight collection {Collection} created.", options.Value.InsightCollection);
            }
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

    private static int GetInt(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value) && value.TryGetInt32(out var n))
            {
                return n;
            }
        }
        return 0;
    }

    private static bool GetBool(JsonElement payload, params string[] names)
    {
        foreach (var name in names)
        {
            if (payload.TryGetProperty(name, out var value) && (value.ValueKind == JsonValueKind.True || value.ValueKind == JsonValueKind.False))
            {
                return value.GetBoolean();
            }
        }
        return false;
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
