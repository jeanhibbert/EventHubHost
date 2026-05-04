namespace EventHubHost.ApiService.Data;

public sealed class InsightRecordEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset AskedAt { get; set; }
    public string Question { get; set; } = string.Empty;
    public string Answer { get; set; } = string.Empty;
    public bool UsedLlm { get; set; }
    public int VectorMatchCount { get; set; }
    public int RecentEventCount { get; set; }
    public int TemporalMatches { get; set; }
    public int TemporalWindowSeconds { get; set; }
}
