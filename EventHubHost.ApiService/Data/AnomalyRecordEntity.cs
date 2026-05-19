namespace EventHubHost.ApiService.Data;

public sealed class AnomalyRecordEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset DetectedAt { get; set; }
    public string Severity { get; set; } = string.Empty;
    public string SourceSystem { get; set; } = string.Empty;
    public int? EventType { get; set; }
    public double ObservedRate { get; set; }
    public double ExpectedRate { get; set; }
    public string Explanation { get; set; } = string.Empty;
    public bool UsedLlm { get; set; }
}
