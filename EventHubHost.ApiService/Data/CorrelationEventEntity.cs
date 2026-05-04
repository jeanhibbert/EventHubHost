namespace EventHubHost.ApiService.Data;

public sealed class CorrelationEventEntity
{
    public Guid Id { get; set; }
    public string SourceSystem { get; set; } = string.Empty;
    public int EventType { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public DateTimeOffset OccurredAt { get; set; }
}
