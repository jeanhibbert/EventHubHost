using Microsoft.EntityFrameworkCore;

namespace EventHubHost.ApiService.Data;

public sealed class CorrelationDbContext(DbContextOptions<CorrelationDbContext> options) : DbContext(options)
{
    public DbSet<CorrelationEventEntity> Events => Set<CorrelationEventEntity>();
    public DbSet<InsightRecordEntity> Insights => Set<InsightRecordEntity>();
    public DbSet<AnomalyRecordEntity> Anomalies => Set<AnomalyRecordEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var eventEntity = modelBuilder.Entity<CorrelationEventEntity>();
        eventEntity.ToTable("Events");
        eventEntity.HasKey(eventItem => eventItem.Id);
        eventEntity.Property(eventItem => eventItem.SourceSystem).HasMaxLength(64).IsRequired();
        eventEntity.Property(eventItem => eventItem.Name).HasMaxLength(128).IsRequired();
        eventEntity.Property(eventItem => eventItem.Description).HasMaxLength(512).IsRequired();
        eventEntity.HasIndex(eventItem => eventItem.OccurredAt);
        eventEntity.HasIndex(eventItem => new { eventItem.SourceSystem, eventItem.EventType, eventItem.OccurredAt });

        var insightEntity = modelBuilder.Entity<InsightRecordEntity>();
        insightEntity.ToTable("Insights");
        insightEntity.HasKey(insight => insight.Id);
        insightEntity.Property(insight => insight.Question).HasMaxLength(2000).IsRequired();
        insightEntity.Property(insight => insight.Answer).IsRequired();
        insightEntity.HasIndex(insight => insight.AskedAt);

        var anomalyEntity = modelBuilder.Entity<AnomalyRecordEntity>();
        anomalyEntity.ToTable("Anomalies");
        anomalyEntity.HasKey(anomaly => anomaly.Id);
        anomalyEntity.Property(anomaly => anomaly.Severity).HasMaxLength(32).IsRequired();
        anomalyEntity.Property(anomaly => anomaly.SourceSystem).HasMaxLength(64).IsRequired();
        anomalyEntity.Property(anomaly => anomaly.Explanation).IsRequired();
        anomalyEntity.HasIndex(anomaly => anomaly.DetectedAt);
        anomalyEntity.HasIndex(anomaly => new { anomaly.Severity, anomaly.DetectedAt });
    }
}
