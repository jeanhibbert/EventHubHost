using Microsoft.EntityFrameworkCore;

namespace EventHubHost.ApiService.Data;

public sealed class CorrelationDbContext(DbContextOptions<CorrelationDbContext> options) : DbContext(options)
{
    public DbSet<CorrelationEventEntity> Events => Set<CorrelationEventEntity>();

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
    }
}
