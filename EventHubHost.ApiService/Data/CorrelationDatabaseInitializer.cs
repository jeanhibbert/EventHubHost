using Microsoft.EntityFrameworkCore;

namespace EventHubHost.ApiService.Data;

public sealed class CorrelationDatabaseInitializer(
    IDbContextFactory<CorrelationDbContext> dbContextFactory,
    ILogger<CorrelationDatabaseInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        const int maxAttempts = 30;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await using var dbContext = await dbContextFactory.CreateDbContextAsync(cancellationToken);
                await dbContext.Database.EnsureCreatedAsync(cancellationToken);
                // EnsureCreatedAsync does not migrate schemas; when the SQL volume already exists from
                // a previous run the new Insights table will be missing. Create it idempotently.
                await dbContext.Database.ExecuteSqlRawAsync(
                    """
                    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Events_OccurredAt' AND object_id = OBJECT_ID(N'[Events]'))
                    BEGIN
                        CREATE INDEX [IX_Events_OccurredAt] ON [Events] ([OccurredAt]);
                    END

                    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Events_SourceSystem_EventType_OccurredAt' AND object_id = OBJECT_ID(N'[Events]'))
                    BEGIN
                        CREATE INDEX [IX_Events_SourceSystem_EventType_OccurredAt] ON [Events] ([SourceSystem], [EventType], [OccurredAt]);
                    END

                    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Insights')
                    BEGIN
                        CREATE TABLE [Insights] (
                            [Id] uniqueidentifier NOT NULL CONSTRAINT [PK_Insights] PRIMARY KEY,
                            [AskedAt] datetimeoffset NOT NULL,
                            [Question] nvarchar(2000) NOT NULL,
                            [Answer] nvarchar(max) NOT NULL,
                            [UsedLlm] bit NOT NULL,
                            [VectorMatchCount] int NOT NULL,
                            [RecentEventCount] int NOT NULL,
                            [TemporalMatches] int NOT NULL,
                            [TemporalWindowSeconds] int NOT NULL
                        );
                        CREATE INDEX [IX_Insights_AskedAt] ON [Insights] ([AskedAt]);
                    END

                    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Anomalies')
                    BEGIN
                        CREATE TABLE [Anomalies] (
                            [Id] uniqueidentifier NOT NULL CONSTRAINT [PK_Anomalies] PRIMARY KEY,
                            [DetectedAt] datetimeoffset NOT NULL,
                            [Severity] nvarchar(32) NOT NULL,
                            [SourceSystem] nvarchar(64) NOT NULL,
                            [EventType] int NULL,
                            [ObservedRate] float NOT NULL,
                            [ExpectedRate] float NOT NULL,
                            [Explanation] nvarchar(max) NOT NULL,
                            [UsedLlm] bit NOT NULL
                        );
                        CREATE INDEX [IX_Anomalies_DetectedAt] ON [Anomalies] ([DetectedAt]);
                        CREATE INDEX [IX_Anomalies_Severity_DetectedAt] ON [Anomalies] ([Severity], [DetectedAt]);
                    END
                    """,
                    cancellationToken);
                logger.LogInformation("Correlation SQL database is ready.");
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogInformation(ex, "SQL database is not ready yet. Attempt {Attempt} of {MaxAttempts}.", attempt, maxAttempts);
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
