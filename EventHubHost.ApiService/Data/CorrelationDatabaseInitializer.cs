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
