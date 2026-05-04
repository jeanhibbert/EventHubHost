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
