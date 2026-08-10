using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SqlCdc;

/// <summary>
/// Runs a <see cref="SqlCdcWatcher"/> for the lifetime of the host and dispatches
/// its events to the registered <see cref="ICdcChangeHandler"/> implementations.
/// </summary>
internal sealed class SqlCdcHostedService : BackgroundService
{
    private readonly SqlCdcWatcher _watcher;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<SqlCdcHostedService> _logger;

    public SqlCdcHostedService(
        SqlCdcWatcher watcher,
        IServiceScopeFactory scopeFactory,
        ILogger<SqlCdcHostedService> logger)
    {
        _watcher = watcher;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Resolving capture instances hits the database. Yielding keeps that off the host startup
        // path, so a database that is briefly unavailable delays CDC instead of failing the host.
        await Task.Yield();

        try
        {
            await _watcher.StartAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "The CDC watcher failed to start. No change events will be delivered.");
            return;
        }

        if (!HasHandlers())
        {
            // No handlers registered: the application reads SqlCdcWatcher.Changes itself.
            return;
        }

        try
        {
            await foreach (var change in _watcher.Changes.WithCancellation(stoppingToken))
            {
                await DispatchAsync(change, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await _watcher.StopAsync();
    }

    private bool HasHandlers()
    {
        using var scope = _scopeFactory.CreateScope();
        return scope.ServiceProvider.GetServices<ICdcChangeHandler>().Any();
    }

    private async Task DispatchAsync(CdcChange change, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        foreach (var handler in scope.ServiceProvider.GetServices<ICdcChangeHandler>())
        {
            try
            {
                await handler.HandleAsync(change, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // The watermark advances when an event reaches the channel, not when it is handled,
                // so a throwing handler drops this event. Handlers own their own retry policy.
                _logger.LogError(
                    ex,
                    "Handler {Handler} failed for change {ChangeKey} on {TableName}; the event is dropped",
                    handler.GetType().Name, change.Key, change.TableName);
            }
        }
    }
}
