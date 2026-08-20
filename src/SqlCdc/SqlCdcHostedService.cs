using System.Diagnostics;
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
    /// <summary>Ceiling for the exponential backoff between handler attempts.</summary>
    private static readonly TimeSpan MaxHandlerBackoff = TimeSpan.FromMinutes(1);

    private readonly SqlCdcWatcher _watcher;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<SqlCdcHostedService> _logger;
    private readonly ICdcDeadLetterSink? _deadLetterSink;

    public SqlCdcHostedService(
        SqlCdcWatcher watcher,
        IServiceScopeFactory scopeFactory,
        ILogger<SqlCdcHostedService> logger,
        ICdcDeadLetterSink? deadLetterSink = null)
    {
        _watcher = watcher;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _deadLetterSink = deadLetterSink;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Resolving capture instances hits the database. Yielding keeps that off the host startup
        // path, so a database that is briefly unavailable delays CDC instead of failing the host.
        await Task.Yield();

        if (!await StartWatcherAsync(stoppingToken))
        {
            return;
        }

        if (!HasHandlers())
        {
            // No handlers registered: the application reads SqlCdcWatcher.Changes itself.
            _logger.LogInformation("No CDC handlers are registered; leaving channel consumption to the application.");

            if (_watcher.Options.CheckpointMode == CdcCheckpointMode.OnAcknowledgement)
            {
                _logger.LogWarning(
                    "Checkpoint mode is {CheckpointMode} but no handler is registered: the application must call " +
                    "CdcChange.Acknowledge() on every change it reads, otherwise polling stalls at the first batch.",
                    CdcCheckpointMode.OnAcknowledgement);
            }

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }

            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Re-evaluated on every iteration: a restarted watcher delivers on a new channel.
                await foreach (var change in _watcher.Changes.WithCancellation(stoppingToken))
                {
                    await DispatchAsync(change, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            // The channel completing means the polling loop ended. A deliberate StopAsync by the
            // application is respected; a crash is restarted, otherwise the host would keep
            // running with CDC silently dead.
            if (!_watcher.HasCrashed)
            {
                _logger.LogInformation(
                    "The CDC watcher was stopped; the hosted service is no longer dispatching changes.");
                return;
            }

            _logger.LogError(
                "The CDC polling loop crashed; restarting the watcher in {RetryDelay}.",
                _watcher.Options.RetryDelay);

            try
            {
                await Task.Delay(_watcher.Options.RetryDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            if (!await StartWatcherAsync(stoppingToken))
            {
                return;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await _watcher.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Starts the watcher, retrying with the configured <see cref="CdcWatcherOptions.RetryDelay"/>
    /// until it succeeds or the host is stopped. Returns false when the host is shutting down.
    /// </summary>
    private async Task<bool> StartWatcherAsync(CancellationToken stoppingToken)
    {
        var retryDelay = _watcher.Options.RetryDelay;
        while (true)
        {
            try
            {
                await _watcher.StartAsync(stoppingToken);
                return true;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "The CDC watcher failed to start; retrying in {RetryDelay}. No change events are being delivered yet.",
                    retryDelay);

                try
                {
                    await Task.Delay(retryDelay, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return false;
                }
            }
        }
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
            await InvokeHandlerAsync(handler, change, ct);
        }

        // Acknowledged only after every handler has been given the change, and never on
        // cancellation (which rethrows below), so a shutdown mid-batch replays it on restart.
        change.Acknowledge();
    }

    /// <summary>
    /// Calls one handler, retrying up to <see cref="CdcWatcherOptions.MaxHandlerAttempts"/> times
    /// and dead-lettering the change when the attempts run out. Returning normally after a failure
    /// is deliberate: one poisonous change must not block every change behind it.
    /// </summary>
    private async Task InvokeHandlerAsync(ICdcChangeHandler handler, CdcChange change, CancellationToken ct)
    {
        var handlerName = handler.GetType().Name;
        var maxAttempts = _watcher.Options.MaxHandlerAttempts;

        for (var attempt = 1; ; attempt++)
        {
            using var activity = SqlCdcDiagnostics.ActivitySource.StartActivity(
                "SqlCdc.Handle", ActivityKind.Consumer);
            activity?.SetTag("watcher", _watcher.Name);
            activity?.SetTag("capture_instance", change.CaptureInstance);
            activity?.SetTag("table", change.TableName);
            activity?.SetTag("operation", change.Operation.ToString());
            activity?.SetTag("change_key", change.Key);
            activity?.SetTag("handler", handlerName);
            activity?.SetTag("attempt", attempt);

            var startedAt = Stopwatch.GetTimestamp();
            try
            {
                await handler.HandleAsync(change, ct);
                RecordHandlerDuration(handlerName, change, "success", startedAt);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                var willRetry = attempt < maxAttempts;
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                RecordHandlerDuration(handlerName, change, willRetry ? "retry" : "failed", startedAt);
                SqlCdcDiagnostics.HandlerFailures.Add(1, HandlerTags(handlerName, change));

                if (willRetry)
                {
                    var delay = BackoffFor(attempt);
                    _logger.LogWarning(
                        ex,
                        "Handler {Handler} failed for change {ChangeKey} on {TableName} " +
                        "(attempt {Attempt} of {MaxAttempts}); retrying in {RetryDelay}",
                        handlerName, change.Key, change.TableName, attempt, maxAttempts, delay);

                    await Task.Delay(delay, ct);
                    continue;
                }

                _logger.LogError(
                    ex,
                    "Handler {Handler} failed for change {ChangeKey} on {TableName} after {Attempts} attempt(s); " +
                    "the event is {Outcome}",
                    handlerName, change.Key, change.TableName, attempt,
                    _deadLetterSink is null ? "dropped" : "dead-lettered");

                await DeadLetterAsync(
                    new CdcDeadLetter(change, handlerName, attempt, ex, DateTimeOffset.UtcNow), ct);
                return;
            }
        }
    }

    private async Task DeadLetterAsync(CdcDeadLetter deadLetter, CancellationToken ct)
    {
        SqlCdcDiagnostics.DeadLetters.Add(1, HandlerTags(deadLetter.HandlerName, deadLetter.Change));

        if (_deadLetterSink is null)
        {
            return;
        }

        try
        {
            await _deadLetterSink.WriteAsync(deadLetter, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Nothing left to fall back on: say so loudly rather than pretend the change was kept.
            _logger.LogError(
                ex,
                "The dead-letter sink failed for change {ChangeKey} on {TableName}; the event is lost",
                deadLetter.Change.Key, deadLetter.Change.TableName);
        }
    }

    private TimeSpan BackoffFor(int attempt)
    {
        var milliseconds = _watcher.Options.HandlerRetryDelay.TotalMilliseconds * Math.Pow(2, attempt - 1);
        return milliseconds >= MaxHandlerBackoff.TotalMilliseconds
            ? MaxHandlerBackoff
            : TimeSpan.FromMilliseconds(milliseconds);
    }

    private void RecordHandlerDuration(string handlerName, CdcChange change, string outcome, long startedAt)
    {
        var tags = HandlerTags(handlerName, change);
        tags.Add("outcome", outcome);
        SqlCdcDiagnostics.HandlerDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds, tags);
    }

    private TagList HandlerTags(string handlerName, CdcChange change) => new()
    {
        { "watcher", _watcher.Name },
        { "capture_instance", change.CaptureInstance },
        { "handler", handlerName },
    };
}
