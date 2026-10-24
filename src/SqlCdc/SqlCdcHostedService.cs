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
    private readonly bool _hasHandlers;
    private readonly bool _hasBatchHandlers;

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

        // Checked here rather than in ExecuteAsync so the host fails to start with this exception
        // in hand, instead of starting and then stopping itself from a background failure.
        _hasHandlers = HasSingleHandlers();
        _hasBatchHandlers = HasBatchHandlers();
        if ((_hasHandlers || _hasBatchHandlers) && _deadLetterSink is null)
        {
            throw new InvalidOperationException(
                "CDC handlers are registered but no ICdcDeadLetterSink is configured. A handler that fails has " +
                "its change acknowledged and the watermark moves past it, so without a sink the change is lost. " +
                "Register one with AddCdcDeadLetterSink(...), for example new SqlCdcDeadLetterSink(connectionString).");
        }
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

        if (!_hasHandlers && !_hasBatchHandlers)
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

        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Re-evaluated on every iteration: a restarted watcher delivers on a new channel.
                if (_hasBatchHandlers)
                {
                    await foreach (var change in _watcher.Changes.WithCancellation(stoppingToken))
                    {
                        var batch = new List<CdcChange> { change };
                        while (batch.Count < _watcher.Options.BatchSize && _watcher.Channel.Reader.TryRead(out var nextChange))
                        {
                            batch.Add(nextChange);
                        }

                        await DispatchBatchAsync(batch, stoppingToken);
                    }
                }
                else if (_hasHandlers)
                {
                    await foreach (var change in _watcher.Changes.WithCancellation(stoppingToken))
                    {
                        await DispatchAsync(change, stoppingToken);
                    }
                }
                else
                {
                    // The application reads the channel; this service only watches for it to end,
                    // so a crash is restarted here too rather than leaving CDC dead behind a
                    // consumer that merely sees its enumeration complete.
                    await _watcher.Channel.Reader.Completion.WaitAsync(stoppingToken);
                }
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
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
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
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

    private bool HasSingleHandlers()
    {
        using var scope = _scopeFactory.CreateScope();
        return scope.ServiceProvider.GetServices<ICdcChangeHandler>().Any();
    }

    private bool HasBatchHandlers()
    {
        using var scope = _scopeFactory.CreateScope();
        return scope.ServiceProvider.GetServices<ICdcBatchChangeHandler>().Any();
    }

    private static Type GetHandlerType(object handler) =>
        handler is ITableBoundChangeHandler bound ? bound.HandlerType : handler.GetType();

    private static bool HandlerMatches(ICdcChangeHandler handler, CdcChange change)
    {
        if (handler is ICdcTableChangeHandler tableHandler)
        {
            if (!string.Equals(tableHandler.SourceTable, change.SourceTable, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (tableHandler.SourceSchema is not null &&
                !string.Equals(tableHandler.SourceSchema, change.SourceSchema, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        var tableAttrs = GetHandlerType(handler).GetCustomAttributes(typeof(CdcTableAttribute), inherit: true);
        if (tableAttrs.Length > 0)
        {
            var matches = false;
            foreach (CdcTableAttribute attr in tableAttrs)
            {
                if (string.Equals(attr.Table, change.SourceTable, StringComparison.OrdinalIgnoreCase) &&
                    (attr.Schema is null || string.Equals(attr.Schema, change.SourceSchema, StringComparison.OrdinalIgnoreCase)))
                {
                    matches = true;
                    break;
                }
            }

            if (!matches)
            {
                return false;
            }
        }

        return true;
    }

    private async Task DispatchAsync(CdcChange change, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var handlers = scope.ServiceProvider.GetServices<ICdcChangeHandler>().ToList();
        for (var i = 0; i < handlers.Count; i++)
        {
            if (HandlerMatches(handlers[i], change))
            {
                await InvokeHandlerAsync(handlers[i], i, change, ct);
            }
        }

        // Acknowledged only after every handler has been given the change, and never on
        // cancellation (which rethrows below), so a shutdown mid-batch replays it on restart.
        change.Acknowledge();
    }

    private async Task DispatchBatchAsync(IReadOnlyList<CdcChange> batch, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();

        if (_hasHandlers)
        {
            var singleHandlers = scope.ServiceProvider.GetServices<ICdcChangeHandler>().ToList();
            foreach (var change in batch)
            {
                for (var i = 0; i < singleHandlers.Count; i++)
                {
                    if (HandlerMatches(singleHandlers[i], change))
                    {
                        await InvokeHandlerAsync(singleHandlers[i], i, change, ct);
                    }
                }
            }
        }

        var batchHandlers = scope.ServiceProvider.GetServices<ICdcBatchChangeHandler>().ToList();
        for (var i = 0; i < batchHandlers.Count; i++)
        {
            await InvokeBatchHandlerAsync(batchHandlers[i], i, batch, ct);
        }

        foreach (var change in batch)
        {
            change.Acknowledge();
        }
    }

    private async Task InvokeBatchHandlerAsync(
        ICdcBatchChangeHandler initialHandler, int handlerIndex, IReadOnlyList<CdcChange> batch, CancellationToken ct)
    {
        var handler = initialHandler;
        var handlerName = handler.GetType().Name;
        var maxAttempts = _watcher.Options.MaxHandlerAttempts;

        for (var attempt = 1; ; attempt++)
        {
            Exception? failure = null;
            using (var retryScope = attempt == 1 ? null : _scopeFactory.CreateScope())
            {
                if (retryScope is not null)
                {
                    handler = retryScope.ServiceProvider.GetServices<ICdcBatchChangeHandler>().ElementAt(handlerIndex);
                }

                try
                {
                    await handler.HandleBatchAsync(batch, ct);
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    failure = ex;
                }
            }

            if (failure is null)
            {
                return;
            }

            var willRetry = attempt < maxAttempts;
            if (willRetry)
            {
                var delay = BackoffFor(attempt);
                _logger.LogWarning(
                    failure,
                    "Batch handler {Handler} failed for batch of {Count} changes (attempt {Attempt} of {MaxAttempts}); retrying in {RetryDelay}",
                    handlerName, batch.Count, attempt, maxAttempts, delay);

                await Task.Delay(delay, ct);
                continue;
            }

            _logger.LogError(
                failure,
                "Batch handler {Handler} failed for batch of {Count} changes after {Attempts} attempt(s); dead-lettering changes",
                handlerName, batch.Count, attempt);

            foreach (var change in batch)
            {
                await DeadLetterAsync(
                    new CdcDeadLetter(change, handlerName, attempt, failure, DateTimeOffset.UtcNow), ct);
            }

            return;
        }
    }

    /// <summary>
    /// Calls one handler, retrying up to <see cref="CdcWatcherOptions.MaxHandlerAttempts"/> times
    /// and dead-lettering the change when the attempts run out. Returning normally after a failure
    /// is deliberate: one poisonous change must not block every change behind it.
    /// </summary>
    private async Task InvokeHandlerAsync(
        ICdcChangeHandler initialHandler, int handlerIndex, CdcChange change, CancellationToken ct)
    {
        var handler = initialHandler;
        var handlerName = GetHandlerType(handler).Name;
        var maxAttempts = _watcher.Options.MaxHandlerAttempts;

        for (var attempt = 1; ; attempt++)
        {
            Exception? failure = null;
            using (var retryScope = attempt == 1 ? null : _scopeFactory.CreateScope())
            {
                if (retryScope is not null)
                {
                    handler = retryScope.ServiceProvider.GetServices<ICdcChangeHandler>().ElementAt(handlerIndex);
                }

                ActivityLink[]? links = change.TraceContext is { } tc
                    ? [new ActivityLink(tc)]
                    : null;

                using var activity = SqlCdcDiagnostics.ActivitySource.StartActivity(
                    "SqlCdc.Handle", ActivityKind.Consumer, parentContext: default, tags: null, links: links);
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
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    failure = ex;
                    var attemptWillRetry = attempt < maxAttempts;
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    RecordHandlerDuration(handlerName, change, attemptWillRetry ? "retry" : "failed", startedAt);
                    SqlCdcDiagnostics.HandlerFailures.Add(1, HandlerTags(handlerName, change));
                }
            }

            if (failure is null)
            {
                return;
            }

            var willRetry = attempt < maxAttempts;
            if (willRetry)
            {
                var delay = BackoffFor(attempt);
                _logger.LogWarning(
                    failure,
                    "Handler {Handler} failed for change {ChangeKey} on {TableName} " +
                    "(attempt {Attempt} of {MaxAttempts}); retrying in {RetryDelay}",
                    handlerName, change.Key, change.TableName, attempt, maxAttempts, delay);

                await Task.Delay(delay, ct);
                continue;
            }

            _logger.LogError(
                failure,
                "Handler {Handler} failed for change {ChangeKey} on {TableName} after {Attempts} attempt(s); " +
                "the event is {Outcome}",
                handlerName, change.Key, change.TableName, attempt,
                _deadLetterSink is null ? "dropped" : "dead-lettered");

            await DeadLetterAsync(
                new CdcDeadLetter(change, handlerName, attempt, failure, DateTimeOffset.UtcNow), ct);
            return;
        }
    }

    private async Task DeadLetterAsync(CdcDeadLetter deadLetter, CancellationToken ct)
    {
        SqlCdcDiagnostics.DeadLetters.Add(1, HandlerTags(deadLetter.HandlerName, deadLetter.Change));

        if (_deadLetterSink is null)
        {
            return;
        }

        // A configured dead-letter sink is the durable fallback for a failed handler.  Returning
        // when it is unavailable would let DispatchAsync acknowledge the change and advance the
        // CDC watermark, losing it permanently.  Keep this change unacknowledged until the sink
        // accepts it; backpressure is preferable to a silent data loss.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await _deadLetterSink.WriteAsync(deadLetter, ct);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                var delay = BackoffFor(attempt);

                // The first failure is the event worth alerting on; the retries that follow, one
                // a minute at most, would otherwise flood the error log while the sink is down.
                _logger.Log(
                    attempt == 1 ? LogLevel.Error : LogLevel.Warning,
                    ex,
                    "The dead-letter sink failed for change {ChangeKey} on {TableName} (attempt {Attempt}); " +
                    "delivery is paused and the write is retried in {RetryDelay}. In OnAcknowledgement mode the " +
                    "change stays unacknowledged, so a restart replays it; in OnEmit mode it is lost on restart.",
                    deadLetter.Change.Key, deadLetter.Change.TableName, attempt, delay);

                await Task.Delay(delay, ct);
            }
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
