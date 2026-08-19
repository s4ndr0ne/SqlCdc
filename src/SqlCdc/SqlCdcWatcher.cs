using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SqlCdc;

/// <summary>
/// Watches SQL Server native CDC capture instances and emits <see cref="CdcChange"/>
/// events onto a bounded <see cref="Channel{T}"/>. Create via <see cref="SqlCdcWatcherBuilder"/>.
/// </summary>
public sealed class SqlCdcWatcher : IAsyncDisposable
{
    /// <summary>Number of LSNs mapped to commit times in a single round-trip.</summary>
    private const int LsnTimeMapChunkSize = 500;

    /// <summary>How long to wait for a batch to be acknowledged before warning that polling is stalled.</summary>
    private static readonly TimeSpan CheckpointWarningInterval = TimeSpan.FromSeconds(30);

    /// <summary>Ceiling for the exponential backoff after consecutive polling failures.</summary>
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(5);

    private readonly CdcWatcherOptions _options;
    private readonly ICdcStateStore _stateStore;
    private readonly ICdcConnectionFactory _connections;
    private readonly ICdcLeaseProvider _leaseProvider;
    private readonly bool _ownsLeaseProvider;
    private readonly ILogger _logger;
    private Channel<CdcChange> _channel;
    private readonly ConcurrentDictionary<string, TableRuntime> _tables = new();
    private readonly SemaphoreSlim _stateLock = new(1, 1);
    private CancellationTokenSource? _cts;
    private Task? _pollTask;
    private volatile bool _isLeader;
    private bool _standbyLogged;
    private long _lastLeaseCheckTick;

    internal SqlCdcWatcher(
        CdcWatcherOptions options,
        ICdcStateStore stateStore,
        ILogger? logger = null,
        ICdcLeaseProvider? leaseProvider = null,
        bool ownsLeaseProvider = false,
        ICdcConnectionFactory? connections = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(stateStore);

        if (options.PollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "PollInterval must be positive.");
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be positive.");
        }

        if (options.ChannelCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ChannelCapacity must be positive.");
        }

        if (options.RetryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "RetryDelay must be positive.");
        }

        if (options.CommandTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "CommandTimeout must be positive.");
        }

        if (options.LeaseRetryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "LeaseRetryDelay must be positive.");
        }

        if (options.LeaseKeepaliveInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "LeaseKeepaliveInterval must be positive.");
        }

        if (string.IsNullOrWhiteSpace(options.Name))
        {
            throw new ArgumentException("Name must not be empty.", nameof(options));
        }

        if (options.MaxHandlerAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxHandlerAttempts must be at least 1.");
        }

        if (options.HandlerRetryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "HandlerRetryDelay must be positive.");
        }

        _options = options;
        _stateStore = stateStore;
        _connections = connections ?? (string.IsNullOrWhiteSpace(options.ConnectionString)
            ? throw new ArgumentException(
                "Either a connection string or a connection factory is required.", nameof(options))
            : new SqlCdcConnectionFactory(options.ConnectionString));
        _leaseProvider = leaseProvider ?? NullCdcLeaseProvider.Instance;
        _ownsLeaseProvider = ownsLeaseProvider && leaseProvider is not null;
        _logger = logger ?? NullLogger<SqlCdcWatcher>.Instance;

        _channel = CreateChannel();
        SqlCdcDiagnostics.Register(this);
    }

    /// <summary>Name identifying this watcher in metrics and health data.</summary>
    public string Name => _options.Name;

    /// <summary>The effective options for this watcher.</summary>
    internal CdcWatcherOptions Options => _options;

    private int CommandTimeoutSeconds =>
        (int)Math.Clamp(Math.Ceiling(_options.CommandTimeout.TotalSeconds), 1, int.MaxValue);

    /// <summary>The bounded channel events are delivered onto.</summary>
    public Channel<CdcChange> Channel => _channel;

    /// <summary>Asynchronous sequence of change events. Completion is signaled on <see cref="StopAsync"/>.</summary>
    public IAsyncEnumerable<CdcChange> Changes => _channel.Reader.ReadAllAsync();

    /// <summary>True while the polling loop is running.</summary>
    public bool IsRunning => _pollTask is { IsCompleted: false };

    /// <summary>
    /// True while this instance holds the lease and is therefore the one polling. Always true when
    /// no lease provider is configured; false on a standby instance waiting to take over.
    /// </summary>
    public bool IsLeader => _isLeader;

    /// <summary>
    /// Resolves the capture instances for the configured tables and starts the polling loop.
    /// Safe to call repeatedly and concurrently with <see cref="StopAsync"/>.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            if (_pollTask is { IsCompleted: false })
            {
                return;
            }

            if (_pollTask is not null)
            {
                _channel.Writer.TryComplete();
                _cts?.Dispose();
                _cts = null;
                _pollTask = null;
                _channel = CreateChannel();
            }

            _tables.Clear();
            foreach (var subscription in _options.Tables)
            {
                // Watermarks are not read here: they are loaded once the lease is held, because a
                // standby instance must not act on a watermark the active one has since advanced.
                var runtime = await ResolveTableAsync(subscription, cancellationToken);
                _tables[runtime.CaptureInstance] = runtime;
            }

            if (_tables.Count == 0)
            {
                throw new InvalidOperationException("No CDC tables were resolved. Is CDC enabled for the configured tables?");
            }

            _isLeader = false;
            _standbyLogged = false;

            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var token = cts.Token;
            _cts = cts;
            _pollTask = Task.Run(() => RunLoopAsync(token));
        }
        finally
        {
            _stateLock.Release();
        }
    }

    /// <summary>Stops the polling loop and completes the channel.</summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _stateLock.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        try
        {
            if (_cts is null)
            {
                return;
            }

            _cts.Cancel();
            try
            {
                if (_pollTask is not null)
                {
                    await _pollTask;
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _channel.Writer.TryComplete();
                _cts.Dispose();
                _cts = null;
                _pollTask = null;
            }

            await ReleaseLeaseAsync(cancellationToken);
        }
        finally
        {
            _stateLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();

        if (_ownsLeaseProvider)
        {
            await _leaseProvider.DisposeAsync();
        }

        SqlCdcDiagnostics.Unregister(this);
        _stateLock.Dispose();
    }

    /// <summary>
    /// Takes a snapshot of what the watcher is doing: whether it is polling, whether it holds the
    /// lease, and per capture instance the failure count, last successful poll and changes emitted.
    /// Reads counters the polling loop already maintains, so it is cheap enough for a health probe.
    /// </summary>
    public CdcWatcherStatus GetStatus()
    {
        var tables = _tables.Values
            .Select(t => new CdcTableStatus(
                t.CaptureInstance,
                t.Schema,
                t.Table,
                t.ConsecutiveFailures,
                t.LastSuccessfulPoll,
                t.LastEmittedCommitTime,
                Interlocked.Read(ref t.ChangesEmitted)))
            .OrderBy(t => t.CaptureInstance, StringComparer.Ordinal)
            .ToList();

        return new CdcWatcherStatus(Name, IsRunning, IsLeader, _channel.Reader.Count, tables);
    }

    /// <summary>
    /// Hands the lease back so a standby instance can take over immediately instead of waiting for
    /// SQL Server to notice the session is gone.
    /// </summary>
    private async Task ReleaseLeaseAsync(CancellationToken cancellationToken)
    {
        if (!_isLeader)
        {
            return;
        }

        _isLeader = false;
        try
        {
            await _leaseProvider.ReleaseAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // A shutdown too short to release cleanly: the lease still goes away with the process,
            // it just takes SQL Server a little longer to notice.
            _logger.LogDebug("Stopping was cancelled before the CDC lease could be released.");
        }
        catch (Exception ex)
        {
            // Dropping the lease connection releases it anyway; failing to stop would be worse.
            _logger.LogWarning(ex, "Releasing the CDC lease failed.");
        }
    }

    private Channel<CdcChange> CreateChannel() =>
        System.Threading.Channels.Channel.CreateBounded<CdcChange>(new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!await EnsureLeadershipAsync(ct))
                {
                    await Task.Delay(_options.LeaseRetryDelay, ct);
                    continue;
                }

                foreach (var table in _tables.Values)
                {
                    if (ct.IsCancellationRequested)
                    {
                        return;
                    }

                    // A table that just failed backs off on its own, so a single broken capture
                    // instance neither blocks the healthy ones nor is retried every poll interval.
                    if (Environment.TickCount64 < table.NextAttemptTick)
                    {
                        continue;
                    }

                    using var activity = SqlCdcDiagnostics.ActivitySource.StartActivity(
                        "SqlCdc.Poll", ActivityKind.Client);
                    activity?.SetTag("watcher", _options.Name);
                    activity?.SetTag("capture_instance", table.CaptureInstance);
                    var startedAt = Stopwatch.GetTimestamp();

                    try
                    {
                        await PollTableAsync(table, ct, activity);
                        table.ConsecutiveFailures = 0;
                        table.NextAttemptTick = 0;
                        table.LastSuccessfulPoll = DateTimeOffset.UtcNow;
                        SqlCdcDiagnostics.PollDuration.Record(
                            Stopwatch.GetElapsedTime(startedAt).TotalSeconds, TableTags(table));
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        table.ConsecutiveFailures++;
                        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        SqlCdcDiagnostics.PollFailures.Add(1, TableTags(table));
                        var retryDelay = RetryDelayFor(table.ConsecutiveFailures);
                        var retryMs = retryDelay.TotalMilliseconds;
                        table.NextAttemptTick = retryMs > long.MaxValue - Environment.TickCount64
                            ? long.MaxValue
                            : Environment.TickCount64 + (long)retryMs;
                        _logger.LogError(
                            ex,
                            "CDC polling failed for capture instance {CaptureInstance} " +
                            "({ConsecutiveFailures} consecutive failures), retrying in {RetryDelay}",
                            table.CaptureInstance, table.ConsecutiveFailures, retryDelay);
                    }
                }

                if (!ct.IsCancellationRequested)
                {
                    await Task.Delay(_options.PollInterval, ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "The CDC polling loop terminated unexpectedly.");
        }
        finally
        {
            // External cancellation must complete readers too; StopAsync remains responsible for
            // disposing the CTS and allowing a later StartAsync to create a fresh channel.
            _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Gates polling on the lease. Returns false when this instance must stand by, in which case
    /// the caller backs off for <see cref="CdcWatcherOptions.LeaseRetryDelay"/> before asking again.
    /// </summary>
    private async Task<bool> EnsureLeadershipAsync(CancellationToken ct)
    {
        try
        {
            if (_isLeader)
            {
                // The lease is only verified every LeaseKeepaliveInterval, not on every polling
                // cycle: with a short poll interval the keepalive would otherwise dominate the
                // traffic on the lease connection. The trade-off is that a lost lease can go
                // unnoticed for up to one interval, during which the monotonic watermark keeps
                // the old and the new leader from rewinding each other.
                var elapsedMs = Environment.TickCount64 - _lastLeaseCheckTick;
                if (elapsedMs < _options.LeaseKeepaliveInterval.TotalMilliseconds)
                {
                    return true;
                }

                if (await _leaseProvider.IsHeldAsync(ct))
                {
                    _lastLeaseCheckTick = Environment.TickCount64;
                    return true;
                }

                _isLeader = false;
                _standbyLogged = false;
                _logger.LogWarning(
                    "Lost the CDC lease; polling is paused until it is re-acquired. " +
                    "Another instance may have taken over.");
                return false;
            }

            if (!await _leaseProvider.TryAcquireAsync(ct))
            {
                if (!_standbyLogged)
                {
                    _standbyLogged = true;
                    _logger.LogInformation(
                        "Another instance holds the CDC lease; standing by and retrying every {LeaseRetryDelay}.",
                        _options.LeaseRetryDelay);
                }

                return false;
            }

            // Read the watermarks only now: while this instance was standing by, the active one
            // advanced them, and resuming from a stale watermark would replay everything since.
            await LoadWatermarksAsync(ct);

            _lastLeaseCheckTick = Environment.TickCount64;
            _isLeader = true;
            _standbyLogged = false;
            _logger.LogInformation("Acquired the CDC lease; this instance is now the active watcher.");
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _isLeader = false;
            _logger.LogError(
                ex,
                "Could not establish the CDC lease; retrying in {LeaseRetryDelay}. No change events are being delivered.",
                _options.LeaseRetryDelay);
            return false;
        }
    }

    /// <summary>
    /// Backoff before a failing capture instance is polled again: the configured delay, doubling
    /// with each consecutive failure up to <see cref="MaxRetryDelay"/>. A capture instance that is
    /// broken for hours (permissions revoked, table dropped) is then retried every few minutes
    /// instead of being hammered — and logged — every <see cref="CdcWatcherOptions.RetryDelay"/>.
    /// </summary>
    private TimeSpan RetryDelayFor(int consecutiveFailures)
    {
        var milliseconds = _options.RetryDelay.TotalMilliseconds * Math.Pow(2, consecutiveFailures - 1);
        return milliseconds >= MaxRetryDelay.TotalMilliseconds
            ? MaxRetryDelay
            : TimeSpan.FromMilliseconds(milliseconds);
    }

    /// <summary>Reloads every table's watermark from the state store.</summary>
    private async Task LoadWatermarksAsync(CancellationToken ct)
    {
        foreach (var table in _tables.Values)
        {
            table.Watermark = await _stateStore.GetLastLsnAsync(table.CaptureInstance, ct);
        }
    }

    private TagList TableTags(TableRuntime table) => new()
    {
        { "watcher", _options.Name },
        { "capture_instance", table.CaptureInstance },
    };

    private async Task PollTableAsync(TableRuntime table, CancellationToken ct, Activity? activity = null)
    {
        // One connection for the whole poll — bounds, changes and time mapping — instead of one
        // per round-trip. It stays open across channel writes and acknowledgement waits, which is
        // fine: a connection dropped during a long consumer stall surfaces as a poll failure on
        // the next read and the poll is retried on a fresh one.
        await using var conn = await _connections.OpenConnectionAsync(ct);

        var (maxLsn, minLsn) = await GetLogBoundsAsync(conn, table.CaptureInstance, ct);
        if (maxLsn is null)
        {
            return;
        }

        byte[] cursor;
        if (table.Watermark is null)
        {
            var (start, watermark) = InitializeCursor(table, maxLsn, minLsn);
            if (start is null)
            {
                return;
            }

            if (watermark is not null)
            {
                table.Watermark = watermark;
                await SaveLastLsnAsync(conn, table.CaptureInstance, watermark, ct);
            }

            cursor = start;
        }
        else
        {
            cursor = LsnHelpers.Increment(table.Watermark);
        }

        // The CDC cleanup job trims the change tables on its retention schedule (3 days by default).
        // A watermark older than that points at rows which no longer exist, and reading from it makes
        // fn_cdc_get_all_changes fail on every poll. Clamp forward and report the gap instead.
        if (minLsn is not null && LsnHelpers.Compare(cursor, minLsn) < 0)
        {
            // Report the loss once per min LSN: while the watermark stays stale and no new data
            // arrives the clamp fires on every poll, which would otherwise spam the warning.
            if (table.LastClampedMinLsn is null || !table.LastClampedMinLsn.AsSpan().SequenceEqual(minLsn))
            {
                _logger.LogWarning(
                    "Capture instance {CaptureInstance}: next LSN to read {Cursor} is older than the earliest " +
                    "retained LSN {MinLsn}. The changes in between were removed by the CDC cleanup job and " +
                    "are lost; resuming from the earliest retained LSN.",
                    table.CaptureInstance,
                    Convert.ToHexString(cursor),
                    Convert.ToHexString(minLsn));
                table.LastClampedMinLsn = (byte[])minLsn.Clone();
            }

            cursor = minLsn;
        }

        while (LsnHelpers.Compare(cursor, maxLsn) <= 0)
        {
            var batch = await ReadChangesAsync(conn, table, cursor, maxLsn, ct);
            if (batch.Rows.Count > 0 && batch.FullyConsumedLsn is not null)
            {
                SqlCdcDiagnostics.BatchRows.Record(batch.Rows.Count, TableTags(table));

                var (timeMap, serverTime) = await MapLsnToTimeAsync(conn, batch.Rows.Select(r => r.Lsn), ct);
                var barrier = _options.CheckpointMode == CdcCheckpointMode.OnAcknowledgement
                    ? new CheckpointBarrier()
                    : null;
                var emitted = 0;

                foreach (var change in CdcChangePairer.Pair(
                    table.Schema, table.Table, table.CaptureInstance, table.CapturedColumns, batch.Rows, timeMap))
                {
                    // Registered before the change is written: once it is on the channel a consumer
                    // can acknowledge it at any moment, and the barrier must already know about it.
                    var published = barrier is null ? change : change with { Acknowledgement = barrier.Register() };
                    await _channel.Writer.WriteAsync(published, ct);

                    emitted++;
                    RecordEmitted(table, published, serverTime);
                }

                Interlocked.Add(ref table.ChangesEmitted, emitted);
                activity?.SetTag("changes", emitted);

                if (barrier is not null)
                {
                    barrier.Seal();
                    await WaitForAcknowledgementsAsync(table, barrier, ct);
                }

                table.Watermark = batch.FullyConsumedLsn;
                await SaveLastLsnAsync(conn, table.CaptureInstance, batch.FullyConsumedLsn, ct);
                cursor = LsnHelpers.Increment(batch.FullyConsumedLsn);
            }

            if (!batch.HitCap || table.Watermark is null || LsnHelpers.Compare(table.Watermark, maxLsn) >= 0)
            {
                break;
            }
        }
    }

    private Task SaveLastLsnAsync(SqlConnection connection, string captureInstance, byte[] lsn, CancellationToken ct) =>
        _stateStore is SqlCdcStateStore sqlStateStore
            ? sqlStateStore.SaveLastLsnAsync(connection, captureInstance, lsn, ct)
            : _stateStore.SaveLastLsnAsync(captureInstance, lsn, ct);

    /// <summary>
    /// Counts an emitted change and records how far behind the source it was. Lag is measured
    /// entirely on the SQL Server clock: <c>fn_cdc_map_lsn_to_time</c> reports the server's local
    /// time, so subtracting the client's UTC would measure the time zone offset instead.
    /// </summary>
    private void RecordEmitted(TableRuntime table, CdcChange change, DateTime? serverTime)
    {
        var tags = TableTags(table);
        tags.Add("operation", change.Operation.ToString());
        SqlCdcDiagnostics.ChangesEmitted.Add(1, tags);

        if (change.CommitTime == default)
        {
            return;
        }

        table.LastEmittedCommitTime = change.CommitTime;

        if (serverTime is not null)
        {
            var lag = serverTime.Value - change.CommitTime;
            if (lag > TimeSpan.Zero)
            {
                SqlCdcDiagnostics.ChangeLag.Record(lag.TotalSeconds, TableTags(table));
            }
        }
    }

    /// <summary>
    /// Waits for the whole batch to be acknowledged before its watermark is persisted. A consumer
    /// that never acknowledges pauses polling for that table indefinitely, so the wait is reported
    /// periodically rather than left silent.
    /// </summary>
    private async Task WaitForAcknowledgementsAsync(TableRuntime table, CheckpointBarrier barrier, CancellationToken ct)
    {
        var waited = TimeSpan.Zero;
        while (true)
        {
            try
            {
                await barrier.Completion.WaitAsync(CheckpointWarningInterval, ct);
                return;
            }
            catch (TimeoutException)
            {
                waited += CheckpointWarningInterval;
                _logger.LogWarning(
                    "Capture instance {CaptureInstance}: still waiting after {Waited} for the current batch to be " +
                    "acknowledged. The watermark cannot advance and polling is paused for this table. Is every " +
                    "change acknowledged with CdcChange.Acknowledge()?",
                    table.CaptureInstance, waited);
            }
        }
    }

    private async Task<ChangeBatch> ReadChangesAsync(
        SqlConnection conn, TableRuntime table, byte[] fromLsn, byte[] toLsn, CancellationToken ct)
    {
        var functionName = $"cdc.{SqlIdentifier.Quote($"fn_cdc_get_all_changes_{table.CaptureInstance}", nameof(table.CaptureInstance))}";
        var builder = new ChangeBatchBuilder(_options.BatchSize);

        await using var cmd = new SqlCommand($"SELECT * FROM {functionName}(@from, @to, N'all update old')", conn);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.Add("@from", SqlDbType.Binary, 10).Value = fromLsn;
        cmd.Parameters.Add("@to", SqlDbType.Binary, 10).Value = toLsn;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var ordinal = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            ordinal[reader.GetName(i)] = i;
        }

        while (await reader.ReadAsync(ct))
        {
            if (!builder.Add(ReadRow(reader, ordinal)))
            {
                break;
            }
        }

        var batch = builder.Build();
        if (batch.Rows.Count > _options.BatchSize)
        {
            _logger.LogWarning(
                "Capture instance {CaptureInstance}: a single transaction produced {RowCount} rows, " +
                "exceeding the configured batch size of {BatchSize}. It is read in full to keep " +
                "update before/after images together.",
                table.CaptureInstance, batch.Rows.Count, _options.BatchSize);
        }

        return batch;
    }

    private static RawRow ReadRow(SqlDataReader reader, Dictionary<string, int> ordinal)
    {
        var lsn = (byte[])reader.GetValue(ordinal["__$start_lsn"]);
        var seqVal = (byte[])reader.GetValue(ordinal["__$seqval"]);
        var operation = Convert.ToInt32(reader.GetValue(ordinal["__$operation"]));
        var updateMask = (byte[])reader.GetValue(ordinal["__$update_mask"]);

        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, index) in ordinal)
        {
            if (name.StartsWith("__$", StringComparison.Ordinal))
            {
                continue;
            }

            var value = reader.GetValue(index);
            values[name] = value is DBNull ? null : value;
        }

        return new RawRow(lsn, seqVal, operation, updateMask, values);
    }

    private async Task<TableRuntime> ResolveTableAsync(CdcTableSubscription subscription, CancellationToken ct)
    {
        await using var conn = await _connections.OpenConnectionAsync(ct);

        var available = await GetCaptureInstancesAsync(conn, subscription, ct);
        var captureInstance = SelectCaptureInstance(subscription, available);

        var columns = await GetCapturedColumnsAsync(conn, captureInstance, ct);
        if (columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"No captured columns found for capture instance '{captureInstance}'.");
        }

        return new TableRuntime(subscription.Schema, subscription.Table, captureInstance, columns);
    }

    /// <summary>
    /// Lists the capture instances defined for a source table, oldest first. SQL Server allows two
    /// per table, which is how a schema change is rolled out without stopping capture.
    /// </summary>
    private async Task<List<string>> GetCaptureInstancesAsync(
        SqlConnection conn,
        CdcTableSubscription subscription,
        CancellationToken ct)
    {
        const string sql =
            """
            SELECT ct.capture_instance
            FROM cdc.change_tables ct
            JOIN sys.tables t ON t.object_id = ct.source_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.name = @table
            ORDER BY ct.create_date, ct.capture_instance;
            """;

        var result = new List<string>();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@schema", subscription.Schema);
        cmd.Parameters.AddWithValue("@table", subscription.Table);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    /// <summary>
    /// Picks which capture instance to read. An explicit choice is honoured but verified, and when
    /// a table has the two instances of a schema migration the older one wins: it is the one the
    /// watermark belongs to, so the switch stays an explicit operational step rather than
    /// something that happens on its own at the next restart.
    /// </summary>
    private string SelectCaptureInstance(CdcTableSubscription subscription, IReadOnlyList<string> available)
    {
        var table = $"[{subscription.Schema}].[{subscription.Table}]";

        if (available.Count == 0)
        {
            throw new InvalidOperationException(
                $"No CDC capture instance found for table {table}. " +
                "Enable CDC first: EXEC sys.sp_cdc_enable_table ...");
        }

        if (subscription.CaptureInstance is { } requested)
        {
            var match = available.FirstOrDefault(
                ci => string.Equals(ci, requested, StringComparison.OrdinalIgnoreCase));

            // Without this check a typo only surfaces later, as a "invalid object name
            // cdc.fn_cdc_get_all_changes_..." on every poll.
            return match ?? throw new InvalidOperationException(
                $"Capture instance '{requested}' is not defined for table {table}. " +
                $"Available: {string.Join(", ", available)}.");
        }

        if (available.Count > 1)
        {
            _logger.LogWarning(
                "Table {Table} has {Count} capture instances ({CaptureInstances}); reading the oldest one, " +
                "{Selected}. This is expected during a schema migration. Pass the capture instance explicitly " +
                "to WatchTable to choose, and note that switching starts that instance from its own watermark.",
                table, available.Count, string.Join(", ", available), available[0]);
        }

        return available[0];
    }

    private async Task<List<string>> GetCapturedColumnsAsync(SqlConnection conn, string captureInstance, CancellationToken ct)
    {
        const string sql =
            """
            SELECT cc.column_name
            FROM cdc.captured_columns cc
            JOIN cdc.change_tables ct ON ct.object_id = cc.object_id
            WHERE ct.capture_instance = @ci
            ORDER BY cc.column_ordinal;
            """;

        var result = new List<string>();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@ci", captureInstance);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    /// <summary>
    /// Reads the end of the log and the earliest LSN still retained for a capture instance.
    /// Both come from a single round-trip, so the retention check costs nothing extra.
    /// </summary>
    private async Task<(byte[]? MaxLsn, byte[]? MinLsn)> GetLogBoundsAsync(
        SqlConnection conn, string captureInstance, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(
            "SELECT sys.fn_cdc_get_max_lsn(), sys.fn_cdc_get_min_lsn(@ci);", conn);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@ci", captureInstance);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return (null, null);
        }

        var maxLsn = await reader.IsDBNullAsync(0, ct) ? null : (byte[])reader.GetValue(0);
        var minLsn = await reader.IsDBNullAsync(1, ct) ? null : (byte[])reader.GetValue(1);
        return (maxLsn, minLsn);
    }

    /// <summary>
    /// Picks the first LSN to read for a table that has no watermark yet. Returns the cursor to read
    /// from and, when the start point can be recorded without reading anything, the watermark to store.
    /// </summary>
    private (byte[]? Cursor, byte[]? Watermark) InitializeCursor(TableRuntime table, byte[] maxLsn, byte[]? minLsn)
    {
        if (_options.StartMode == CdcStartMode.FromNow)
        {
            // "From now" is exactly "everything after the current end of the log". Anchoring on
            // fn_cdc_get_max_lsn() avoids fn_cdc_map_time_to_lsn, which interprets its argument in
            // the SQL Server's local time (not the client's UTC) and returns NULL on an idle database.
            _logger.LogInformation(
                "Starting capture instance {CaptureInstance} from current max LSN {Lsn}",
                table.CaptureInstance, Convert.ToHexString(maxLsn));

            return (LsnHelpers.Increment(maxLsn), maxLsn);
        }

        if (minLsn is null)
        {
            return (null, null);
        }

        _logger.LogInformation(
            "Starting capture instance {CaptureInstance} from min LSN {Lsn}",
            table.CaptureInstance, Convert.ToHexString(minLsn));

        // The watermark stays unset until the first batch is actually emitted, so nothing is skipped.
        return (minLsn, null);
    }

    /// <summary>
    /// Maps LSNs to their commit times, and reports the server's own clock alongside them so lag
    /// can be computed without knowing the server's time zone.
    /// </summary>
    private async Task<(Dictionary<string, DateTime> TimeMap, DateTime? ServerTime)> MapLsnToTimeAsync(
        SqlConnection conn,
        IEnumerable<byte[]> lsns,
        CancellationToken ct)
    {
        var result = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        var distinct = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        DateTime? serverTime = null;
        foreach (var lsn in lsns)
        {
            distinct.TryAdd(Convert.ToHexString(lsn), lsn);
        }

        if (distinct.Count == 0)
        {
            return (result, serverTime);
        }

        // One round-trip per chunk rather than per LSN. The chunk size stays well under the
        // 1000-row limit of a table value constructor and the 2100-parameter limit.
        foreach (var chunk in distinct.Values.Chunk(LsnTimeMapChunkSize))
        {
            var tuples = string.Join(",", Enumerable.Range(0, chunk.Length).Select(i => $"(@l{i})"));
            await using var cmd = new SqlCommand(
                $"SELECT v.lsn, sys.fn_cdc_map_lsn_to_time(v.lsn), SYSDATETIME() " +
                $"FROM (VALUES {tuples}) AS v(lsn);", conn);
            cmd.CommandTimeout = CommandTimeoutSeconds;
            for (var i = 0; i < chunk.Length; i++)
            {
                cmd.Parameters.Add($"@l{i}", SqlDbType.Binary, 10).Value = chunk[i];
            }

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                serverTime = reader.GetDateTime(2);

                if (await reader.IsDBNullAsync(1, ct))
                {
                    continue;
                }

                result[Convert.ToHexString((byte[])reader.GetValue(0))] = reader.GetDateTime(1);
            }
        }

        return (result, serverTime);
    }

    private sealed class TableRuntime
    {
        public TableRuntime(string schema, string table, string captureInstance, IReadOnlyList<string> capturedColumns)
        {
            Schema = schema;
            Table = table;
            CaptureInstance = captureInstance;
            CapturedColumns = capturedColumns;
        }

        /// <summary>Changes emitted for this capture instance; read from other threads by GetStatus.</summary>
        public long ChangesEmitted;

        public string Schema { get; }
        public string Table { get; }
        public string CaptureInstance { get; }
        public IReadOnlyList<string> CapturedColumns { get; }
        public byte[]? Watermark { get; set; }

        /// <summary>When this capture instance was last polled without error.</summary>
        public DateTimeOffset? LastSuccessfulPoll { get; set; }

        /// <summary>Commit time of the last change emitted, in SQL Server local time.</summary>
        public DateTime? LastEmittedCommitTime { get; set; }

        /// <summary>Failed polls in a row for this table; reset on the first success.</summary>
        public int ConsecutiveFailures { get; set; }

        /// <summary><see cref="Environment.TickCount64"/> before which this table is not polled again.</summary>
        public long NextAttemptTick { get; set; }

        /// <summary>The min LSN for which the retention-gap warning was last reported, so it is not repeated.</summary>
        public byte[]? LastClampedMinLsn { get; set; }
    }
}
