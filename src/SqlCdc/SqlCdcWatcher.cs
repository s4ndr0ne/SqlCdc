using System.Collections.Concurrent;
using System.Data;
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

    private readonly CdcWatcherOptions _options;
    private readonly ICdcStateStore _stateStore;
    private readonly ILogger _logger;
    private readonly Channel<CdcChange> _channel;
    private readonly ConcurrentDictionary<string, TableRuntime> _tables = new();
    private CancellationTokenSource? _cts;
    private Task? _pollTask;

    internal SqlCdcWatcher(CdcWatcherOptions options, ICdcStateStore stateStore, ILogger? logger = null)
    {
        _options = options;
        _stateStore = stateStore;
        _logger = logger ?? NullLogger<SqlCdcWatcher>.Instance;

        _channel = System.Threading.Channels.Channel.CreateBounded<CdcChange>(new BoundedChannelOptions(options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });
    }

    /// <summary>The bounded channel events are delivered onto.</summary>
    public Channel<CdcChange> Channel => _channel;

    /// <summary>Asynchronous sequence of change events. Completion is signaled on <see cref="StopAsync"/>.</summary>
    public IAsyncEnumerable<CdcChange> Changes => _channel.Reader.ReadAllAsync();

    /// <summary>True while the polling loop is running.</summary>
    public bool IsRunning => _pollTask is { IsCompleted: false };

    /// <summary>
    /// Resolves the capture instances for the configured tables and starts the polling loop.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        foreach (var subscription in _options.Tables)
        {
            var runtime = await ResolveTableAsync(subscription, cancellationToken);
            runtime.Watermark = await _stateStore.GetLastLsnAsync(runtime.CaptureInstance, cancellationToken);
            _tables[runtime.CaptureInstance] = runtime;
        }

        if (_tables.Count == 0)
        {
            throw new InvalidOperationException("No CDC tables were resolved. Is CDC enabled for the configured tables?");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _pollTask = Task.Run(() => RunLoopAsync(_cts.Token));
    }

    /// <summary>Stops the polling loop and completes the channel.</summary>
    public async Task StopAsync()
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
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
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

                try
                {
                    await PollTableAsync(table, ct);
                    table.ConsecutiveFailures = 0;
                    table.NextAttemptTick = 0;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    table.ConsecutiveFailures++;
                    table.NextAttemptTick = Environment.TickCount64 + (long)_options.RetryDelay.TotalMilliseconds;
                    _logger.LogError(
                        ex,
                        "CDC polling failed for capture instance {CaptureInstance} " +
                        "({ConsecutiveFailures} consecutive failures), retrying in {RetryDelay}",
                        table.CaptureInstance, table.ConsecutiveFailures, _options.RetryDelay);
                }
            }

            if (!ct.IsCancellationRequested)
            {
                await Task.Delay(_options.PollInterval, ct);
            }
        }
    }

    private async Task PollTableAsync(TableRuntime table, CancellationToken ct)
    {
        var (maxLsn, minLsn) = await GetLogBoundsAsync(table.CaptureInstance, ct);
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
                await _stateStore.SaveLastLsnAsync(table.CaptureInstance, watermark, ct);
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
            _logger.LogWarning(
                "Capture instance {CaptureInstance}: next LSN to read {Cursor} is older than the earliest " +
                "retained LSN {MinLsn}. The changes in between were removed by the CDC cleanup job and " +
                "are lost; resuming from the earliest retained LSN.",
                table.CaptureInstance,
                Convert.ToHexString(cursor),
                Convert.ToHexString(minLsn));

            cursor = minLsn;
        }

        while (LsnHelpers.Compare(cursor, maxLsn) <= 0)
        {
            var batch = await ReadChangesAsync(table, cursor, maxLsn, ct);
            if (batch.Rows.Count > 0 && batch.FullyConsumedLsn is not null)
            {
                var timeMap = await MapLsnToTimeAsync(batch.Rows.Select(r => r.Lsn), ct);
                foreach (var change in CdcChangePairer.Pair(
                    table.Schema, table.Table, table.CaptureInstance, table.CapturedColumns, batch.Rows, timeMap))
                {
                    await _channel.Writer.WriteAsync(change, ct);
                }

                table.Watermark = batch.FullyConsumedLsn;
                await _stateStore.SaveLastLsnAsync(table.CaptureInstance, batch.FullyConsumedLsn, ct);
                cursor = LsnHelpers.Increment(batch.FullyConsumedLsn);
            }

            if (!batch.HitCap || table.Watermark is null || LsnHelpers.Compare(table.Watermark, maxLsn) >= 0)
            {
                break;
            }
        }
    }

    private async Task<ChangeBatch> ReadChangesAsync(TableRuntime table, byte[] fromLsn, byte[] toLsn, CancellationToken ct)
    {
        var functionName = $"cdc.fn_cdc_get_all_changes_{table.CaptureInstance}";
        var builder = new ChangeBatchBuilder(_options.BatchSize);

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand($"SELECT * FROM {functionName}(@from, @to, N'all update old')", conn);
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
        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);

        const string sql =
            """
            SELECT ct.capture_instance
            FROM cdc.change_tables ct
            JOIN sys.tables t ON t.object_id = ct.source_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.name = @table;
            """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@schema", subscription.Schema);
        cmd.Parameters.AddWithValue("@table", subscription.Table);
        var captureInstance = subscription.CaptureInstance ?? (string?)await cmd.ExecuteScalarAsync(ct);

        if (string.IsNullOrWhiteSpace(captureInstance))
        {
            throw new InvalidOperationException(
                $"No CDC capture instance found for table [{subscription.Schema}].[{subscription.Table}]. " +
                "Enable CDC first: EXEC sys.sp_cdc_enable_table ...");
        }

        var columns = await GetCapturedColumnsAsync(conn, captureInstance, ct);
        if (columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"No captured columns found for capture instance '{captureInstance}'.");
        }

        return new TableRuntime(subscription.Schema, subscription.Table, captureInstance, columns);
    }

    private static async Task<List<string>> GetCapturedColumnsAsync(SqlConnection conn, string captureInstance, CancellationToken ct)
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
    private async Task<(byte[]? MaxLsn, byte[]? MinLsn)> GetLogBoundsAsync(string captureInstance, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(
            "SELECT sys.fn_cdc_get_max_lsn(), sys.fn_cdc_get_min_lsn(@ci);", conn);
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

    private async Task<Dictionary<string, DateTime>> MapLsnToTimeAsync(IEnumerable<byte[]> lsns, CancellationToken ct)
    {
        var result = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        var distinct = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var lsn in lsns)
        {
            distinct.TryAdd(Convert.ToHexString(lsn), lsn);
        }

        if (distinct.Count == 0)
        {
            return result;
        }

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);

        // One round-trip per chunk rather than per LSN. The chunk size stays well under the
        // 1000-row limit of a table value constructor and the 2100-parameter limit.
        foreach (var chunk in distinct.Values.Chunk(LsnTimeMapChunkSize))
        {
            var tuples = string.Join(",", Enumerable.Range(0, chunk.Length).Select(i => $"(@l{i})"));
            await using var cmd = new SqlCommand(
                $"SELECT v.lsn, sys.fn_cdc_map_lsn_to_time(v.lsn) FROM (VALUES {tuples}) AS v(lsn);", conn);
            for (var i = 0; i < chunk.Length; i++)
            {
                cmd.Parameters.Add($"@l{i}", SqlDbType.Binary, 10).Value = chunk[i];
            }

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (await reader.IsDBNullAsync(1, ct))
                {
                    continue;
                }

                result[Convert.ToHexString((byte[])reader.GetValue(0))] = reader.GetDateTime(1);
            }
        }

        return result;
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

        public string Schema { get; }
        public string Table { get; }
        public string CaptureInstance { get; }
        public IReadOnlyList<string> CapturedColumns { get; }
        public byte[]? Watermark { get; set; }

        /// <summary>Failed polls in a row for this table; reset on the first success.</summary>
        public int ConsecutiveFailures { get; set; }

        /// <summary><see cref="Environment.TickCount64"/> before which this table is not polled again.</summary>
        public long NextAttemptTick { get; set; }
    }
}
