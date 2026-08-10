using Microsoft.Extensions.Logging;

namespace SqlCdc;

/// <summary>
/// Fluent builder for <see cref="SqlCdcWatcher"/>.
/// </summary>
public sealed class SqlCdcWatcherBuilder
{
    private readonly List<CdcTableSubscription> _tables = new();
    private string? _connectionString;
    private ICdcStateStore? _stateStore;
    private ILogger? _logger;
    private TimeSpan _pollInterval = TimeSpan.FromMilliseconds(500);
    private int _batchSize = 1000;
    private int _channelCapacity = 100_000;
    private CdcStartMode _startMode = CdcStartMode.FromNow;
    private TimeSpan _retryDelay = TimeSpan.FromSeconds(5);

    private SqlCdcWatcherBuilder()
    {
    }

    public static SqlCdcWatcherBuilder Create() => new();

    public SqlCdcWatcherBuilder UseConnectionString(string connectionString)
    {
        _connectionString = connectionString;
        return this;
    }

    public SqlCdcWatcherBuilder WatchTable(string schema, string table, string? captureInstance = null)
    {
        _tables.Add(new CdcTableSubscription(schema, table, captureInstance));
        return this;
    }

    public SqlCdcWatcherBuilder WatchTables(params (string Schema, string Table)[] tables)
    {
        foreach (var (schema, table) in tables)
        {
            _tables.Add(new CdcTableSubscription(schema, table));
        }

        return this;
    }

    public SqlCdcWatcherBuilder UseStateStore(ICdcStateStore stateStore)
    {
        _stateStore = stateStore;
        return this;
    }

    public SqlCdcWatcherBuilder UseLogger(ILogger logger)
    {
        _logger = logger;
        return this;
    }

    public SqlCdcWatcherBuilder WithPollInterval(TimeSpan pollInterval)
    {
        if (pollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval), "Poll interval must be positive.");
        }

        _pollInterval = pollInterval;
        return this;
    }

    public SqlCdcWatcherBuilder WithBatchSize(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be positive.");
        }

        _batchSize = batchSize;
        return this;
    }

    public SqlCdcWatcherBuilder WithChannelCapacity(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "Channel capacity must be positive.");
        }

        _channelCapacity = capacity;
        return this;
    }

    public SqlCdcWatcherBuilder StartFrom(CdcStartMode startMode)
    {
        _startMode = startMode;
        return this;
    }

    public SqlCdcWatcherBuilder WithRetryDelay(TimeSpan retryDelay)
    {
        if (retryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelay), "Retry delay must be positive.");
        }

        _retryDelay = retryDelay;
        return this;
    }

    public SqlCdcWatcher Build()
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException("A connection string is required. Call UseConnectionString(...).");
        }

        if (_tables.Count == 0)
        {
            throw new InvalidOperationException("At least one table must be watched. Call WatchTable(...).");
        }

        var options = new CdcWatcherOptions
        {
            ConnectionString = _connectionString,
            Tables = _tables,
            PollInterval = _pollInterval,
            BatchSize = _batchSize,
            ChannelCapacity = _channelCapacity,
            StartMode = _startMode,
            RetryDelay = _retryDelay,
        };

        return new SqlCdcWatcher(options, _stateStore ?? new InMemoryCdcStateStore(), _logger);
    }
}
