using Microsoft.Data.SqlClient;
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
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);
    private CdcCheckpointMode _checkpointMode = CdcCheckpointMode.OnEmit;
    private TimeSpan _leaseRetryDelay = TimeSpan.FromSeconds(10);
    private TimeSpan _leaseKeepaliveInterval = TimeSpan.FromSeconds(10);
    private ICdcLeaseProvider? _leaseProvider;
    private string? _singleActiveInstanceLeaseName;
    private ICdcConnectionFactory? _connectionFactory;
    private Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>>? _accessTokenCallback;
    private string _name = "default";
    private int _maxHandlerAttempts = 1;
    private TimeSpan _handlerRetryDelay = TimeSpan.FromSeconds(1);

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

    public SqlCdcWatcherBuilder WithCommandTimeout(TimeSpan commandTimeout)
    {
        if (commandTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(commandTimeout), "Command timeout must be positive.");
        }

        _commandTimeout = commandTimeout;
        return this;
    }

    /// <summary>
    /// Chooses when the watermark LSN is persisted. Use
    /// <see cref="CdcCheckpointMode.OnAcknowledgement"/> for at-least-once delivery end to end.
    /// </summary>
    public SqlCdcWatcherBuilder WithCheckpointMode(CdcCheckpointMode checkpointMode)
    {
        _checkpointMode = checkpointMode;
        return this;
    }

    /// <summary>
    /// Makes only one instance poll at a time, electing a leader through a SQL Server application
    /// lock on the watched database. Required whenever the application runs with more than one
    /// replica: without it every replica emits the same changes and fights over the watermark.
    /// </summary>
    /// <param name="leaseName">
    /// Name shared by the instances that elect a leader between them. Use distinct names to run
    /// independent watchers (different table sets) against the same database.
    /// </param>
    public SqlCdcWatcherBuilder UseSingleActiveInstance(
        string leaseName = SqlApplicationLockLeaseProvider.DefaultLeaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseName);

        // Built in Build(), where the connection string is known regardless of call order.
        _singleActiveInstanceLeaseName = leaseName;
        _leaseProvider = null;
        return this;
    }

    /// <summary>
    /// Uses a custom leader election implementation. The caller keeps ownership: the watcher
    /// releases the lease when it stops but does not dispose the provider.
    /// </summary>
    public SqlCdcWatcherBuilder UseLeaseProvider(ICdcLeaseProvider leaseProvider)
    {
        ArgumentNullException.ThrowIfNull(leaseProvider);

        _leaseProvider = leaseProvider;
        _singleActiveInstanceLeaseName = null;
        return this;
    }

    /// <summary>Delay before retrying the lease while another instance holds it.</summary>
    public SqlCdcWatcherBuilder WithLeaseRetryDelay(TimeSpan leaseRetryDelay)
    {
        if (leaseRetryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(leaseRetryDelay), "Lease retry delay must be positive.");
        }

        _leaseRetryDelay = leaseRetryDelay;
        return this;
    }

    /// <summary>
    /// How often the active instance verifies it still holds the lease. Between checks it keeps
    /// polling on the assumption the lease is held, so this bounds both the keepalive traffic and
    /// how long a lost lease can go unnoticed. Defaults to 10 seconds.
    /// </summary>
    public SqlCdcWatcherBuilder WithLeaseKeepaliveInterval(TimeSpan leaseKeepaliveInterval)
    {
        if (leaseKeepaliveInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaseKeepaliveInterval), "Lease keepalive interval must be positive.");
        }

        _leaseKeepaliveInterval = leaseKeepaliveInterval;
        return this;
    }

    /// <summary>
    /// Names this watcher, which is how it is told apart from others in metrics and health data.
    /// </summary>
    public SqlCdcWatcherBuilder WithName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        _name = name;
        return this;
    }

    /// <summary>
    /// Retries a failing handler before the change is dead-lettered. The delay doubles with each
    /// attempt, capped at one minute. By default a handler is called once and a failure drops the
    /// change, so a transient error in a handler loses an event.
    /// </summary>
    /// <param name="maxAttempts">Total calls per handler and change, including the first.</param>
    /// <param name="retryDelay">Delay before the second attempt. Defaults to 1 second.</param>
    public SqlCdcWatcherBuilder WithHandlerRetry(int maxAttempts, TimeSpan? retryDelay = null)
    {
        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), "There must be at least one attempt.");
        }

        if (retryDelay is { } delay && delay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelay), "Retry delay must be positive.");
        }

        _maxHandlerAttempts = maxAttempts;
        _handlerRetryDelay = retryDelay ?? _handlerRetryDelay;
        return this;
    }

    /// <summary>
    /// Opens every connection — CDC reads, watermarks and the lease — through the given factory
    /// instead of a plain connection string. This is the hook for a configured
    /// <c>TokenCredential</c>, a <c>SqlRetryLogicBaseProvider</c>, or any other per-connection
    /// setup; a connection string is then no longer required.
    /// </summary>
    public SqlCdcWatcherBuilder UseConnectionFactory(ICdcConnectionFactory connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);

        _connectionFactory = connectionFactory;
        return this;
    }

    /// <inheritdoc cref="UseConnectionFactory(ICdcConnectionFactory)"/>
    /// <param name="openConnection">
    /// Returns a connection to the CDC database, ideally already open. Called once per operation,
    /// so it must hand out a new connection each time rather than share one.
    /// </param>
    public SqlCdcWatcherBuilder UseConnectionFactory(Func<CancellationToken, Task<SqlConnection>> openConnection)
    {
        ArgumentNullException.ThrowIfNull(openConnection);

        _connectionFactory = new DelegateCdcConnectionFactory(openConnection);
        return this;
    }

    /// <summary>
    /// Acquires an access token per connection, for Entra ID authentication driven by the
    /// application — a configured <c>TokenCredential</c>, for instance. Not needed when the
    /// connection string already says <c>Authentication=Active Directory ...</c>, which
    /// Microsoft.Data.SqlClient handles on its own.
    /// </summary>
    public SqlCdcWatcherBuilder UseAccessTokenCallback(
        Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>> accessTokenCallback)
    {
        ArgumentNullException.ThrowIfNull(accessTokenCallback);

        _accessTokenCallback = accessTokenCallback;
        return this;
    }

    public SqlCdcWatcher Build()
    {
        if (string.IsNullOrWhiteSpace(_connectionString) && _connectionFactory is null)
        {
            throw new InvalidOperationException(
                "A connection string is required. Call UseConnectionString(...), or supply a connection " +
                "factory with UseConnectionFactory(...).");
        }

        if (_accessTokenCallback is not null && _connectionFactory is not null)
        {
            throw new InvalidOperationException(
                "UseAccessTokenCallback(...) configures the built-in connection factory and cannot be combined " +
                "with UseConnectionFactory(...). Set the callback on the connections your factory returns.");
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
            CommandTimeout = _commandTimeout,
            CheckpointMode = _checkpointMode,
            LeaseRetryDelay = _leaseRetryDelay,
            LeaseKeepaliveInterval = _leaseKeepaliveInterval,
            Name = _name,
            MaxHandlerAttempts = _maxHandlerAttempts,
            HandlerRetryDelay = _handlerRetryDelay,
        };

        var connections = _connectionFactory
            ?? new SqlCdcConnectionFactory(_connectionString!, _accessTokenCallback);

        var leaseProvider = _leaseProvider;
        var ownsLeaseProvider = false;
        if (_singleActiveInstanceLeaseName is not null)
        {
            leaseProvider = new SqlApplicationLockLeaseProvider(
                connections, _singleActiveInstanceLeaseName, _logger);
            ownsLeaseProvider = true;
        }

        return new SqlCdcWatcher(
            options,
            _stateStore ?? new InMemoryCdcStateStore(),
            _logger,
            leaseProvider,
            ownsLeaseProvider,
            connections);
    }
}
