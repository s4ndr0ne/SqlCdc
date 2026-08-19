namespace SqlCdc;

/// <summary>
/// The shape of the <c>SqlCdc</c> configuration section, bound by
/// <c>AddSqlCdc(IConfiguration)</c>.
/// </summary>
/// <remarks>
/// Every setting except <see cref="Tables"/> is nullable on purpose: a value that is absent from
/// configuration leaves the corresponding default (or whatever the code already set) alone,
/// rather than resetting it.
/// </remarks>
public sealed class SqlCdcConfiguration
{
    /// <summary>Connection string to the CDC-enabled database.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Name of the watcher in metrics and health data.</summary>
    public string? Name { get; set; }

    /// <summary>Tables to watch.</summary>
    public IList<CdcTableConfiguration> Tables { get; set; } = new List<CdcTableConfiguration>();

    /// <summary>Delay between polling cycles, for example <c>00:00:00.500</c>.</summary>
    public TimeSpan? PollInterval { get; set; }

    /// <summary>Maximum rows pulled per table in a single cycle.</summary>
    public int? BatchSize { get; set; }

    /// <summary>Capacity of the bounded channel.</summary>
    public int? ChannelCapacity { get; set; }

    /// <summary><c>FromNow</c> or <c>FromBeginning</c>.</summary>
    public CdcStartMode? StartMode { get; set; }

    /// <summary>Delay after a polling error.</summary>
    public TimeSpan? RetryDelay { get; set; }

    /// <summary>Timeout for each SQL round-trip.</summary>
    public TimeSpan? CommandTimeout { get; set; }

    /// <summary><c>OnEmit</c> or <c>OnAcknowledgement</c>.</summary>
    public CdcCheckpointMode? CheckpointMode { get; set; }

    /// <summary>How often a standby instance retries the lease.</summary>
    public TimeSpan? LeaseRetryDelay { get; set; }

    /// <summary>How often the active instance verifies it still holds the lease.</summary>
    public TimeSpan? LeaseKeepaliveInterval { get; set; }

    /// <summary>
    /// Elects a single active watcher across replicas. Also turned on by setting
    /// <see cref="LeaseName"/> alone.
    /// </summary>
    public bool? SingleActiveInstance { get; set; }

    /// <summary>Lease name shared by the instances that elect a leader between them.</summary>
    public string? LeaseName { get; set; }

    /// <summary>Attempts per handler before a change is dead-lettered.</summary>
    public int? MaxHandlerAttempts { get; set; }

    /// <summary>Delay before the second handler attempt.</summary>
    public TimeSpan? HandlerRetryDelay { get; set; }

    /// <summary>
    /// Applies everything that was actually configured to a builder. Nothing is validated here:
    /// configuration and code are meant to be mixed, so a setting missing from one may well be
    /// supplied by the other. <see cref="SqlCdcWatcherBuilder.Build"/> has the final say.
    /// </summary>
    public void ApplyTo(SqlCdcWatcherBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            builder.UseConnectionString(ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(Name))
        {
            builder.WithName(Name);
        }

        foreach (var table in Tables)
        {
            if (string.IsNullOrWhiteSpace(table.Table))
            {
                throw new InvalidOperationException(
                    "A watched table is missing its 'Table' name in the SqlCdc configuration section.");
            }

            builder.WatchTable(table.Schema, table.Table, table.CaptureInstance);
        }

        if (PollInterval is { } pollInterval)
        {
            builder.WithPollInterval(pollInterval);
        }

        if (BatchSize is { } batchSize)
        {
            builder.WithBatchSize(batchSize);
        }

        if (ChannelCapacity is { } channelCapacity)
        {
            builder.WithChannelCapacity(channelCapacity);
        }

        if (StartMode is { } startMode)
        {
            builder.StartFrom(startMode);
        }

        if (RetryDelay is { } retryDelay)
        {
            builder.WithRetryDelay(retryDelay);
        }

        if (CommandTimeout is { } commandTimeout)
        {
            builder.WithCommandTimeout(commandTimeout);
        }

        if (CheckpointMode is { } checkpointMode)
        {
            builder.WithCheckpointMode(checkpointMode);
        }

        if (LeaseRetryDelay is { } leaseRetryDelay)
        {
            builder.WithLeaseRetryDelay(leaseRetryDelay);
        }

        if (LeaseKeepaliveInterval is { } leaseKeepaliveInterval)
        {
            builder.WithLeaseKeepaliveInterval(leaseKeepaliveInterval);
        }

        if (SingleActiveInstance == true || !string.IsNullOrWhiteSpace(LeaseName))
        {
            builder.UseSingleActiveInstance(
                string.IsNullOrWhiteSpace(LeaseName)
                    ? SqlApplicationLockLeaseProvider.DefaultLeaseName
                    : LeaseName);
        }

        if (MaxHandlerAttempts is { } maxHandlerAttempts)
        {
            builder.WithHandlerRetry(maxHandlerAttempts, HandlerRetryDelay);
        }
        else if (HandlerRetryDelay is { } handlerRetryDelay)
        {
            builder.WithHandlerRetry(1, handlerRetryDelay);
        }
    }
}

/// <summary>One watched table inside the <c>SqlCdc</c> configuration section.</summary>
public sealed class CdcTableConfiguration
{
    /// <summary>Schema of the source table. Defaults to <c>dbo</c>.</summary>
    public string Schema { get; set; } = "dbo";

    /// <summary>Name of the source table.</summary>
    public string? Table { get; set; }

    /// <summary>Capture instance to read, when the table has more than the default one.</summary>
    public string? CaptureInstance { get; set; }
}
