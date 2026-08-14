namespace SqlCdc;

/// <summary>
/// Describes a table to watch through its CDC capture instance.
/// </summary>
public sealed record CdcTableSubscription(
    string Schema,
    string Table,
    string? CaptureInstance = null);

/// <summary>
/// Where to start processing when no stored watermark LSN exists yet.
/// </summary>
public enum CdcStartMode
{
    /// <summary>Start from the current point in the log; historical changes are skipped.</summary>
    FromNow,

    /// <summary>Start from the earliest available LSN of the capture instance.</summary>
    FromBeginning,
}

/// <summary>
/// Configuration for <see cref="SqlCdcWatcher"/>.
/// </summary>
public sealed class CdcWatcherOptions
{
    /// <summary>Connection string to the SQL Server database where CDC is enabled.</summary>
    public required string ConnectionString { get; set; }

    /// <summary>Tables to watch.</summary>
    public required IReadOnlyList<CdcTableSubscription> Tables { get; set; }

    /// <summary>Delay between polling cycles. Defaults to 500 ms.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Maximum number of changes pulled per table in a single cycle. Defaults to 1000.</summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>Capacity of the bounded channel used to deliver events. Defaults to 100_000.</summary>
    public int ChannelCapacity { get; set; } = 100_000;

    /// <summary>Start mode used when no watermark LSN has been persisted yet. Defaults to <see cref="CdcStartMode.FromNow"/>.</summary>
    public CdcStartMode StartMode { get; set; } = CdcStartMode.FromNow;

    /// <summary>Delay before retrying after an error. Defaults to 5 seconds.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Timeout for each SQL round-trip against the CDC database. Defaults to 30 seconds.</summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// When the watermark LSN is persisted. Defaults to <see cref="CdcCheckpointMode.OnEmit"/>.
    /// </summary>
    public CdcCheckpointMode CheckpointMode { get; set; } = CdcCheckpointMode.OnEmit;

    /// <summary>
    /// Delay before trying the lease again while another instance holds it. Defaults to 10 seconds,
    /// which is also how long a standby instance takes to pick up a released lease.
    /// </summary>
    public TimeSpan LeaseRetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Name that identifies this watcher in metrics and health check data. Defaults to
    /// <c>default</c>; give each watcher its own name when an application runs more than one.
    /// </summary>
    public string Name { get; set; } = "default";

    /// <summary>
    /// How many times a handler is called for the same change before it is dead-lettered.
    /// Defaults to 1, which is a single attempt and no retry.
    /// </summary>
    public int MaxHandlerAttempts { get; set; } = 1;

    /// <summary>
    /// Delay before the second handler attempt; it doubles with each further attempt, capped at
    /// one minute. Defaults to 1 second. Only used when <see cref="MaxHandlerAttempts"/> is above 1.
    /// </summary>
    public TimeSpan HandlerRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
}
