namespace SqlCdc;

/// <summary>
/// Point-in-time state of one watched capture instance, as reported by
/// <see cref="SqlCdcWatcher.GetStatus"/>.
/// </summary>
/// <param name="CaptureInstance">The CDC capture instance being polled.</param>
/// <param name="SourceSchema">Schema of the source table.</param>
/// <param name="SourceTable">Name of the source table.</param>
/// <param name="ConsecutiveFailures">Failed polls in a row; zero after any success.</param>
/// <param name="LastSuccessfulPoll">When this capture instance was last polled without error.</param>
/// <param name="LastEmittedCommitTime">Commit time of the last change emitted, in SQL Server local time.</param>
/// <param name="ChangesEmitted">Changes written to the channel since the watcher started.</param>
public sealed record CdcTableStatus(
    string CaptureInstance,
    string SourceSchema,
    string SourceTable,
    int ConsecutiveFailures,
    DateTimeOffset? LastSuccessfulPoll,
    DateTime? LastEmittedCommitTime,
    long ChangesEmitted)
{
    /// <summary>Time since the last successful poll, or <c>null</c> if this instance never polled.</summary>
    public TimeSpan? TimeSinceLastSuccessfulPoll =>
        LastSuccessfulPoll is null ? null : DateTimeOffset.UtcNow - LastSuccessfulPoll.Value;
}

/// <summary>
/// Point-in-time state of a <see cref="SqlCdcWatcher"/>. Cheap to build: it reads counters that
/// the polling loop already maintains, so it is safe to call from a health check.
/// </summary>
/// <param name="Name">The watcher's name, used to tell several watchers apart in metrics.</param>
/// <param name="IsRunning">True while the polling loop is alive.</param>
/// <param name="IsLeader">True while this instance holds the lease; always true without one.</param>
/// <param name="ChannelLength">Changes queued on the channel, waiting to be consumed.</param>
/// <param name="Tables">State of each watched capture instance.</param>
public sealed record CdcWatcherStatus(
    string Name,
    bool IsRunning,
    bool IsLeader,
    int ChannelLength,
    IReadOnlyList<CdcTableStatus> Tables)
{
    /// <summary>Highest number of consecutive failures across the watched capture instances.</summary>
    public int MaxConsecutiveFailures => Tables.Count == 0 ? 0 : Tables.Max(t => t.ConsecutiveFailures);
}
