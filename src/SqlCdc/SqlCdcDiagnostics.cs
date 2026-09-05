using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace SqlCdc;

/// <summary>
/// The <see cref="Meter"/> and <see cref="ActivitySource"/> published by the package. Subscribe to
/// them by name, for example with OpenTelemetry:
/// <code>
/// .WithMetrics(m => m.AddMeter(SqlCdcDiagnostics.MeterName))
/// .WithTracing(t => t.AddSource(SqlCdcDiagnostics.ActivitySourceName))
/// </code>
/// </summary>
public static class SqlCdcDiagnostics
{
    /// <summary>Name of the meter carrying every metric listed on this type.</summary>
    public const string MeterName = "SqlCdc";

    /// <summary>Name of the activity source carrying the poll and handler spans.</summary>
    public const string ActivitySourceName = "SqlCdc";

    /// <summary>Changes written to the channel. Tagged by watcher, capture instance and operation.</summary>
    public const string ChangesEmittedMetric = "sqlcdc.changes.emitted";

    /// <summary>
    /// Seconds between the commit of a change and its emission — the end-to-end freshness of the
    /// pipeline. Measured against the SQL Server clock, so it holds whatever time zone the server
    /// runs in.
    /// </summary>
    public const string ChangeLagMetric = "sqlcdc.change.lag";

    /// <summary>Rows read per batch, to see whether the batch size is the limiting factor.</summary>
    public const string BatchRowsMetric = "sqlcdc.batch.rows";

    /// <summary>Seconds spent polling one capture instance.</summary>
    public const string PollDurationMetric = "sqlcdc.poll.duration";

    /// <summary>Failed polls. A rising count with a flat <see cref="ChangesEmittedMetric"/> is the alert.</summary>
    public const string PollFailuresMetric = "sqlcdc.poll.failures";

    /// <summary>Seconds spent in a handler, tagged by handler and outcome.</summary>
    public const string HandlerDurationMetric = "sqlcdc.handler.duration";

    /// <summary>Handler attempts that threw, including the ones that were retried.</summary>
    public const string HandlerFailuresMetric = "sqlcdc.handler.failures";

    /// <summary>Changes that exhausted their attempts and were dead-lettered or dropped.</summary>
    public const string DeadLettersMetric = "sqlcdc.dead_letters";

    /// <summary>
    /// No longer emitted. A CDC row with an unsupported <c>__$operation</c> value, or an update
    /// image without its counterpart, now fails the poll of its capture instance instead of being
    /// skipped, so it shows up in <see cref="PollFailuresMetric"/> and the health check.
    /// </summary>
    [Obsolete("No longer emitted: unsupported rows fail the poll and count in " + PollFailuresMetric + ".")]
    public const string SkippedRowsMetric = "sqlcdc.skipped.rows";

    /// <summary>Changes currently queued on the channel: the consumer's backlog.</summary>
    public const string ChannelLengthMetric = "sqlcdc.channel.length";

    /// <summary>1 while the watcher holds the lease and polls, 0 while it stands by.</summary>
    public const string LeaderMetric = "sqlcdc.leader";

    /// <summary>
    /// Attempts to acquire or verify the lease that failed with an error. A standby waiting its
    /// turn does not count; a rising count means the instance cannot reach the database at all.
    /// </summary>
    public const string LeaseFailuresMetric = "sqlcdc.lease.failures";

    private static readonly string? Version = typeof(SqlCdcDiagnostics).Assembly.GetName().Version?.ToString();

    private static readonly Meter Meter = new(MeterName, Version);

    /// <summary>
    /// Live watchers, so the observable gauges can be created once on a static meter instead of
    /// one instrument per watcher. Watchers add themselves when built and drop out when disposed.
    /// Held weakly: a watcher that is never disposed must not be pinned forever by this static
    /// registry — once collected it simply drops out of the gauges.
    /// </summary>
    private static readonly ConditionalWeakTable<SqlCdcWatcher, object?> Watchers = new();

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName, Version);

    internal static readonly Counter<long> ChangesEmitted = Meter.CreateCounter<long>(
        ChangesEmittedMetric, "{change}", "Change events written to the channel.");

    internal static readonly Histogram<double> ChangeLag = Meter.CreateHistogram<double>(
        ChangeLagMetric, "s", "Seconds between the commit of a change and its emission.");

    internal static readonly Histogram<int> BatchRows = Meter.CreateHistogram<int>(
        BatchRowsMetric, "{row}", "CDC rows read in one batch.");

    internal static readonly Histogram<double> PollDuration = Meter.CreateHistogram<double>(
        PollDurationMetric, "s", "Seconds spent polling one capture instance.");

    internal static readonly Counter<long> PollFailures = Meter.CreateCounter<long>(
        PollFailuresMetric, "{failure}", "Polls that failed.");

    internal static readonly Histogram<double> HandlerDuration = Meter.CreateHistogram<double>(
        HandlerDurationMetric, "s", "Seconds spent in a change handler.");

    internal static readonly Counter<long> HandlerFailures = Meter.CreateCounter<long>(
        HandlerFailuresMetric, "{failure}", "Handler attempts that threw.");

    internal static readonly Counter<long> DeadLetters = Meter.CreateCounter<long>(
        DeadLettersMetric, "{change}", "Changes that exhausted their attempts.");

    internal static readonly Counter<long> LeaseFailures = Meter.CreateCounter<long>(
        LeaseFailuresMetric, "{failure}", "Attempts to acquire or verify the lease that failed with an error.");

    private static readonly ObservableGauge<int> ChannelLength = Meter.CreateObservableGauge(
        ChannelLengthMetric, ObserveChannelLength, "{change}", "Changes queued on the channel.");

    private static readonly ObservableGauge<int> Leader = Meter.CreateObservableGauge(
        LeaderMetric, ObserveLeader, description: "1 when this instance holds the lease, 0 when it stands by.");

    internal static void Register(SqlCdcWatcher watcher) => Watchers.AddOrUpdate(watcher, null);

    internal static void Unregister(SqlCdcWatcher watcher) => Watchers.Remove(watcher);

    private static IEnumerable<Measurement<int>> ObserveChannelLength() =>
        Watchers.Select(entry => new Measurement<int>(entry.Key.Channel.Reader.Count, WatcherTag(entry.Key)));

    private static IEnumerable<Measurement<int>> ObserveLeader() =>
        Watchers.Select(entry => new Measurement<int>(entry.Key.IsLeader ? 1 : 0, WatcherTag(entry.Key)));

    private static KeyValuePair<string, object?> WatcherTag(SqlCdcWatcher watcher) =>
        new("watcher", watcher.Name);
}
