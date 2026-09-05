using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace SqlCdc;

/// <summary>
/// Thresholds for <see cref="SqlCdcHealthCheck"/>.
/// </summary>
public sealed class SqlCdcHealthCheckOptions
{
    /// <summary>
    /// Consecutive failed polls on a single capture instance before the check reports a failure.
    /// Below this, failures are reported as degraded. Defaults to 3.
    /// </summary>
    public int UnhealthyAfterConsecutiveFailures { get; set; } = 3;

    /// <summary>
    /// How stale the last successful poll may get before the check reports degraded. Leave
    /// <c>null</c> to only look at failures, which is enough unless polling can hang without
    /// throwing. A value below a few polling intervals will flap.
    /// </summary>
    public TimeSpan? MaxTimeSinceLastPoll { get; set; }

    /// <summary>
    /// How long an instance may stand by before the check reports degraded. Leave <c>null</c>,
    /// the default, where standbys are expected to wait indefinitely — a replica set with one
    /// leader and the rest idle. Set it where every instance is expected to lead eventually, or
    /// where a lone instance standing by can only mean the lease is held by something else: a
    /// watcher with the same name in another application, or a session that has not been
    /// cleaned up yet.
    /// </summary>
    public TimeSpan? MaxStandbyDuration { get; set; }
}

/// <summary>
/// Reports whether a <see cref="SqlCdcWatcher"/> is actually delivering changes. Without it a
/// watcher that has been retrying a broken capture instance for hours looks identical to a
/// healthy one: the failure only shows up in the logs.
/// </summary>
/// <remarks>
/// A standby instance is healthy, not degraded: it is doing exactly what it should, and failing
/// its probe would take a perfectly good replica out of rotation. A standby that cannot even ask
/// for the lease — the database unreachable, the application lock denied — is another matter: it
/// is reported like a capture instance whose polls keep failing.
/// </remarks>
public sealed class SqlCdcHealthCheck : IHealthCheck
{
    private readonly Func<CdcWatcherStatus> _status;
    private readonly SqlCdcHealthCheckOptions _options;

    public SqlCdcHealthCheck(SqlCdcWatcher watcher, SqlCdcHealthCheckOptions? options = null)
        : this((watcher ?? throw new ArgumentNullException(nameof(watcher))).GetStatus, options)
    {
    }

    internal SqlCdcHealthCheck(Func<CdcWatcherStatus> status, SqlCdcHealthCheckOptions? options)
    {
        _status = status;
        _options = options ?? new SqlCdcHealthCheckOptions();
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var status = _status();
        var data = BuildData(status);

        if (!status.IsRunning)
        {
            return Result(HealthCheckResult.Unhealthy(
                "The CDC watcher is not running; no changes are being delivered.", data: data));
        }

        if (!status.IsLeader)
        {
            if (status.ConsecutiveLeaseFailures >= _options.UnhealthyAfterConsecutiveFailures)
            {
                return Result(HealthCheckResult.Unhealthy(
                    $"The CDC lease cannot be acquired or verified ({status.ConsecutiveLeaseFailures} consecutive " +
                    "failures); no changes are being delivered by this instance.", data: data));
            }

            if (status.ConsecutiveLeaseFailures > 0)
            {
                return Result(HealthCheckResult.Degraded(
                    $"Recent failures acquiring or verifying the CDC lease ({status.ConsecutiveLeaseFailures}).",
                    data: data));
            }

            if (_options.MaxStandbyDuration is { } maxStandby && status.TimeInStandby > maxStandby)
            {
                return Result(HealthCheckResult.Degraded(
                    $"Standing by for {status.TimeInStandby:g}, longer than {maxStandby:g}. Another watcher " +
                    "holds this lease; if this is the only instance, check for a watcher with the same name " +
                    "elsewhere or a lease session that has not been released.", data: data));
            }

            return Result(HealthCheckResult.Healthy(
                "Standing by: another instance holds the CDC lease.", data));
        }

        var failing = status.Tables
            .Where(t => t.ConsecutiveFailures >= _options.UnhealthyAfterConsecutiveFailures)
            .ToList();

        if (failing.Count > 0)
        {
            return Result(HealthCheckResult.Unhealthy(
                $"Polling keeps failing for {Describe(failing)}.", data: data));
        }

        var degraded = status.Tables.Where(t => t.ConsecutiveFailures > 0).ToList();
        if (degraded.Count > 0)
        {
            return Result(HealthCheckResult.Degraded(
                $"Recent polling failures for {Describe(degraded)}.", data: data));
        }

        if (_options.MaxTimeSinceLastPoll is { } maxAge)
        {
            var stale = status.Tables
                .Where(t => t.TimeSinceLastSuccessfulPoll is null || t.TimeSinceLastSuccessfulPoll > maxAge)
                .ToList();

            if (stale.Count > 0)
            {
                return Result(HealthCheckResult.Degraded(
                    $"No successful poll in the last {maxAge} for {Describe(stale)}.", data: data));
            }
        }

        return Result(HealthCheckResult.Healthy("Polling.", data));
    }

    private static Task<HealthCheckResult> Result(HealthCheckResult result) => Task.FromResult(result);

    private static string Describe(IEnumerable<CdcTableStatus> tables) =>
        string.Join(", ", tables.Select(t => t.CaptureInstance));

    private static IReadOnlyDictionary<string, object> BuildData(CdcWatcherStatus status)
    {
        var data = new Dictionary<string, object>
        {
            ["watcher"] = status.Name,
            ["isLeader"] = status.IsLeader,
            ["channelLength"] = status.ChannelLength,
            ["consecutiveLeaseFailures"] = status.ConsecutiveLeaseFailures,
        };

        if (status.TimeInStandby is { } inStandby)
        {
            data["secondsInStandby"] = inStandby.TotalSeconds;
        }

        foreach (var table in status.Tables)
        {
            data[table.CaptureInstance] = new Dictionary<string, object?>
            {
                ["consecutiveFailures"] = table.ConsecutiveFailures,
                ["changesEmitted"] = table.ChangesEmitted,
                ["lastSuccessfulPoll"] = table.LastSuccessfulPoll,
                ["secondsSinceLastPoll"] = table.TimeSinceLastSuccessfulPoll?.TotalSeconds,
            };
        }

        return data;
    }
}
