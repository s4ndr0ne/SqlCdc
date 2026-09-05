namespace SqlCdc.IntegrationTests;

/// <summary>Reads events off a running watcher with a deadline, so a missing event fails fast.</summary>
internal static class ChangeCollector
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Collects changes until <paramref name="until"/> matches one, or the timeout expires.</summary>
    public static async Task<List<CdcChange>> CollectUntilAsync(
        SqlCdcWatcher watcher,
        Func<CdcChange, bool> until,
        TimeSpan? timeout = null)
    {
        var collected = new List<CdcChange>();
        using var cts = new CancellationTokenSource(timeout ?? DefaultTimeout);

        try
        {
            await foreach (var change in watcher.Changes.WithCancellation(cts.Token))
            {
                collected.Add(change);
                change.Acknowledge();
                if (until(change))
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Returned as-is: the assertions describe what was missing better than a timeout would.
        }

        return collected;
    }

    /// <summary>Collects exactly <paramref name="count"/> changes, or fewer on timeout.</summary>
    public static Task<List<CdcChange>> CollectAsync(SqlCdcWatcher watcher, int count, TimeSpan? timeout = null)
    {
        var seen = 0;
        return CollectUntilAsync(watcher, _ => ++seen >= count, timeout);
    }
}
