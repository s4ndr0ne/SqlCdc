namespace SqlCdc.Tests;

/// <summary>
/// Exercises the watcher's lease-release lifecycle without a database, using a fake lease provider
/// that records how many times it was asked to release. Guards the exact-once release fix that the
/// poll loop's finally and StopAsync could otherwise both trigger.
/// </summary>
public class WatcherLeaseLifecycleTests
{
    private static CdcWatcherOptions Options() => new()
    {
        ConnectionString = "Server=.;Database=x",
        Tables = [new CdcTableSubscription("dbo", "Orders")],
        PollInterval = TimeSpan.FromMilliseconds(50),
        LeaseRetryDelay = TimeSpan.FromMilliseconds(10),
        LeaseKeepaliveInterval = TimeSpan.FromMilliseconds(10),
    };

    [Fact]
    public async Task StopAsync_OnANeverStartedWatcher_DoesNotReleaseAFileItNeverHeld()
    {
        var lease = new RecordingLeaseProvider();
        await using var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: lease,
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());

        Assert.False(watcher.IsLeader);
        await watcher.StopAsync();

        Assert.Equal(0, lease.ReleaseAttempts);
    }

    /// <summary>Counts ReleaseAsync calls so the test can assert on exactly-once behaviour.</summary>
    private sealed class RecordingLeaseProvider : ICdcLeaseProvider
    {
        public int ReleaseAttempts;

        public Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public Task<bool> IsHeldAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public Task ReleaseAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref ReleaseAttempts);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Never opens a real connection; CaptureInstance resolution is not exercised here.</summary>
    private sealed class UnitTestConnectionFactory : ICdcConnectionFactory
    {
        public Task<Microsoft.Data.SqlClient.SqlConnection> OpenConnectionAsync(
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("This test never opens a connection.");
    }
}
