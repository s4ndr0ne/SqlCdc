namespace SqlCdc.Tests;

using System.Reflection;

/// <summary>
/// Exercises the watcher's lease-release lifecycle without a database, using a fake lease provider
/// that records how many times it was asked to release. Guards the exact-once release fix that the
/// poll loop's finally and StopAsync could otherwise both trigger.
/// </summary>
public class WatcherLeaseLifecycleTests
{
    private static CdcWatcherOptions Options() => new()
    {
        ConnectionString = "Server=.;Database=x;Encrypt=True;",
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

    [Fact]
    public async Task DisposeAsync_CanBeCalledRepeatedly_WithoutThrowing()
    {
        var lease = new RecordingLeaseProvider();
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: lease,
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());

        await watcher.DisposeAsync();
        // Second call must be idempotent and not throw ObjectDisposedException
        await watcher.DisposeAsync();
        await watcher.StopAsync();
    }

    [Fact]
    public async Task DisposeAsync_StopsAnAlreadyRunningWatcher()
    {
        var lease = new RecordingLeaseProvider();
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: lease,
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());
        using var pollCts = new CancellationTokenSource();
        var pollTask = Task.Run(async () => await Task.Delay(Timeout.InfiniteTimeSpan, pollCts.Token));
        SetPrivateField(watcher, "_cts", pollCts);
        SetPrivateField(watcher, "_pollTask", pollTask);

        await watcher.DisposeAsync();

        Assert.True(pollCts.IsCancellationRequested);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pollTask);
    }

    [Fact]
    public async Task StartAsync_CancellationWhileWaitingForStop_DoesNotOverReleaseStateLock()
    {
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: new RecordingLeaseProvider(),
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());
        using var pollCts = new CancellationTokenSource();
        pollCts.Cancel();
        SetPrivateField(watcher, "_cts", pollCts);
        var pollTaskSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        SetPrivateField(watcher, "_pollTask", pollTaskSource.Task);
        using var startCts = new CancellationTokenSource();

        try
        {
            var start = watcher.StartAsync(startCts.Token);
            await Task.Delay(50);
            startCts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await start);
            var stateLock = (SemaphoreSlim)typeof(SqlCdcWatcher)
                .GetField("_stateLock", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(watcher)!;
            Assert.Equal(1, stateLock.CurrentCount);
        }
        finally
        {
            pollTaskSource.TrySetResult();
            await watcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task StopAsync_WhenWaitIsCancelled_DoesNotDetachTheRunningPollTask()
    {
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: new RecordingLeaseProvider(),
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());
        using var pollCts = new CancellationTokenSource();
        var pollTaskSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        SetPrivateField(watcher, "_cts", pollCts);
        SetPrivateField(watcher, "_pollTask", pollTaskSource.Task);
        using var stopCts = new CancellationTokenSource();

        var stop = watcher.StopAsync(stopCts.Token);
        await Task.Delay(50);
        stopCts.Cancel();

        await stop;

        Assert.False(pollTaskSource.Task.IsCompleted);
        Assert.Same(pollTaskSource.Task, GetPrivateField<Task>(watcher, "_pollTask"));

        pollTaskSource.TrySetResult();
        await watcher.DisposeAsync();
    }

    [Fact]
    public async Task StartAsync_AfterFailedStartup_DoesNotRemainStarting()
    {
        await using var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: new RecordingLeaseProvider(),
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());

        await Assert.ThrowsAsync<NotSupportedException>(() => watcher.StartAsync());
        await Assert.ThrowsAsync<NotSupportedException>(() => watcher.StartAsync());
        Assert.False(watcher.IsRunning);
    }

    [Fact]
    public async Task CompletedPollLoop_ReturnsLifecycleToIdle()
    {
        await using var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: new RecordingLeaseProvider(),
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());
        var lifecycle = GetPrivateField<WatcherLifecycleCoordinator>(watcher, "_lifecycle");
        Assert.True(lifecycle.TryBeginStart());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var runLoop = (Task)typeof(SqlCdcWatcher).GetMethod(
            "RunLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(watcher, [cancellation.Token])!;
        await runLoop;

        Assert.False(watcher.IsRunning);
        await Assert.ThrowsAsync<NotSupportedException>(() => watcher.StartAsync());
    }

    [Fact]
    public async Task LeaseVerificationFailure_ReleasesThePossiblyHeldLease()
    {
        var lease = new RecordingLeaseProvider { ThrowOnIsHeld = true };
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: lease,
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());
        SetPrivateField(watcher, "_isLeader", true);
        SetPrivateField(watcher, "_lastLeaseCheckTick", Environment.TickCount64 - 1000);

        var verify = typeof(SqlCdcWatcher).GetMethod(
            "VerifyLeaseAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var result = (Task<bool>)verify.Invoke(watcher, [CancellationToken.None])!;

        Assert.False(await result);
        Assert.Equal(1, lease.ReleaseAttempts);

        await watcher.DisposeAsync();
    }

    [Fact]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var lease = new RecordingLeaseProvider();
        var watcher = new SqlCdcWatcher(
            Options(),
            new InMemoryCdcStateStore(),
            logger: null,
            leaseProvider: lease,
            ownsLeaseProvider: false,
            connections: new UnitTestConnectionFactory());

        await watcher.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => watcher.StartAsync());
    }

    /// <summary>Counts ReleaseAsync calls so the test can assert on exactly-once behaviour.</summary>
    private sealed class RecordingLeaseProvider : ICdcLeaseProvider
    {
        public int ReleaseAttempts;
        public bool ThrowOnIsHeld;

        public Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public Task<bool> IsHeldAsync(CancellationToken cancellationToken = default)
        {
            if (ThrowOnIsHeld)
            {
                throw new InvalidOperationException("lease probe failed");
            }

            return Task.FromResult(true);
        }

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

    private static void SetPrivateField<T>(SqlCdcWatcher watcher, string name, T value) =>
        typeof(SqlCdcWatcher).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(watcher, value);

    private static T GetPrivateField<T>(SqlCdcWatcher watcher, string name) =>
        (T)typeof(SqlCdcWatcher).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(watcher)!;
}
