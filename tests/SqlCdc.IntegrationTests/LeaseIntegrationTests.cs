namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class LeaseIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public LeaseIntegrationTests(SqlServerFixture sql) => _sql = sql;

    [Fact]
    public async Task OnlyOneProvider_HoldsTheLeaseAtATime()
    {
        await using var first = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "exclusive");
        await using var second = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "exclusive");

        Assert.True(await first.TryAcquireAsync());
        Assert.False(await second.TryAcquireAsync());

        Assert.True(await first.IsHeldAsync());
        Assert.False(await second.IsHeldAsync());

        // Acquiring again from the holder is idempotent, not a second lock.
        Assert.True(await first.TryAcquireAsync());
    }

    [Fact]
    public async Task ReleasedLease_IsTakenByTheStandby()
    {
        await using var first = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "handover");
        await using var second = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "handover");

        Assert.True(await first.TryAcquireAsync());
        Assert.False(await second.TryAcquireAsync());

        await first.ReleaseAsync();

        Assert.False(await first.IsHeldAsync());
        Assert.True(await second.TryAcquireAsync());
        Assert.True(await second.IsHeldAsync());
    }

    [Fact]
    public async Task DifferentLeaseNames_DoNotContend()
    {
        await using var orders = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "orders");
        await using var customers = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "customers");

        Assert.True(await orders.TryAcquireAsync());
        Assert.True(await customers.TryAcquireAsync());
    }

    [Fact]
    public async Task DisposingTheHolder_ReleasesTheLease()
    {
        var first = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "disposed");
        await using var second = new SqlApplicationLockLeaseProvider(_sql.ConnectionString, "disposed");

        Assert.True(await first.TryAcquireAsync());
        Assert.False(await second.TryAcquireAsync());

        await first.DisposeAsync();

        Assert.True(await second.TryAcquireAsync());
    }

    /// <summary>
    /// The reason the lease exists: two instances of the same application must not both poll, and
    /// the standby must resume from the watermark the active instance persisted rather than
    /// replaying everything it already emitted.
    /// </summary>
    [Fact]
    public async Task StandbyWatcher_StaysIdle_AndTakesOverWhereTheLeaderStopped()
    {
        const string table = "Orders_Lease";
        var captureInstance = await _sql.CreateCdcTableAsync(table);
        var store = new SqlCdcStateStore(_sql.ConnectionString);

        await using var active = BuildWatcher(table, store);
        await using var standby = BuildWatcher(table, store);

        await active.StartAsync();
        Assert.True(await Wait.UntilAsync(() => active.IsLeader), "the first watcher never became the leader");

        await standby.StartAsync();
        Assert.False(
            await Wait.UntilAsync(() => standby.IsLeader, TimeSpan.FromSeconds(3)),
            "the second watcher took the lease while the first one still held it");

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

        var fromLeader = await ChangeCollector.CollectAsync(active, 1);
        Assert.Equal(1, Assert.Single(fromLeader).After["Id"]);
        Assert.Equal(0, standby.Channel.Reader.Count);

        // Reading the change off the channel only proves it was delivered; the watermark is saved
        // right after. Stopping in between is a legitimate at-least-once replay, so wait for the
        // checkpoint before failing over — otherwise this asserts on a race, not on the handover.
        Assert.True(
            await Wait.UntilAsync(async () => await store.GetLastLsnAsync(captureInstance) is not null),
            "the leader never checkpointed the change it delivered");

        // The leader goes away: the standby picks the lease up and continues from the persisted
        // watermark, so the change the leader already delivered is not emitted a second time.
        await active.StopAsync();
        Assert.True(await Wait.UntilAsync(() => standby.IsLeader), "the standby never took over");

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (2, N'second');");

        var fromStandby = await ChangeCollector.CollectAsync(standby, 1);
        Assert.Equal(2, Assert.Single(fromStandby).After["Id"]);
    }

    private SqlCdcWatcher BuildWatcher(string table, ICdcStateStore store) => SqlCdcWatcherBuilder
        .Create()
        .UseConnectionString(_sql.ConnectionString)
        .WatchTable("dbo", table)
        .StartFrom(CdcStartMode.FromBeginning)
        .UseStateStore(store)
        .UseSingleActiveInstance("failover")
        .WithPollInterval(TimeSpan.FromMilliseconds(200))
        .WithLeaseRetryDelay(TimeSpan.FromMilliseconds(500))
        .Build();
}
