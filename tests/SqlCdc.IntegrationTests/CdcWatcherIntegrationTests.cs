using Microsoft.Extensions.Logging;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class CdcWatcherIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public CdcWatcherIntegrationTests(SqlServerFixture sql) => _sql = sql;

    private SqlCdcWatcher BuildWatcher(
        string[] tables,
        ILogger? logger = null,
        ICdcStateStore? stateStore = null,
        int batchSize = 1000)
    {
        var builder = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString(_sql.ConnectionString)
            // Every test owns a fresh table, so "from beginning" makes the expected event set exact
            // and removes any race between starting the watcher and writing the first row.
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .WithRetryDelay(TimeSpan.FromSeconds(1))
            .WithBatchSize(batchSize);

        foreach (var table in tables)
        {
            builder.WatchTable("dbo", table);
        }

        if (logger is not null)
        {
            builder.UseLogger(logger);
        }

        if (stateStore is not null)
        {
            builder.UseStateStore(stateStore);
        }

        return builder.Build();
    }

    [Fact]
    public async Task InsertUpdateDelete_AreCapturedWithBeforeAndAfterImages()
    {
        const string table = "Orders_Basic";
        await _sql.CreateCdcTableAsync(table);

        await using var watcher = BuildWatcher([table]);
        await watcher.StartAsync();

        await _sql.ExecuteAsync(
            $"""
             INSERT INTO dbo.[{table}] (Id, Name, Price) VALUES (1, N'Widget', 9.99);
             UPDATE dbo.[{table}] SET Price = 19.99 WHERE Id = 1;
             DELETE FROM dbo.[{table}] WHERE Id = 1;
             """);

        var changes = await ChangeCollector.CollectAsync(watcher, 3);

        Assert.Equal(3, changes.Count);

        var insert = changes[0];
        Assert.Equal(CdcOperationType.Insert, insert.Operation);
        Assert.Equal($"[dbo].[{table}]", insert.TableName);
        Assert.Equal(1, insert.After["Id"]);
        Assert.Equal("Widget", insert.After["Name"]);
        Assert.Empty(insert.Before);
        Assert.NotEqual(DateTime.MinValue, insert.CommitTime);

        var update = changes[1];
        Assert.Equal(CdcOperationType.Update, update.Operation);
        Assert.Equal(9.99m, update.Before["Price"]);
        Assert.Equal(19.99m, update.After["Price"]);
        Assert.True(update.UpdateMask["Price"]);
        Assert.False(update.UpdateMask["Name"]);

        var delete = changes[2];
        Assert.Equal(CdcOperationType.Delete, delete.Operation);
        Assert.Equal(19.99m, delete.Before["Price"]);
        Assert.Empty(delete.After);
    }

    [Fact]
    public async Task RestartedWatcher_ResumesFromPersistedWatermark()
    {
        const string table = "Orders_Resume";
        var captureInstance = await _sql.CreateCdcTableAsync(table);
        var stateStore = new SqlCdcStateStore(_sql.ConnectionString);

        await using (var first = BuildWatcher([table], stateStore: stateStore))
        {
            await first.StartAsync();
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'before restart');");

            var before = await ChangeCollector.CollectAsync(first, 1);
            Assert.Equal(1, Assert.Single(before).After["Id"]);

            // Events reach the channel before the watermark is written, so stopping right after
            // reading one would cancel the write in flight and re-deliver it on restart.
            await WaitForWatermarkAsync(stateStore, captureInstance);
        }

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (2, N'after restart');");

        await using var second = BuildWatcher([table], stateStore: stateStore);
        await second.StartAsync();

        // The watermark survived the restart, so row 1 must not be delivered a second time.
        var after = await ChangeCollector.CollectAsync(second, 1);
        Assert.Equal(2, Assert.Single(after).After["Id"]);
    }

    [Fact]
    public async Task TransactionLargerThanBatchSize_IsDeliveredInFull()
    {
        // Regression: with the batch cap cutting mid-transaction, this watcher made no progress at
        // all and the rows below were never delivered.
        const string table = "Orders_BigTx";
        await _sql.CreateCdcTableAsync(table);

        await using var watcher = BuildWatcher([table], batchSize: 2);
        await watcher.StartAsync();

        await _sql.ExecuteAsync(
            $"""
             BEGIN TRANSACTION;
             INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'a'), (2, N'b'), (3, N'c'), (4, N'd'), (5, N'e');
             COMMIT TRANSACTION;
             """);

        var changes = await ChangeCollector.CollectAsync(watcher, 5);

        Assert.Equal(5, changes.Count);
        Assert.Equal([1, 2, 3, 4, 5], changes.Select(c => c.After["Id"]).Cast<int>().Order());
        Assert.All(changes, c => Assert.Equal(CdcOperationType.Insert, c.Operation));
    }

    [Fact]
    public async Task WatermarkOlderThanRetention_ClampsForwardAndKeepsRunning()
    {
        const string table = "Orders_Retention";
        var captureInstance = await _sql.CreateCdcTableAsync(table);
        var stateStore = new SqlCdcStateStore(_sql.ConnectionString);

        await using (var first = BuildWatcher([table], stateStore: stateStore))
        {
            await first.StartAsync();
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");
            Assert.Single(await ChangeCollector.CollectAsync(first, 1));
            await WaitForWatermarkAsync(stateStore, captureInstance);
        }

        var watermark = await stateStore.GetLastLsnAsync(captureInstance);
        Assert.NotNull(watermark);

        // Produce changes the watcher never sees, then let the cleanup job discard them, exactly as
        // it would after a service outage longer than the retention window.
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (2, N'lost'), (3, N'lost');");
        await WaitUntilAsync(
            async () =>
            {
                await _sql.CleanupChangeTableAsync(captureInstance);
                var minLsn = await _sql.GetMinLsnAsync(captureInstance);
                return minLsn is not null && minLsn.AsSpan().SequenceCompareTo(watermark) > 0;
            },
            "the retained min LSN never moved past the stored watermark");

        var logger = new RecordingLogger();
        await using var second = BuildWatcher([table], logger: logger, stateStore: stateStore);
        await second.StartAsync();

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (4, N'after cleanup');");
        var changes = await ChangeCollector.CollectUntilAsync(second, c => Equals(c.After.GetValueOrDefault("Id"), 4));

        // Before the clamp this threw on every poll and nothing was ever delivered again.
        Assert.Contains(changes, c => Equals(c.After.GetValueOrDefault("Id"), 4));
        Assert.True(
            logger.HasEntry(LogLevel.Warning, "older than the earliest retained"),
            "the lost LSN range should be reported as a warning");
    }

    [Fact]
    public async Task BrokenCaptureInstance_DoesNotStopTheOtherTables()
    {
        const string broken = "Orders_IsoBroken";
        const string healthy = "Orders_IsoHealthy";
        await _sql.CreateCdcTableAsync(broken);
        await _sql.CreateCdcTableAsync(healthy);

        var logger = new RecordingLogger();
        await using var watcher = BuildWatcher([broken, healthy], logger: logger);
        await watcher.StartAsync();

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{broken}] (Id, Name) VALUES (1, N'a');");
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{healthy}] (Id, Name) VALUES (1, N'a');");
        Assert.Equal(2, (await ChangeCollector.CollectAsync(watcher, 2)).Count);

        // Dropping the capture instance makes every poll of that table throw.
        await _sql.DisableCdcAsync(broken);
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{healthy}] (Id, Name) VALUES (2, N'b');");

        var changes = await ChangeCollector.CollectUntilAsync(
            watcher,
            c => c.SourceTable == healthy && Equals(c.After.GetValueOrDefault("Id"), 2));

        Assert.Contains(changes, c => c.SourceTable == healthy && Equals(c.After.GetValueOrDefault("Id"), 2));
        Assert.True(
            logger.HasEntry(LogLevel.Error, $"dbo_{broken}"),
            "the failing capture instance should be reported by name");
    }

    private static async Task<byte[]> WaitForWatermarkAsync(ICdcStateStore stateStore, string captureInstance)
    {
        byte[]? watermark = null;
        await WaitUntilAsync(
            async () => (watermark = await stateStore.GetLastLsnAsync(captureInstance)) is not null,
            "the watermark was never persisted");

        return watermark!;
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string because)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(500);
        }

        Assert.Fail($"Timed out waiting: {because}.");
    }
}
