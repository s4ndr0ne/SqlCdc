using Microsoft.Extensions.Logging;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class CaptureInstanceIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public CaptureInstanceIntegrationTests(SqlServerFixture sql) => _sql = sql;

    /// <summary>
    /// A table being migrated has two capture instances. Reading whichever one the server happened
    /// to return first is a coin flip that decides whether the watermark still applies, so the
    /// older one — the one already being consumed — has to win, and say so.
    /// </summary>
    [Fact]
    public async Task WithTwoCaptureInstances_TheOlderOneIsUsed_AndReported()
    {
        const string table = "Orders_Multi";
        var original = await _sql.CreateCdcTableAsync(table);
        var migrated = await _sql.AddCaptureInstanceAsync(table, $"{original}_v2");

        var logger = new RecordingLogger();
        await using var watcher = Build(table, logger);

        await watcher.StartAsync();
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

        var change = Assert.Single(await ChangeCollector.CollectAsync(watcher, 1));

        Assert.Equal(original, change.CaptureInstance);
        Assert.True(
            logger.HasEntry(LogLevel.Warning, "capture instances"),
            "the second capture instance was picked up silently");
        Assert.True(logger.HasEntry(LogLevel.Warning, migrated), "the warning does not name both instances");
    }

    [Fact]
    public async Task AnExplicitCaptureInstance_IsUsed()
    {
        const string table = "Orders_MultiExplicit";
        var original = await _sql.CreateCdcTableAsync(table);
        var migrated = await _sql.AddCaptureInstanceAsync(table, $"{original}_v2");

        var logger = new RecordingLogger();
        await using var watcher = Build(table, logger, migrated);

        await watcher.StartAsync();
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

        var change = Assert.Single(await ChangeCollector.CollectAsync(watcher, 1));

        Assert.Equal(migrated, change.CaptureInstance);

        // Nothing ambiguous is left to warn about once the choice is explicit.
        Assert.False(logger.HasEntry(LogLevel.Warning, "capture instances"));
    }

    /// <summary>
    /// A misspelled capture instance used to be accepted and then fail on every poll with
    /// "Invalid object name 'cdc.fn_cdc_get_all_changes_...'".
    /// </summary>
    [Fact]
    public async Task AnUnknownCaptureInstance_FailsAtStartupWithTheAvailableOnes()
    {
        const string table = "Orders_BadInstance";
        var captureInstance = await _sql.CreateCdcTableAsync(table);

        await using var watcher = Build(table, new RecordingLogger(), "dbo_Typo");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => watcher.StartAsync());

        Assert.Contains("dbo_Typo", error.Message);
        Assert.Contains(captureInstance, error.Message);
    }

    [Fact]
    public async Task ATableWithoutCdc_FailsAtStartup()
    {
        await _sql.ExecuteAsync("CREATE TABLE dbo.[Orders_NoCdc] (Id int NOT NULL PRIMARY KEY);");

        await using var watcher = Build("Orders_NoCdc", new RecordingLogger());

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => watcher.StartAsync());

        Assert.Contains("No CDC capture instance found", error.Message);
        Assert.Contains("sp_cdc_enable_table", error.Message);
    }

    private SqlCdcWatcher Build(string table, ILogger logger, string? captureInstance = null) =>
        SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table, captureInstance)
            .UseLogger(logger)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .Build();
}
