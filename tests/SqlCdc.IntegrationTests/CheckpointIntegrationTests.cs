using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class CheckpointIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public CheckpointIntegrationTests(SqlServerFixture sql) => _sql = sql;

    /// <summary>
    /// The point of the acknowledgement mode: a change that reached the channel but was not
    /// processed must not move the watermark, so a crash at that moment replays it.
    /// </summary>
    [Fact]
    public async Task OnAcknowledgement_TheWatermarkWaitsForTheConsumer()
    {
        const string table = "Orders_Ack";
        var captureInstance = await _sql.CreateCdcTableAsync(table);
        var store = new InMemoryCdcStateStore();

        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .StartFrom(CdcStartMode.FromBeginning)
            .UseStateStore(store)
            .WithCheckpointMode(CdcCheckpointMode.OnAcknowledgement)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .Build();

        await watcher.StartAsync();

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

        using var cts = new CancellationTokenSource(ChangeCollector.DefaultTimeout);
        var change = await watcher.Channel.Reader.ReadAsync(cts.Token);
        Assert.Equal(1, change.After["Id"]);

        // Delivered but not acknowledged: the poller is parked on the checkpoint barrier. Empty
        // polls before the change may have persisted a watermark, but never one at or past the
        // unacknowledged change — that is the replay guarantee.
        await Task.Delay(TimeSpan.FromSeconds(2));
        var stored = await store.GetLastLsnAsync(captureInstance);
        Assert.True(
            stored is null || stored.AsSpan().SequenceCompareTo(change.StartLsn) < 0,
            "the watermark must not advance to or past an unacknowledged change");

        change.Acknowledge();

        Assert.True(
            await Wait.UntilAsync(async () =>
                await store.GetLastLsnAsync(captureInstance) is { } lsn &&
                lsn.AsSpan().SequenceCompareTo(change.StartLsn) >= 0),
            "the watermark was never persisted after the change was acknowledged");
    }

    /// <summary>
    /// The hosted service acknowledges after dispatch, so a pipeline with handlers keeps flowing
    /// past the first batch without the application doing anything.
    /// </summary>
    [Fact]
    public async Task OnAcknowledgement_TheHostedServiceAcknowledgesAfterDispatch()
    {
        const string table = "Orders_AckHost";
        await _sql.CreateCdcTableAsync(table);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<ChangeSink>();
        builder.Services.AddScoped<ScopeMarker>();
        builder.Services.AddCdcChangeHandler<CollectingHandler>();
        builder.Services.AddCdcDeadLetterSink(new SqlCdcDeadLetterSink(_sql.ConnectionString));
        builder.Services.AddSqlCdc(cdc => cdc
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithCheckpointMode(CdcCheckpointMode.OnAcknowledgement)
            .WithPollInterval(TimeSpan.FromMilliseconds(200)));

        using var host = builder.Build();
        var sink = host.Services.GetRequiredService<ChangeSink>();
        await host.StartAsync();

        try
        {
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");
            Assert.True(
                await sink.WaitForAsync(r => r.Count >= 1, ChangeCollector.DefaultTimeout),
                "the first change was never handled");

            // A missing acknowledgement would stall the poller here: the second batch only arrives
            // because the first one was checkpointed.
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (2, N'second');");
            Assert.True(
                await sink.WaitForAsync(r => r.Count >= 2, ChangeCollector.DefaultTimeout),
                $"polling stalled after the first batch: only {sink.Received.Count} change(s) were handled");
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
