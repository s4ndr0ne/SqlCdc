using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class DiagnosticsIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public DiagnosticsIntegrationTests(SqlServerFixture sql) => _sql = sql;

    [Fact]
    public async Task EmittedChanges_AreCountedAndTraced()
    {
        const string table = "Orders_Metrics";
        const string watcherName = "metrics";
        await _sql.CreateCdcTableAsync(table);

        var emitted = 0L;
        double? lag = null;
        var pollSpans = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == SqlCdcDiagnostics.MeterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name == SqlCdcDiagnostics.ChangesEmittedMetric && IsWatcher(tags, watcherName))
            {
                Interlocked.Add(ref emitted, measurement);
            }
        });
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name == SqlCdcDiagnostics.ChangeLagMetric && IsWatcher(tags, watcherName))
            {
                lag = measurement;
            }
        });
        listener.Start();

        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SqlCdcDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "SqlCdc.Poll")
                {
                    Interlocked.Increment(ref pollSpans);
                }
            },
        };
        ActivitySource.AddActivityListener(activityListener);

        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .WithName(watcherName)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .Build();

        await watcher.StartAsync();
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'a'), (2, N'b');");

        var changes = await ChangeCollector.CollectAsync(watcher, 2);
        Assert.Equal(2, changes.Count);

        Assert.Equal(2, Interlocked.Read(ref emitted));
        Assert.True(pollSpans > 0, "no SqlCdc.Poll span was recorded");

        // Lag is computed entirely on the SQL Server clock, so it stays sane even when the server
        // runs in a different time zone than the test host.
        Assert.NotNull(lag);
        Assert.InRange(lag!.Value, 0, TimeSpan.FromMinutes(5).TotalSeconds);

        var status = watcher.GetStatus();
        var reported = Assert.Single(status.Tables);
        Assert.Equal($"dbo_{table}", reported.CaptureInstance);
        Assert.Equal(2, reported.ChangesEmitted);
        Assert.Equal(0, reported.ConsecutiveFailures);
        Assert.NotNull(reported.LastSuccessfulPoll);
    }

    [Fact]
    public async Task TheHealthCheck_ReportsARunningWatcher()
    {
        const string table = "Orders_Health";
        await _sql.CreateCdcTableAsync(table);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSqlCdc(cdc => cdc
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .WithName("health")
            .WithPollInterval(TimeSpan.FromMilliseconds(200)));
        builder.Services.AddHealthChecks().AddSqlCdc(tags: ["ready"]);

        using var host = builder.Build();
        var watcher = host.Services.GetRequiredService<SqlCdcWatcher>();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        // Before the host starts the watcher is not polling, and the probe has to say so.
        var beforeStart = await health.CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, beforeStart.Status);

        await host.StartAsync();
        try
        {
            Assert.True(await Wait.UntilAsync(() => watcher.IsRunning), "the watcher never started");

            var report = await health.CheckHealthAsync();
            var entry = report.Entries["sqlcdc"];

            Assert.Equal(HealthStatus.Healthy, entry.Status);
            Assert.Equal("health", entry.Data["watcher"]);
            Assert.Contains("ready", entry.Tags);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task AFailingHandler_IsRetried_ThenDeadLettered()
    {
        const string table = "Orders_Dlq";
        await _sql.CreateCdcTableAsync(table);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<AttemptCounter>();
        builder.Services.AddCdcChangeHandler<AlwaysFailingHandler>();
        builder.Services.AddCdcDeadLetterSink(new SqlCdcDeadLetterSink(_sql.ConnectionString));
        builder.Services.AddSqlCdc(cdc => cdc
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithHandlerRetry(2, TimeSpan.FromMilliseconds(50))
            .WithPollInterval(TimeSpan.FromMilliseconds(200)));

        using var host = builder.Build();
        var attempts = host.Services.GetRequiredService<AttemptCounter>();
        await host.StartAsync();

        try
        {
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (42, N'poison');");

            Assert.True(
                await Wait.UntilAsync(async () => await CountDeadLettersAsync(table) > 0),
                "the failed change was never dead-lettered");

            var deadLetter = await ReadDeadLetterAsync(table);
            Assert.Equal(2, deadLetter.Attempts);
            Assert.Equal(nameof(AlwaysFailingHandler), deadLetter.HandlerName);
            Assert.Equal("Insert", deadLetter.Operation);
            Assert.Contains("poison", deadLetter.Payload);
            Assert.Contains("handler is broken", deadLetter.Error);

            // Both attempts of the same change, and no more: the change is not retried forever.
            Assert.Equal(2, attempts.Count);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static bool IsWatcher(ReadOnlySpan<KeyValuePair<string, object?>> tags, string name)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "watcher" && Equals(tag.Value, name))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<int> CountDeadLettersAsync(string table)
    {
        await using var conn = new SqlConnection(_sql.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "IF OBJECT_ID(N'[dbo].[cdc_dead_letter]', N'U') IS NULL SELECT 0; " +
            "ELSE SELECT COUNT(*) FROM dbo.cdc_dead_letter WHERE SourceTable = @table;", conn);
        cmd.Parameters.AddWithValue("@table", table);
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<DeadLetterRow> ReadDeadLetterAsync(string table)
    {
        await using var conn = new SqlConnection(_sql.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "SELECT TOP 1 Operation, Payload, HandlerName, Attempts, Error " +
            "FROM dbo.cdc_dead_letter WHERE SourceTable = @table ORDER BY Id;", conn);
        cmd.Parameters.AddWithValue("@table", table);

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "no dead letter row was found");

        return new DeadLetterRow(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetInt32(3),
            reader.GetString(4));
    }

    private sealed record DeadLetterRow(
        string Operation,
        string Payload,
        string HandlerName,
        int Attempts,
        string Error);
}

/// <summary>Counts how many times the broken handler was actually called.</summary>
public sealed class AttemptCounter
{
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public void Increment() => Interlocked.Increment(ref _count);
}

public sealed class AlwaysFailingHandler : ICdcChangeHandler
{
    private readonly AttemptCounter _attempts;

    public AlwaysFailingHandler(AttemptCounter attempts) => _attempts = attempts;

    public Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default)
    {
        _attempts.Increment();
        throw new InvalidOperationException("this handler is broken");
    }
}
