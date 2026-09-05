using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class HostedServiceIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public HostedServiceIntegrationTests(SqlServerFixture sql) => _sql = sql;

    private IHost BuildHost(string table, bool withHandler)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<ChangeSink>();
        builder.Services.AddScoped<ScopeMarker>();
        builder.Services.AddSqlCdc(cdc => cdc
            .UseConnectionString(_sql.ConnectionString)
            .WatchTable("dbo", table)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200)));

        if (withHandler)
        {
            builder.Services.AddCdcChangeHandler<CollectingHandler>();
            builder.Services.AddCdcDeadLetterSink(new SqlCdcDeadLetterSink(_sql.ConnectionString));
        }

        return builder.Build();
    }

    [Fact]
    public async Task RegisteredHandler_ReceivesChanges_InItsOwnScopePerChange()
    {
        const string table = "Orders_Di";
        await _sql.CreateCdcTableAsync(table);

        using var host = BuildHost(table, withHandler: true);
        var sink = host.Services.GetRequiredService<ChangeSink>();
        await host.StartAsync();

        try
        {
            await _sql.ExecuteAsync(
                $"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first'), (2, N'second');");

            Assert.True(
                await sink.WaitForAsync(r => r.Count >= 2, ChangeCollector.DefaultTimeout),
                $"the handler received {sink.Received.Count} changes instead of 2");

            var received = sink.Received.ToList();
            Assert.All(received, r => Assert.Equal(CdcOperationType.Insert, r.Change.Operation));
            Assert.Equal([1, 2], received.Select(r => r.Change.After["Id"]).Cast<int>().Order());

            // Handlers are documented to run in a dedicated scope per change, so scoped
            // dependencies such as a DbContext are not shared between events.
            Assert.Equal(2, received.Select(r => r.ScopeId).Distinct().Count());
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task WithoutHandlers_TheWatcherIsResolvableAndRunning()
    {
        const string table = "Orders_DiNoHandler";
        await _sql.CreateCdcTableAsync(table);

        using var host = BuildHost(table, withHandler: false);
        var watcher = host.Services.GetRequiredService<SqlCdcWatcher>();
        await host.StartAsync();

        try
        {
            await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

            var changes = await ChangeCollector.CollectAsync(watcher, 1);
            Assert.Equal(1, Assert.Single(changes).After["Id"]);
            Assert.True(watcher.IsRunning);
        }
        finally
        {
            await host.StopAsync();
        }

        Assert.False(watcher.IsRunning);
    }
}

/// <summary>Scoped marker used to prove each change is handled in its own scope.</summary>
public sealed class ScopeMarker
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed record Received(CdcChange Change, Guid ScopeId);

/// <summary>Singleton collecting what the scoped handlers saw.</summary>
public sealed class ChangeSink
{
    private readonly ConcurrentQueue<Received> _received = new();

    public IReadOnlyCollection<Received> Received => _received;

    public void Add(Received received) => _received.Enqueue(received);

    public async Task<bool> WaitForAsync(Func<IReadOnlyCollection<Received>, bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate(_received))
            {
                return true;
            }

            await Task.Delay(100);
        }

        return predicate(_received);
    }
}

public sealed class CollectingHandler : ICdcChangeHandler
{
    private readonly ChangeSink _sink;
    private readonly ScopeMarker _marker;

    public CollectingHandler(ChangeSink sink, ScopeMarker marker)
    {
        _sink = sink;
        _marker = marker;
    }

    public Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default)
    {
        _sink.Add(new Received(change, _marker.Id));
        return Task.CompletedTask;
    }
}
