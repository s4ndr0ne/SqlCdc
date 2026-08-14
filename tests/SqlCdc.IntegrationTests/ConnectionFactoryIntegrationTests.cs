using Microsoft.Data.SqlClient;

namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class ConnectionFactoryIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public ConnectionFactoryIntegrationTests(SqlServerFixture sql) => _sql = sql;

    /// <summary>
    /// Everything that touches SQL Server — the CDC reads, the watermark table and the lease —
    /// has to go through the factory, otherwise a deployment authenticating with a token would
    /// have parts of the pipeline silently falling back to the raw connection string.
    /// </summary>
    [Fact]
    public async Task EveryConnection_IsOpenedByTheFactory()
    {
        const string table = "Orders_Factory";
        var captureInstance = await _sql.CreateCdcTableAsync(table);

        var factory = new CountingConnectionFactory(_sql.ConnectionString);
        var store = new SqlCdcStateStore(factory);

        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionFactory(factory)
            .WatchTable("dbo", table)
            .UseStateStore(store)
            .UseSingleActiveInstance("factory")
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .Build();

        await watcher.StartAsync();
        Assert.True(await Wait.UntilAsync(() => watcher.IsLeader), "the lease was never taken");

        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (1, N'first');");

        var change = Assert.Single(await ChangeCollector.CollectAsync(watcher, 1));
        Assert.Equal(1, change.After["Id"]);

        Assert.True(
            await Wait.UntilAsync(async () => await store.GetLastLsnAsync(captureInstance) is not null),
            "the watermark was never persisted through the factory");

        Assert.True(factory.OpenCount > 0, "the factory was never used");
    }

    [Fact]
    public async Task ADelegateFactory_MayReturnAConnectionItHasNotOpened()
    {
        const string table = "Orders_FactoryDelegate";
        await _sql.CreateCdcTableAsync(table);

        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionFactory(_ => Task.FromResult(new SqlConnection(_sql.ConnectionString)))
            .WatchTable("dbo", table)
            .StartFrom(CdcStartMode.FromBeginning)
            .WithPollInterval(TimeSpan.FromMilliseconds(200))
            .Build();

        await watcher.StartAsync();
        await _sql.ExecuteAsync($"INSERT INTO dbo.[{table}] (Id, Name) VALUES (2, N'second');");

        var change = Assert.Single(await ChangeCollector.CollectAsync(watcher, 1));
        Assert.Equal(2, change.After["Id"]);
    }

    /// <summary>Wraps the built-in factory to prove the pipeline goes through it.</summary>
    private sealed class CountingConnectionFactory : ICdcConnectionFactory
    {
        private readonly SqlCdcConnectionFactory _inner;
        private int _opened;

        public CountingConnectionFactory(string connectionString) =>
            _inner = new SqlCdcConnectionFactory(connectionString);

        public int OpenCount => Volatile.Read(ref _opened);

        public Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _opened);
            return _inner.OpenConnectionAsync(cancellationToken);
        }
    }
}
