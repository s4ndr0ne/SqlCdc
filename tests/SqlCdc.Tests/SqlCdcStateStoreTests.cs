using Microsoft.Data.SqlClient;

namespace SqlCdc.Tests;

public class SqlCdcStateStoreTests
{
    private const string ConnectionString = "Server=.;Database=Cdc;Integrated Security=True";

    private sealed class FakeConnectionFactory : ICdcConnectionFactory
    {
        public Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }

    [Fact]
    public void SharesConnectionsWith_SameConnectionString_IsTrue()
    {
        var store = new SqlCdcStateStore(ConnectionString);

        Assert.True(store.SharesConnectionsWith(new SqlCdcConnectionFactory(ConnectionString)));
    }

    [Fact]
    public void SharesConnectionsWith_DifferentDatabase_IsFalse()
    {
        // Regression: the watcher saved watermarks on its poll connection whatever database the
        // store pointed at, so a store on a separate database read from one place and wrote to another.
        var store = new SqlCdcStateStore("Server=.;Database=Ops;Integrated Security=True");

        Assert.False(store.SharesConnectionsWith(new SqlCdcConnectionFactory(ConnectionString)));
    }

    [Fact]
    public void SharesConnectionsWith_DifferentTokenCallback_IsFalse()
    {
        var store = new SqlCdcStateStore(new SqlCdcConnectionFactory(
            ConnectionString,
            (_, _) => Task.FromResult(new SqlAuthenticationToken("a", DateTimeOffset.MaxValue))));

        Assert.False(store.SharesConnectionsWith(new SqlCdcConnectionFactory(ConnectionString)));
    }

    [Fact]
    public void SharesConnectionsWith_SameCustomFactoryInstance_IsTrue()
    {
        var factory = new FakeConnectionFactory();
        var store = new SqlCdcStateStore(factory);

        Assert.True(store.SharesConnectionsWith(factory));
    }

    [Fact]
    public void SharesConnectionsWith_DifferentCustomFactory_IsFalse()
    {
        // Two custom factories cannot be compared, so they are assumed to differ: the cost is a
        // separate connection per save, never a watermark in the wrong database.
        var store = new SqlCdcStateStore(new FakeConnectionFactory());

        Assert.False(store.SharesConnectionsWith(new FakeConnectionFactory()));
        Assert.False(store.SharesConnectionsWith(new SqlCdcConnectionFactory(ConnectionString)));
    }
}
