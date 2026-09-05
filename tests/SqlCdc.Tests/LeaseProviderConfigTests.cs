using System.Reflection;

namespace SqlCdc.Tests;

/// <summary>
/// Verifies the wiring introduced so a hung database cannot stall shutdown indefinitely: the
/// lease provider shares the watcher's configured command timeout, and never runs without a
/// bounded timeout even when none is configured.
/// </summary>
public class LeaseProviderConfigTests
{
    [Fact]
    public void Builder_ThreadsTheCommandTimeout_IntoTheLeaseProvider()
    {
        var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders")
            .WithCommandTimeout(TimeSpan.FromSeconds(42))
            .UseSingleActiveInstance("orders")
            .Build();

        var lease = Assert.IsType<SqlApplicationLockLeaseProvider>(watcher.LeaseProvider);
        Assert.Equal(42, CommandTimeoutSeconds(lease));
    }

    [Fact]
    public void Builder_UsesTheDefaultCommandTimeout_WhenNoneIsConfigured()
    {
        var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders")
            .UseSingleActiveInstance("orders")
            .Build();

        var lease = Assert.IsType<SqlApplicationLockLeaseProvider>(watcher.LeaseProvider);
        Assert.Equal(30, CommandTimeoutSeconds(lease));
    }

    [Fact]
    public void PublicConstructor_FallsBackToABoundedTimeout()
    {
        var lease = new SqlApplicationLockLeaseProvider("Server=.;Database=x", "orders");
        Assert.Equal(30, CommandTimeoutSeconds(lease));
    }

    private static int CommandTimeoutSeconds(SqlApplicationLockLeaseProvider lease)
    {
        var field = typeof(SqlApplicationLockLeaseProvider)
            .GetField("_commandTimeoutSeconds", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsType<int>(field.GetValue(lease));
    }
}
