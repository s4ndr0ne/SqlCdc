namespace SqlCdc.Tests;

public class BuilderTests
{
    [Fact]
    public void Build_WithoutConnectionString_Throws()
    {
        var builder = SqlCdcWatcherBuilder.Create().WatchTable("dbo", "Orders");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_WithoutTables_Throws()
    {
        var builder = SqlCdcWatcherBuilder.Create().UseConnectionString("Server=.;Database=x");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_WithValidConfig_Succeeds()
    {
        var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders")
            .Build();

        Assert.NotNull(watcher);
        Assert.False(watcher.IsRunning);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Build_WithInvalidPollInterval_Throws(int seconds)
    {
        var builder = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders");

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithPollInterval(TimeSpan.FromSeconds(seconds)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Build_WithInvalidBatchSize_Throws(int batchSize)
    {
        var builder = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders");

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchSize(batchSize));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Build_WithInvalidLeaseRetryDelay_Throws(int seconds)
    {
        var builder = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders");

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithLeaseRetryDelay(TimeSpan.FromSeconds(seconds)));
    }

    [Fact]
    public async Task Build_WithSingleActiveInstance_Succeeds_AndIsNotLeaderUntilStarted()
    {
        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders")
            .UseSingleActiveInstance("orders")
            .WithCheckpointMode(CdcCheckpointMode.OnAcknowledgement)
            .Build();

        Assert.False(watcher.IsLeader);
        Assert.False(watcher.IsRunning);
    }

    [Fact]
    public async Task AConnectionFactory_ReplacesTheConnectionString()
    {
        // Nothing connects until StartAsync, so building proves the factory satisfies the
        // requirement that a connection string otherwise carries.
        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionFactory(_ => Task.FromResult(new Microsoft.Data.SqlClient.SqlConnection()))
            .WatchTable("dbo", "Orders")
            .Build();

        Assert.Null(watcher.Options.ConnectionString);
    }

    [Fact]
    public void WithoutAConnectionStringOrFactory_TheErrorNamesBoth()
    {
        var builder = SqlCdcWatcherBuilder.Create().WatchTable("dbo", "Orders");

        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());

        Assert.Contains("UseConnectionString", error.Message);
        Assert.Contains("UseConnectionFactory", error.Message);
    }

    [Fact]
    public void AnAccessTokenCallback_CannotBeCombinedWithACustomFactory()
    {
        var builder = SqlCdcWatcherBuilder
            .Create()
            .WatchTable("dbo", "Orders")
            .UseConnectionFactory(_ => Task.FromResult(new Microsoft.Data.SqlClient.SqlConnection()))
            .UseAccessTokenCallback((_, _) => throw new NotSupportedException());

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void UseSingleActiveInstance_WithBlankLeaseName_Throws()
    {
        var builder = SqlCdcWatcherBuilder.Create();

        Assert.Throws<ArgumentException>(() => builder.UseSingleActiveInstance("  "));
    }

    [Fact]
    public void Options_DefaultToTheSingleInstanceBehaviour()
    {
        // Leader election and acknowledgement checkpointing are both opt-in, so an existing
        // single-instance deployment behaves exactly as before.
        var options = new CdcWatcherOptions
        {
            ConnectionString = "Server=.;Database=x",
            Tables = [new CdcTableSubscription("dbo", "Orders")],
        };

        Assert.Equal(CdcCheckpointMode.OnEmit, options.CheckpointMode);
        Assert.Equal(TimeSpan.FromSeconds(10), options.LeaseRetryDelay);
        Assert.Equal(1, options.MaxHandlerAttempts);
        Assert.Equal("default", options.Name);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-2)]
    public void WithHandlerRetry_WithoutAnAttempt_Throws(int maxAttempts)
    {
        var builder = SqlCdcWatcherBuilder.Create();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithHandlerRetry(maxAttempts));
    }

    [Fact]
    public void WithHandlerRetry_WithANonPositiveDelay_Throws()
    {
        var builder = SqlCdcWatcherBuilder.Create();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithHandlerRetry(3, TimeSpan.Zero));
    }

    [Fact]
    public void WithName_WithABlankName_Throws()
    {
        var builder = SqlCdcWatcherBuilder.Create();

        Assert.Throws<ArgumentException>(() => builder.WithName(" "));
    }

    [Fact]
    public async Task TheWatcherReportsItsConfiguredName_AndAnEmptyStatusBeforeStarting()
    {
        await using var watcher = SqlCdcWatcherBuilder
            .Create()
            .UseConnectionString("Server=.;Database=x")
            .WatchTable("dbo", "Orders")
            .WithName("orders")
            .WithHandlerRetry(3, TimeSpan.FromMilliseconds(50))
            .Build();

        var status = watcher.GetStatus();

        Assert.Equal("orders", watcher.Name);
        Assert.Equal("orders", status.Name);
        Assert.False(status.IsRunning);
        Assert.Empty(status.Tables);
        Assert.Equal(0, status.MaxConsecutiveFailures);
    }
}
