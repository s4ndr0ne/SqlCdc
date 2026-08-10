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
}
