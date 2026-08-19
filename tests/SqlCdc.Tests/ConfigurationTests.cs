using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SqlCdc.Tests;

public class ConfigurationTests
{
    private const string ConnectionString = "Server=.;Database=Sales";

    [Fact]
    public void ASectionConfiguresTheWatcher()
    {
        var watcher = Build(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Name"] = "sales",
            ["SqlCdc:Tables:0:Schema"] = "sales",
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:Tables:1:Table"] = "Customers",
            ["SqlCdc:Tables:1:CaptureInstance"] = "dbo_Customers_v2",
            ["SqlCdc:PollInterval"] = "00:00:00.250",
            ["SqlCdc:BatchSize"] = "250",
            ["SqlCdc:ChannelCapacity"] = "5000",
            ["SqlCdc:StartMode"] = "FromBeginning",
            ["SqlCdc:RetryDelay"] = "00:00:07",
            ["SqlCdc:CommandTimeout"] = "00:01:00",
            ["SqlCdc:CheckpointMode"] = "OnAcknowledgement",
            ["SqlCdc:LeaseRetryDelay"] = "00:00:03",
            ["SqlCdc:LeaseKeepaliveInterval"] = "00:00:15",
            ["SqlCdc:MaxHandlerAttempts"] = "4",
            ["SqlCdc:HandlerRetryDelay"] = "00:00:02",
        });

        var options = watcher.Options;

        Assert.Equal(ConnectionString, options.ConnectionString);
        Assert.Equal("sales", options.Name);
        Assert.Equal(TimeSpan.FromMilliseconds(250), options.PollInterval);
        Assert.Equal(250, options.BatchSize);
        Assert.Equal(5000, options.ChannelCapacity);
        Assert.Equal(CdcStartMode.FromBeginning, options.StartMode);
        Assert.Equal(TimeSpan.FromSeconds(7), options.RetryDelay);
        Assert.Equal(TimeSpan.FromMinutes(1), options.CommandTimeout);
        Assert.Equal(CdcCheckpointMode.OnAcknowledgement, options.CheckpointMode);
        Assert.Equal(TimeSpan.FromSeconds(3), options.LeaseRetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(15), options.LeaseKeepaliveInterval);
        Assert.Equal(4, options.MaxHandlerAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), options.HandlerRetryDelay);

        Assert.Collection(
            options.Tables,
            first =>
            {
                Assert.Equal("sales", first.Schema);
                Assert.Equal("Orders", first.Table);
                Assert.Null(first.CaptureInstance);
            },
            second =>
            {
                // Schema is not in configuration for this one and falls back to dbo.
                Assert.Equal("dbo", second.Schema);
                Assert.Equal("Customers", second.Table);
                Assert.Equal("dbo_Customers_v2", second.CaptureInstance);
            });
    }

    [Fact]
    public void SettingsAbsentFromTheSection_KeepTheirDefaults()
    {
        var watcher = Build(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Table"] = "Orders",
        });

        Assert.Equal(TimeSpan.FromMilliseconds(500), watcher.Options.PollInterval);
        Assert.Equal(1000, watcher.Options.BatchSize);
        Assert.Equal(CdcStartMode.FromNow, watcher.Options.StartMode);
        Assert.Equal("default", watcher.Options.Name);
    }

    [Fact]
    public void CodeWinsOverTheSection()
    {
        var watcher = Build(
            new Dictionary<string, string?>
            {
                ["SqlCdc:ConnectionString"] = ConnectionString,
                ["SqlCdc:Tables:0:Table"] = "Orders",
                ["SqlCdc:BatchSize"] = "250",
            },
            cdc => cdc.WithBatchSize(999));

        Assert.Equal(999, watcher.Options.BatchSize);
    }

    [Fact]
    public void AMissingSection_FailsWithItsPath()
    {
        var configuration = new ConfigurationBuilder().Build();
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(
            () => services.AddSqlCdc(configuration.GetSection("SqlCdc")));

        Assert.Contains("SqlCdc", error.Message);
        Assert.Contains("does not exist", error.Message);
    }

    [Fact]
    public void ATableWithoutAName_FailsWithAClearMessage()
    {
        var error = Assert.Throws<InvalidOperationException>(() => Build(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Schema"] = "dbo",
        }));

        Assert.Contains("'Table'", error.Message);
    }

    [Fact]
    public void LeaseNameAlone_TurnsOnSingleActiveInstance()
    {
        var watcher = Build(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:LeaseName"] = "sales",
        });

        // The lease is only taken once the loop runs, so all that can be asserted here is that
        // building with it configured succeeds and the watcher starts out as a standby.
        Assert.False(watcher.IsLeader);
    }

    private static SqlCdcWatcher Build(
        Dictionary<string, string?> settings,
        Action<SqlCdcWatcherBuilder>? configure = null)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var services = new ServiceCollection();
        services.AddSqlCdc(configuration.GetSection("SqlCdc"), configure);

        return services.BuildServiceProvider().GetRequiredService<SqlCdcWatcher>();
    }
}
