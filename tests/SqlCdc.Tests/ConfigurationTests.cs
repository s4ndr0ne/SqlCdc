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
    public void SingleActiveInstanceInConfig_ConflictsWithALeaseProviderFromTheContainer()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:SingleActiveInstance"] = "true",
        }).Build();

        var services = new ServiceCollection();
        services.AddSingleton<ICdcLeaseProvider, StubLeaseProvider>();
        services.AddSqlCdc(configuration.GetSection("SqlCdc"));

        // Silently discarding the registered provider would run with the wrong election mechanism,
        // so the contradiction fails at build time with both sides named.
        var error = Assert.Throws<InvalidOperationException>(
            () => services.BuildServiceProvider().GetRequiredService<SqlCdcWatcher>());

        Assert.Contains("SingleActiveInstance", error.Message);
        Assert.Contains(nameof(StubLeaseProvider), error.Message);
    }

    private sealed class StubLeaseProvider : ICdcLeaseProvider
    {
        public Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

        public Task<bool> IsHeldAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

        public Task ReleaseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public void RegisteringTwice_KeepsASingleWatcherAndHostedService()
    {
        var services = new ServiceCollection();
        services.AddSqlCdc(cdc => cdc.UseConnectionString(ConnectionString).WatchTable("dbo", "Orders"));
        services.AddSqlCdc(cdc => cdc.UseConnectionString(ConnectionString).WatchTable("dbo", "Orders"));

        Assert.Single(services, d => d.ServiceType == typeof(SqlCdcWatcher));
        Assert.Single(services, d =>
            d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService) &&
            d.ImplementationType == typeof(SqlCdcHostedService));
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

        var lease = Assert.IsType<SqlApplicationLockLeaseProvider>(watcher.LeaseProvider);
        Assert.Equal("SqlCdc:sales", lease.ResourceName);
        Assert.False(watcher.IsLeader);
    }

    [Fact]
    public void SingleActiveInstanceFalse_RunsWithoutALease()
    {
        var watcher = Build(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:SingleActiveInstance"] = "false",
        });

        Assert.IsType<NullCdcLeaseProvider>(watcher.LeaseProvider);
    }

    [Fact]
    public void SingleActiveInstanceFalseInConfig_ConflictsWithALeaseProviderFromTheContainer()
    {
        // Turning election off in the section while the container registers a provider is as
        // contradictory as turning the built-in one on: neither side should win silently.
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = ConnectionString,
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:SingleActiveInstance"] = "false",
        }).Build();
        var services = new ServiceCollection();
        services.AddSingleton<ICdcLeaseProvider>(new SqlApplicationLockLeaseProvider(ConnectionString));
        services.AddSqlCdc(configuration.GetSection("SqlCdc"));

        var error = Assert.Throws<InvalidOperationException>(
            () => services.BuildServiceProvider().GetRequiredService<SqlCdcWatcher>());

        Assert.Contains("SingleActiveInstance", error.Message);
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
