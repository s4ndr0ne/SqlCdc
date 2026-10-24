using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace SqlCdc.Tests;

public class NewFeaturesTests
{
    private sealed class OrderModel
    {
        public int Id { get; set; }
        public string? CustomerName { get; set; }
        public decimal Total { get; set; }
        public Guid OrderGuid { get; set; }
        public DateTime CreatedAt { get; set; }
        public OrderStatus Status { get; set; }
        public int? OptionalCount { get; set; }
    }

    private enum OrderStatus
    {
        Pending = 0,
        Completed = 1,
    }

    [Fact]
    public void CdcChange_GetAfter_DeserializesValuesCorrectly()
    {
        var guid = Guid.NewGuid();
        var now = DateTime.UtcNow;

        var change = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1, 2, 3],
            SeqVal = [4, 5, 6],
            CommitTime = now,
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?>
            {
                ["Id"] = 42,
                ["CustomerName"] = "Alice",
                ["Total"] = 99.95m,
                ["OrderGuid"] = guid.ToString(),
                ["CreatedAt"] = now,
                ["Status"] = "Completed",
                ["OptionalCount"] = 5,
            },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var model = change.GetAfter<OrderModel>();
        Assert.NotNull(model);
        Assert.Equal(42, model.Id);
        Assert.Equal("Alice", model.CustomerName);
        Assert.Equal(99.95m, model.Total);
        Assert.Equal(guid, model.OrderGuid);
        Assert.Equal(now, model.CreatedAt);
        Assert.Equal(OrderStatus.Completed, model.Status);
        Assert.Equal(5, model.OptionalCount);

        // Before was empty: GetBefore returns null
        Assert.Null(change.GetBefore<OrderModel>());
    }

    [Fact]
    public void CdcChange_GetAfter_InvalidColumnConversionLeavesThePropertyAtItsDefault()
    {
        var change = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1, 2, 3],
            SeqVal = [4, 5, 6],
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?>
            {
                ["Id"] = "not-an-integer",
            },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var model = change.GetAfter<OrderModel>();

        Assert.NotNull(model);
        Assert.Equal(0, model.Id);
    }

    [Fact]
    public void CdcChange_GetBefore_DeserializesValuesCorrectly()
    {
        var change = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Delete,
            StartLsn = [1, 2, 3],
            SeqVal = [4, 5, 6],
            CommitTime = DateTime.UtcNow,
            Before = new Dictionary<string, object?>
            {
                ["Id"] = 101,
                ["CustomerName"] = "Bob",
                ["Total"] = 10.0m,
                ["Status"] = 0,
            },
            After = new Dictionary<string, object?>(),
            UpdateMask = new Dictionary<string, bool>(),
        };

        var before = change.GetBefore<OrderModel>();
        Assert.NotNull(before);
        Assert.Equal(101, before.Id);
        Assert.Equal("Bob", before.CustomerName);
        Assert.Equal(OrderStatus.Pending, before.Status);

        Assert.Null(change.GetAfter<OrderModel>());
    }

    [Fact]
    public void Builder_WatchTableWithColumns_ConfiguresSubscription()
    {
        var watcher = SqlCdcWatcherBuilder.Create()
            .UseConnectionString("Server=.;Database=x;Encrypt=True")
            .WatchTable("dbo", "Orders", "dbo_Orders", ["Id", "Status", "Total"])
            .WithMaxBatchesPerTablePerPoll(5)
            .Build();

        var sub = Assert.Single(watcher.Options.Tables);
        Assert.Equal("dbo", sub.Schema);
        Assert.Equal("Orders", sub.Table);
        Assert.Equal("dbo_Orders", sub.CaptureInstance);
        Assert.NotNull(sub.Columns);
        Assert.Equal(["Id", "Status", "Total"], sub.Columns);
        Assert.Equal(5, watcher.Options.MaxBatchesPerTablePerPoll);
    }

    [Fact]
    public void Configuration_BindsColumnsAndMaxBatchesPerTable()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["SqlCdc:ConnectionString"] = "Server=.;Database=test;Encrypt=True",
            ["SqlCdc:MaxBatchesPerTablePerPoll"] = "3",
            ["SqlCdc:Tables:0:Schema"] = "dbo",
            ["SqlCdc:Tables:0:Table"] = "Orders",
            ["SqlCdc:Tables:0:Columns:0"] = "Id",
            ["SqlCdc:Tables:0:Columns:1"] = "Total",
        }).Build().GetSection("SqlCdc");

        var config = configuration.Get<SqlCdcConfiguration>();
        Assert.NotNull(config);
        Assert.Equal(3, config.MaxBatchesPerTablePerPoll);

        var watcherBuilder = SqlCdcWatcherBuilder.Create();
        config.ApplyTo(watcherBuilder);
        var watcher = watcherBuilder.Build();

        Assert.Equal(3, watcher.Options.MaxBatchesPerTablePerPoll);
        var table = Assert.Single(watcher.Options.Tables);
        Assert.Equal(["Id", "Total"], table.Columns);
    }

    [CdcTable("Orders")]
    private sealed class OrdersOnlyHandler : ICdcChangeHandler
    {
        public List<CdcChange> Handled { get; } = new();

        public Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default)
        {
            Handled.Add(change);
            return Task.CompletedTask;
        }
    }

    private sealed class CustomerTableHandler : ICdcTableChangeHandler
    {
        public string? SourceSchema => "dbo";
        public string SourceTable => "Customers";
        public List<CdcChange> Handled { get; } = new();

        public Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default)
        {
            Handled.Add(change);
            return Task.CompletedTask;
        }
    }

    private sealed class UniversalHandler : ICdcChangeHandler
    {
        public List<CdcChange> Handled { get; } = new();

        public Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default)
        {
            Handled.Add(change);
            return Task.CompletedTask;
        }
    }

    private sealed class TestDeadLetterSink : ICdcDeadLetterSink
    {
        public Task WriteAsync(CdcDeadLetter deadLetter, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    [Fact]
    public async Task HostedService_FiltersChangesByTableCorrectly()
    {
        var services = new ServiceCollection();
        var watcher = SqlCdcWatcherBuilder.Create()
            .UseConnectionString("Server=.;Database=x;Encrypt=True")
            .WatchTable("dbo", "Orders")
            .WatchTable("dbo", "Customers")
            .UseStateStore(new InMemoryCdcStateStore())
            .Build();

        var ordersHandler = new OrdersOnlyHandler();
        var customerHandler = new CustomerTableHandler();
        var universalHandler = new UniversalHandler();

        services.AddSingleton(ordersHandler);
        services.AddScoped<ICdcChangeHandler>(sp => sp.GetRequiredService<OrdersOnlyHandler>());

        services.AddSingleton(customerHandler);
        services.AddScoped<ICdcChangeHandler>(sp => sp.GetRequiredService<CustomerTableHandler>());

        services.AddSingleton(universalHandler);
        services.AddScoped<ICdcChangeHandler>(sp => sp.GetRequiredService<UniversalHandler>());

        var sink = new TestDeadLetterSink();
        services.AddSingleton<ICdcDeadLetterSink>(sink);

        var sp = services.BuildServiceProvider();
        var hostedService = new SqlCdcHostedService(
            watcher,
            sp.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<SqlCdcHostedService>.Instance,
            sink);

        var orderChange = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1],
            SeqVal = [1],
            CommitTime = DateTime.UtcNow,
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?> { ["Id"] = 1 },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var customerChange = new CdcChange
        {
            CaptureInstance = "dbo_Customers",
            SourceSchema = "dbo",
            SourceTable = "Customers",
            Operation = CdcOperationType.Insert,
            StartLsn = [2],
            SeqVal = [2],
            CommitTime = DateTime.UtcNow,
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?> { ["Id"] = 99 },
            UpdateMask = new Dictionary<string, bool>(),
        };

        // Dispatch orderChange and customerChange using reflection on DispatchAsync
        var dispatchMethod = typeof(SqlCdcHostedService).GetMethod(
            "DispatchAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        await (Task)dispatchMethod.Invoke(hostedService, [orderChange, CancellationToken.None])!;
        await (Task)dispatchMethod.Invoke(hostedService, [customerChange, CancellationToken.None])!;

        // OrdersOnlyHandler should only receive orderChange
        Assert.Single(ordersHandler.Handled);
        Assert.Equal("Orders", ordersHandler.Handled[0].SourceTable);

        // CustomerTableHandler should only receive customerChange
        Assert.Single(customerHandler.Handled);
        Assert.Equal("Customers", customerHandler.Handled[0].SourceTable);

        // UniversalHandler should receive both
        Assert.Equal(2, universalHandler.Handled.Count);
    }

    private sealed class TestBatchHandler : ICdcBatchChangeHandler
    {
        public List<IReadOnlyList<CdcChange>> Batches { get; } = new();

        public Task HandleBatchAsync(IReadOnlyList<CdcChange> changes, CancellationToken cancellationToken = default)
        {
            Batches.Add(changes);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task HostedService_DispatchesBatchChanges()
    {
        var services = new ServiceCollection();
        var watcher = SqlCdcWatcherBuilder.Create()
            .UseConnectionString("Server=.;Database=x;Encrypt=True")
            .WatchTable("dbo", "Orders")
            .UseStateStore(new InMemoryCdcStateStore())
            .Build();

        var batchHandler = new TestBatchHandler();
        services.AddSingleton(batchHandler);
        services.AddScoped<ICdcBatchChangeHandler>(sp => sp.GetRequiredService<TestBatchHandler>());

        var sink = new TestDeadLetterSink();
        services.AddSingleton<ICdcDeadLetterSink>(sink);

        var sp = services.BuildServiceProvider();
        var hostedService = new SqlCdcHostedService(
            watcher,
            sp.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<SqlCdcHostedService>.Instance,
            sink);

        var change1 = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1],
            SeqVal = [1],
            CommitTime = DateTime.UtcNow,
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?> { ["Id"] = 1 },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var change2 = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1],
            SeqVal = [2],
            CommitTime = DateTime.UtcNow,
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?> { ["Id"] = 2 },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var dispatchBatchMethod = typeof(SqlCdcHostedService).GetMethod(
            "DispatchBatchAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        await (Task)dispatchBatchMethod.Invoke(hostedService, [new List<CdcChange> { change1, change2 }, CancellationToken.None])!;

        var batch = Assert.Single(batchHandler.Batches);
        Assert.Equal(2, batch.Count);
    }

    [Fact]
    public void CdcChange_GetAfter_MatchesColumnNamesCaseInsensitively()
    {
        var change = new CdcChange
        {
            CaptureInstance = "dbo_Orders",
            SourceSchema = "dbo",
            SourceTable = "Orders",
            Operation = CdcOperationType.Insert,
            StartLsn = [1, 2, 3],
            SeqVal = [4, 5, 6],
            Before = new Dictionary<string, object?>(),
            After = new Dictionary<string, object?>
            {
                ["id"] = 77,
                ["customername"] = "CaseInsensitive",
                ["total"] = "19.99",
            },
            UpdateMask = new Dictionary<string, bool>(),
        };

        var model = change.GetAfter<OrderModel>();
        Assert.NotNull(model);
        Assert.Equal(77, model.Id);
        Assert.Equal("CaseInsensitive", model.CustomerName);
        Assert.Equal(19.99m, model.Total);
    }

    [Fact]
    public void Configuration_HandlerRetryDelayAlone_PreservesConfiguredMaxHandlerAttempts()
    {
        var config = new SqlCdcConfiguration
        {
            HandlerRetryDelay = TimeSpan.FromSeconds(12),
        };

        var builder = SqlCdcWatcherBuilder.Create()
            .UseConnectionString("Server=.;Database=x;Encrypt=True")
            .WatchTable("dbo", "Orders")
            .WithHandlerRetry(4, TimeSpan.FromSeconds(2));

        config.ApplyTo(builder);
        var watcher = builder.Build();

        Assert.Equal(4, watcher.Options.MaxHandlerAttempts);
        Assert.Equal(TimeSpan.FromSeconds(12), watcher.Options.HandlerRetryDelay);
    }
}
