using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace SqlCdc.Tests;

public class HealthCheckTests
{
    [Fact]
    public async Task NotRunning_IsUnhealthy()
    {
        var result = await CheckAsync(Status(running: false, leader: false));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Standby_IsHealthy()
    {
        // A replica that is not the leader is doing its job. Failing its probe would take a
        // perfectly good instance out of rotation.
        var result = await CheckAsync(Status(running: true, leader: false));

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Contains("Standing by", result.Description);
    }

    [Fact]
    public async Task Polling_IsHealthy()
    {
        var result = await CheckAsync(Status(running: true, leader: true, Table("dbo_Orders")));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task ASingleFailure_IsDegraded()
    {
        var result = await CheckAsync(Status(running: true, leader: true, Table("dbo_Orders", failures: 1)));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("dbo_Orders", result.Description);
    }

    [Fact]
    public async Task RepeatedFailures_AreUnhealthy()
    {
        var result = await CheckAsync(
            Status(running: true, leader: true, Table("dbo_Orders", failures: 3), Table("dbo_Customers")),
            new SqlCdcHealthCheckOptions { UnhealthyAfterConsecutiveFailures = 3 });

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("dbo_Orders", result.Description);
        Assert.DoesNotContain("dbo_Customers", result.Description);
    }

    [Fact]
    public async Task AStalePoll_IsDegraded_OnlyWhenAThresholdIsSet()
    {
        var stale = Status(
            running: true,
            leader: true,
            Table("dbo_Orders", lastPoll: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5)));

        Assert.Equal(HealthStatus.Healthy, (await CheckAsync(stale)).Status);

        var result = await CheckAsync(
            stale,
            new SqlCdcHealthCheckOptions { MaxTimeSinceLastPoll = TimeSpan.FromMinutes(1) });

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }

    [Fact]
    public async Task TheStatusIsReported_AsCheckData()
    {
        var result = await CheckAsync(Status(running: true, leader: true, Table("dbo_Orders", emitted: 7)));

        Assert.Equal("orders", result.Data["watcher"]);
        Assert.Equal(true, result.Data["isLeader"]);
        var table = Assert.IsType<Dictionary<string, object?>>(result.Data["dbo_Orders"]);
        Assert.Equal(7L, table["changesEmitted"]);
    }

    private static Task<HealthCheckResult> CheckAsync(
        CdcWatcherStatus status,
        SqlCdcHealthCheckOptions? options = null) =>
        new SqlCdcHealthCheck(() => status, options)
            .CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

    private static CdcWatcherStatus Status(bool running, bool leader, params CdcTableStatus[] tables) =>
        new("orders", running, leader, ChannelLength: 0, tables);

    private static CdcTableStatus Table(
        string captureInstance,
        int failures = 0,
        long emitted = 0,
        DateTimeOffset? lastPoll = null) =>
        new(captureInstance, "dbo", captureInstance, failures, lastPoll ?? DateTimeOffset.UtcNow, null, emitted);
}
