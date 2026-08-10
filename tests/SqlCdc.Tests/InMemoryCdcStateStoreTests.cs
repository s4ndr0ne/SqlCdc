namespace SqlCdc.Tests;

public class InMemoryCdcStateStoreTests
{
    [Fact]
    public async Task GetLastLsnAsync_ReturnsNull_WhenNothingSaved()
    {
        var store = new InMemoryCdcStateStore();
        Assert.Null(await store.GetLastLsnAsync("dbo_Orders"));
    }

    [Fact]
    public async Task SaveAndGet_RoundTrips()
    {
        var store = new InMemoryCdcStateStore();
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05 };

        await store.SaveLastLsnAsync("dbo_Orders", lsn);
        var result = await store.GetLastLsnAsync("dbo_Orders");

        Assert.NotNull(result);
        Assert.Equal(lsn, result);
    }

    [Fact]
    public async Task SaveLastLsnAsync_OverwritesPrevious()
    {
        var store = new InMemoryCdcStateStore();
        await store.SaveLastLsnAsync("dbo_Orders", new byte[10]);
        await store.SaveLastLsnAsync("dbo_Orders", new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07 });

        var result = await store.GetLastLsnAsync("dbo_Orders");
        Assert.Equal(0x07, result![^1]);
    }

    [Fact]
    public async Task CaptureInstances_AreIndependent()
    {
        var store = new InMemoryCdcStateStore();
        await store.SaveLastLsnAsync("dbo_Orders", new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 });
        await store.SaveLastLsnAsync("dbo_Customers", new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 });

        Assert.Equal(0x01, (await store.GetLastLsnAsync("dbo_Orders"))![^1]);
        Assert.Equal(0x02, (await store.GetLastLsnAsync("dbo_Customers"))![^1]);
    }
}
