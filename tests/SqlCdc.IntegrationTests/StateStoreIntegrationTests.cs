namespace SqlCdc.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class StateStoreIntegrationTests
{
    private readonly SqlServerFixture _sql;

    public StateStoreIntegrationTests(SqlServerFixture sql) => _sql = sql;

    /// <summary>
    /// The check-then-write the store used to do let two writers both conclude the row was
    /// missing and both insert it, which surfaced as a primary key violation.
    /// </summary>
    [Fact]
    public async Task ConcurrentSaves_DoNotCollide_AndTheHighestLsnWins()
    {
        var store = new SqlCdcStateStore(_sql.ConnectionString, table: "cdc_watermark_race");
        const string captureInstance = "dbo_Race";

        var lsns = Enumerable.Range(1, 20).Select(Lsn).ToList();
        await Task.WhenAll(lsns.Select(lsn => store.SaveLastLsnAsync(captureInstance, lsn)));

        Assert.Equal(Lsn(20), await store.GetLastLsnAsync(captureInstance));
    }

    [Fact]
    public async Task AnOlderLsn_DoesNotRewindTheWatermark()
    {
        var store = new SqlCdcStateStore(_sql.ConnectionString, table: "cdc_watermark_monotonic");
        const string captureInstance = "dbo_Monotonic";

        await store.SaveLastLsnAsync(captureInstance, Lsn(5));

        // A watcher that lost its lease with a save already in flight must not drag the new leader
        // backwards: that would replay everything between the two LSNs.
        await store.SaveLastLsnAsync(captureInstance, Lsn(3));

        Assert.Equal(Lsn(5), await store.GetLastLsnAsync(captureInstance));

        await store.SaveLastLsnAsync(captureInstance, Lsn(6));
        Assert.Equal(Lsn(6), await store.GetLastLsnAsync(captureInstance));
    }

    [Fact]
    public async Task WithoutCreationRights_AMissingTableIsReportedClearly()
    {
        var store = new SqlCdcStateStore(
            _sql.ConnectionString,
            table: "cdc_watermark_never_created",
            createTableIfMissing: false);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.GetLastLsnAsync("dbo_Missing"));

        Assert.Contains("does not exist", error.Message);
        Assert.Contains("create-state-tables.sql", error.Message);
    }

    [Fact]
    public async Task WithoutCreationRights_AnExistingTableIsUsed()
    {
        const string table = "cdc_watermark_provisioned";
        await _sql.ExecuteAsync(
            $"""
             CREATE TABLE dbo.[{table}]
             (
                 CaptureInstance nvarchar(128) NOT NULL PRIMARY KEY,
                 LastLsn binary(10) NOT NULL,
                 UpdatedAt datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
             );
             """);

        var store = new SqlCdcStateStore(_sql.ConnectionString, table: table, createTableIfMissing: false);

        await store.SaveLastLsnAsync("dbo_Provisioned", Lsn(1));
        Assert.Equal(Lsn(1), await store.GetLastLsnAsync("dbo_Provisioned"));
    }

    private static byte[] Lsn(int value)
    {
        var lsn = new byte[10];
        lsn[9] = (byte)value;
        return lsn;
    }
}
