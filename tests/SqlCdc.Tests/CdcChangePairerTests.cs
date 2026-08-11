namespace SqlCdc.Tests;

public class CdcChangePairerTests
{
    private static RawRow Row(int operation, byte[]? seqVal = null, Dictionary<string, object?>? values = null, byte[]? mask = null)
    {
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10 };
        return new RawRow(
            lsn,
            seqVal ?? new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20 },
            operation,
            mask ?? new byte[] { 0x00, 0x00 },
            values ?? new Dictionary<string, object?>());
    }

    private static Dictionary<string, object?> Values(params (string Name, object? Value)[] entries) =>
        entries.ToDictionary(e => e.Name, e => e.Value, StringComparer.OrdinalIgnoreCase);

    private static readonly string[] Columns = { "Id", "Name", "Price" };

    private static List<CdcChange> Pair(params RawRow[] rows) =>
        CdcChangePairer.Pair("dbo", "Orders", "dbo_Orders", Columns, rows, new Dictionary<string, DateTime>())
            .ToList();

    [Fact]
    public void Insert_ProducesAfterValues_AndEmptyBefore()
    {
        var row = Row(2, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var changes = Pair(row);

        var change = Assert.Single(changes);
        Assert.Equal(CdcOperationType.Insert, change.Operation);
        Assert.Equal("dbo", change.SourceSchema);
        Assert.Equal("Orders", change.SourceTable);
        Assert.Equal("dbo_Orders", change.CaptureInstance);
        Assert.Equal(1, change.After["Id"]);
        Assert.Equal("Widget", change.After["Name"]);
        Assert.Empty(change.Before);
    }

    [Fact]
    public void Delete_ProducesBeforeValues_AndEmptyAfter()
    {
        var row = Row(1, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var change = Assert.Single(Pair(row));

        Assert.Equal(CdcOperationType.Delete, change.Operation);
        Assert.Equal(1, change.Before["Id"]);
        Assert.Empty(change.After);
    }

    [Fact]
    public void Update_PairsBeforeAndAfterImages()
    {
        var before = Row(3, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var after = Row(4, seqVal: before.SeqVal, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 12.50m)),
            mask: new byte[] { 0b0000_0100 });

        var change = Assert.Single(Pair(before, after));

        Assert.Equal(CdcOperationType.Update, change.Operation);
        Assert.Equal(9.99m, change.Before["Price"]);
        Assert.Equal(12.50m, change.After["Price"]);
        Assert.False(change.UpdateMask["Id"]);
        Assert.False(change.UpdateMask["Name"]);
        Assert.True(change.UpdateMask["Price"]);
    }

    [Fact]
    public void Update_WithoutBeforeImage_HasEmptyBefore()
    {
        var after = Row(4, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 12.50m)));

        var change = Assert.Single(Pair(after));

        Assert.Equal(CdcOperationType.Update, change.Operation);
        Assert.Empty(change.Before);
        Assert.Equal(12.50m, change.After["Price"]);
    }

    [Fact]
    public void Key_IsStableForSameLsnAndSeqVal()
    {
        var row = Row(2, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var first = Assert.Single(Pair(row));
        var second = Assert.Single(Pair(row));

        Assert.Equal(first.Key, second.Key);
    }

    [Fact]
    public void CommitTime_IsMappedFromTimeMap()
    {
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10 };
        var time = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var row = new RawRow(
            lsn,
            new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20 },
            2,
            new byte[] { 0x00, 0x00 },
            Values(("Id", 1)));

        var changes = CdcChangePairer.Pair("dbo", "Orders", "dbo_Orders", Columns, new[] { row },
                new Dictionary<string, DateTime> { [Convert.ToHexString(lsn)] = time })
            .ToList();

        Assert.Equal(time, Assert.Single(changes).CommitTime);
    }

    [Fact]
    public void StartLsn_And_SeqVal_AreDefensiveCopies()
    {
        var row = Row(2, values: Values(("Id", 1)));
        var change = Assert.Single(Pair(row));

        Assert.NotSame(row.Lsn, change.StartLsn);
        Assert.NotSame(row.SeqVal, change.SeqVal);

        change.StartLsn[^1] = 0xFF;
        change.SeqVal[^1] = 0xFF;
        Assert.Equal(0x10, row.Lsn[^1]);
        Assert.Equal(0x20, row.SeqVal[^1]);
    }
}
