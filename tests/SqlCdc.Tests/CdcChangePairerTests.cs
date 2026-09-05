namespace SqlCdc.Tests;

using Microsoft.Extensions.Logging;

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
    public void Update_WithoutBeforeImage_FailsWithoutAdvancingTheCheckpoint()
    {
        var after = Row(4, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 12.50m)));

        Assert.Throws<InvalidOperationException>(() => Pair(after));
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

    // --- __$update_mask su più di un byte -------------------------------------------------
    //
    // La maschera è indicizzata DALLA FINE: l'ordinale 1 è il bit meno significativo
    // dell'ULTIMO byte dell'array. Lo dice l'implementazione di sys.fn_cdc_is_bit_set:
    //
    //     SUBSTRING(@update_mask, DATALENGTH(@update_mask) - ((@position - 1) / 8), 1)
    //         & POWER(2, (@position - 1) % 8)
    //
    // Con al più 8 colonne catturate la maschera occupa un solo byte e indicizzare
    // dall'inizio o dalla fine è equivalente: è la ragione per cui gli altri test
    // (tabelle da 3 colonne) non intercettano l'inversione. Da 9 colonne in su i byte
    // sono due e l'ordine conta.

    private static readonly string[] NineColumns =
        { "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9" };

    /// <summary>Costruisce una maschera a 2 byte per 9 colonne, con il layout usato da SQL Server.</summary>
    private static byte[] NineColumnMask(params int[] changedOrdinals)
    {
        var mask = new byte[2];
        foreach (var ordinal in changedOrdinals)
        {
            var byteIndex = mask.Length - 1 - ((ordinal - 1) / 8);
            mask[byteIndex] |= (byte)(1 << ((ordinal - 1) % 8));
        }

        return mask;
    }

    private static CdcChange PairWideUpdate(byte[] mask)
    {
        var values = Values(NineColumns.Select(c => (c, (object?)c)).ToArray());
        var before = Row(3, values: values);
        var after = Row(4, seqVal: before.SeqVal, values: values, mask: mask);

        return Assert.Single(
            CdcChangePairer.Pair("dbo", "Wide", "dbo_Wide", NineColumns, new[] { before, after },
                    new Dictionary<string, DateTime>())
                .ToList());
    }

    [Fact]
    public void UpdateMask_NineColumns_ReadsFirstColumnFromTheLastByte()
    {
        // Modificata solo C1 (ordinale 1) => 0x00 0x01.
        var mask = NineColumnMask(1);
        Assert.Equal(new byte[] { 0x00, 0x01 }, mask);

        var change = PairWideUpdate(mask);

        Assert.True(change.UpdateMask["C1"]);
        foreach (var column in NineColumns.Skip(1))
        {
            Assert.False(change.UpdateMask[column]);
        }
    }

    [Fact]
    public void UpdateMask_NineColumns_ReadsNinthColumnFromTheFirstByte()
    {
        // Modificata solo C9 (ordinale 9) => 0x01 0x00.
        var mask = NineColumnMask(9);
        Assert.Equal(new byte[] { 0x01, 0x00 }, mask);

        var change = PairWideUpdate(mask);

        Assert.True(change.UpdateMask["C9"]);
        foreach (var column in NineColumns.Take(8))
        {
            Assert.False(change.UpdateMask[column]);
        }
    }

    [Fact]
    public void UpdateMask_NineColumns_HandlesBitsInBothBytes()
    {
        // Modificate C2, C8 e C9 => 0x01 0x82.
        var mask = NineColumnMask(2, 8, 9);
        Assert.Equal(new byte[] { 0x01, 0b1000_0010 }, mask);

        var change = PairWideUpdate(mask);

        var expected = new Dictionary<string, bool>
        {
            ["C1"] = false,
            ["C2"] = true,
            ["C3"] = false,
            ["C4"] = false,
            ["C5"] = false,
            ["C6"] = false,
            ["C7"] = false,
            ["C8"] = true,
            ["C9"] = true,
        };

        foreach (var (column, wasUpdated) in expected)
        {
            Assert.Equal(wasUpdated, change.UpdateMask[column]);
        }
    }

    [Fact]
    public void UpdateMask_SingleByteMask_StaysCorrect()
    {
        // Guardia di regressione: con maschera a un byte solo il comportamento non cambia.
        var before = Row(3, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var after = Row(4, seqVal: before.SeqVal, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 12.50m)),
            mask: new byte[] { 0b0000_0100 });

        var change = Assert.Single(Pair(before, after));

        Assert.False(change.UpdateMask["Id"]);
        Assert.False(change.UpdateMask["Name"]);
        Assert.True(change.UpdateMask["Price"]);
    }

    // --- __$operation values outside 1-4 ---------------------------------------------

    [Fact]
    public void UnknownOperation_FailsWithoutAdvancingTheCheckpoint()
    {
        var row = Row(5, values: Values(("Id", 1)));

        Assert.Throws<InvalidOperationException>(() => Pair(row));
    }

    [Fact]
    public void UnknownOperation_StopsPairing()
    {
        var before = Row(3, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));
        var unknown = Row(5, seqVal: before.SeqVal, values: Values(("Id", 1)));
        var after = Row(4, seqVal: before.SeqVal, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 12.50m)),
            mask: new byte[] { 0b0000_0100 });

        Assert.Throws<InvalidOperationException>(() => Pair(before, unknown, after));
    }

    [Fact]
    public void OrphanBeforeImage_FailsWithoutAdvancingTheCheckpoint()
    {
        var logger = new RecordingLogger();
        var before = Row(3, values: Values(("Id", 1), ("Name", "Widget"), ("Price", 9.99m)));

        Assert.Throws<InvalidOperationException>(() => Pair(logger, before));
    }

    [Fact]
    public void UnknownOperation_FailsAtTheFirstInvalidRow()
    {
        var logger = new RecordingLogger();
        var first = Row(5, seqVal: new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, values: Values(("Id", 1)));
        var second = Row(5, seqVal: new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 2 }, values: Values(("Id", 2)));

        Assert.Throws<InvalidOperationException>(() => Pair(logger, first, second));
    }

    private static List<CdcChange> Pair(ILogger logger, params RawRow[] rows) =>
        CdcChangePairer.Pair("dbo", "Orders", "dbo_Orders", Columns, rows, new Dictionary<string, DateTime>(), logger)
            .ToList();

    private sealed class RecordingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
