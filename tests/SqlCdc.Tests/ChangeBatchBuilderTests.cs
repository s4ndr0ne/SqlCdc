namespace SqlCdc.Tests;

public class ChangeBatchBuilderTests
{
    private static byte[] Lsn(byte value) =>
        new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, value };

    private static RawRow Row(byte lsn, int operation = 2) =>
        new(Lsn(lsn), Lsn(lsn), operation, new byte[] { 0x00 }, new Dictionary<string, object?>());

    /// <summary>
    /// Feeds rows until the builder asks to stop; returns the closed batch. Like the watcher, the
    /// caller is expected to hand over at most one row past the batch size.
    /// </summary>
    private static ChangeBatch Read(int batchSize, params RawRow[] rows)
    {
        var builder = new ChangeBatchBuilder(batchSize);
        foreach (var row in rows)
        {
            if (!builder.Add(row))
            {
                break;
            }
        }

        return builder.Build();
    }

    [Fact]
    public void EmptyResultSet_ProducesEmptyBatch()
    {
        var batch = Read(10);

        Assert.Empty(batch.Rows);
        Assert.Null(batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
        Assert.Null(batch.PartialLsn);
    }

    [Fact]
    public void AllRowsRead_EmitsEverything_AndReportsNoCap()
    {
        var batch = Read(10, Row(1), Row(1), Row(2));

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn(2), batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
        Assert.Null(batch.PartialLsn);
    }

    [Fact]
    public void ExactlyBatchSizeRows_IsNotCapped()
    {
        // No row past the cap was seen, so nothing proves more data is pending.
        var batch = Read(3, Row(1), Row(2), Row(2));

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn(2), batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
        Assert.Null(batch.PartialLsn);
    }

    [Fact]
    public void CapReached_EmitsOnlyCompleteTransactions()
    {
        // Cap of 2 is reached inside LSN 2, so only LSN 1 is emitted and LSN 2 is left for the next cycle.
        var batch = Read(2, Row(1), Row(2), Row(2), Row(3));

        Assert.Single(batch.Rows);
        Assert.Equal(Lsn(1), batch.Rows[0].Lsn);
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
        Assert.Equal(Lsn(2), batch.PartialLsn);
    }

    [Fact]
    public void ExtraRowStartingANewTransaction_KeepsTheWholeBatch()
    {
        // The row past the cap belongs to LSN 3, which proves LSN 2 was read completely: every
        // kept row is emitted and nothing is reported as cut.
        var batch = Read(3, Row(1), Row(2), Row(2), Row(3));

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn(2), batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
        Assert.Null(batch.PartialLsn);
    }

    [Fact]
    public void TransactionLargerThanBatchSize_IsReportedAsPartial_WithNoProgress()
    {
        // The builder cannot read past the cap on its own: it reports which transaction was cut so
        // the watcher can read exactly that one in full, instead of re-reading the same rows forever.
        var batch = Read(2, Row(1), Row(1), Row(1), Row(1));

        Assert.Empty(batch.Rows);
        Assert.Null(batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
        Assert.Equal(Lsn(1), batch.PartialLsn);
    }

    [Fact]
    public void UncappedRead_TakesAnOversizedTransactionInFull()
    {
        // How the watcher reads the transaction reported above: no cap, range bounded to its LSN.
        var batch = Read(int.MaxValue, Row(1), Row(1), Row(1), Row(1));

        Assert.Equal(4, batch.Rows.Count);
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
        Assert.Null(batch.PartialLsn);
    }

    [Fact]
    public void TrailingPartialTransaction_IsNotEmitted()
    {
        // Update before/after images share an LSN and must never be split across batches.
        var batch = Read(2, Row(1), Row(2, operation: 3), Row(2, operation: 4), Row(3));

        Assert.Single(batch.Rows);
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
        Assert.Equal(Lsn(2), batch.PartialLsn);
    }

    [Fact]
    public void AddingPastTheCap_Throws()
    {
        var builder = new ChangeBatchBuilder(1);
        Assert.True(builder.Add(Row(1)));
        Assert.False(builder.Add(Row(2)));

        Assert.Throws<InvalidOperationException>(() => builder.Add(Row(3)));
    }
}
