namespace SqlCdc.Tests;

public class ChangeBatchBuilderTests
{
    private static byte[] Lsn(byte value) =>
        new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, value };

    private static RawRow Row(byte lsn, int operation = 2) =>
        new(Lsn(lsn), Lsn(lsn), operation, new byte[] { 0x00 }, new Dictionary<string, object?>());

    /// <summary>Feeds rows until the builder asks to stop; returns the closed batch.</summary>
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
    }

    [Fact]
    public void AllRowsRead_EmitsEverything_AndReportsNoCap()
    {
        var batch = Read(10, Row(1), Row(1), Row(2));

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn(2), batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
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
    }

    [Fact]
    public void TransactionLargerThanBatchSize_IsReadInFull()
    {
        // Regression: stopping on the cap before any LSN group completed produced an empty batch
        // with a null watermark, so the watcher re-read the same rows forever without progressing.
        var batch = Read(2, Row(1), Row(1), Row(1), Row(1));

        Assert.Equal(4, batch.Rows.Count);
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.False(batch.HitCap);
    }

    [Fact]
    public void OversizedTransaction_FollowedByAnother_StillMakesProgress()
    {
        var batch = Read(2, Row(1), Row(1), Row(1), Row(2), Row(2));

        Assert.Equal(3, batch.Rows.Count);
        Assert.All(batch.Rows, r => Assert.Equal(Lsn(1), r.Lsn));
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
    }

    [Fact]
    public void TrailingPartialTransaction_IsNotEmitted()
    {
        // Update before/after images share an LSN and must never be split across batches.
        var batch = Read(3, Row(1), Row(2, operation: 3), Row(2, operation: 4), Row(3));

        Assert.Single(batch.Rows);
        Assert.Equal(Lsn(1), batch.FullyConsumedLsn);
        Assert.True(batch.HitCap);
    }
}
