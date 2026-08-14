namespace SqlCdc.Tests;

public class CheckpointBarrierTests
{
    [Fact]
    public void EmptyBatch_CompletesOnSeal()
    {
        var barrier = new CheckpointBarrier();
        Assert.False(barrier.Completion.IsCompleted);

        barrier.Seal();

        Assert.True(barrier.Completion.IsCompleted);
    }

    [Fact]
    public void Batch_DoesNotComplete_UntilEveryChangeIsAcknowledged()
    {
        var barrier = new CheckpointBarrier();
        var first = barrier.Register();
        var second = barrier.Register();
        barrier.Seal();

        Assert.False(barrier.Completion.IsCompleted);

        first.Acknowledge();
        Assert.False(barrier.Completion.IsCompleted);

        second.Acknowledge();
        Assert.True(barrier.Completion.IsCompleted);
    }

    [Fact]
    public void Batch_DoesNotComplete_BeforeItIsSealed()
    {
        var barrier = new CheckpointBarrier();

        // The consumer can drain the channel faster than the poller writes to it. Completing here
        // would let the watermark jump ahead of changes that have not been published yet.
        barrier.Register().Acknowledge();
        Assert.False(barrier.Completion.IsCompleted);

        barrier.Seal();
        Assert.True(barrier.Completion.IsCompleted);
    }

    [Fact]
    public void AcknowledgingTwice_CountsOnce()
    {
        var barrier = new CheckpointBarrier();
        var first = barrier.Register();
        barrier.Register();
        barrier.Seal();

        first.Acknowledge();
        first.Acknowledge();

        Assert.False(barrier.Completion.IsCompleted);
    }

    [Fact]
    public void ChangeAcknowledge_WithoutABarrier_IsANoOp()
    {
        // What every change looks like in OnEmit mode.
        CreateChange().Acknowledge();
    }

    [Fact]
    public void ChangeAcknowledge_ReleasesTheBarrier()
    {
        var barrier = new CheckpointBarrier();
        var change = CreateChange() with { Acknowledgement = barrier.Register() };
        barrier.Seal();

        Assert.False(barrier.Completion.IsCompleted);

        change.Acknowledge();
        change.Acknowledge();

        Assert.True(barrier.Completion.IsCompleted);
    }

    private static CdcChange CreateChange() => new()
    {
        CaptureInstance = "dbo_Orders",
        SourceSchema = "dbo",
        SourceTable = "Orders",
        Operation = CdcOperationType.Insert,
        StartLsn = new byte[10],
        SeqVal = new byte[10],
        Before = new Dictionary<string, object?>(),
        After = new Dictionary<string, object?>(),
        UpdateMask = new Dictionary<string, bool>(),
    };
}
