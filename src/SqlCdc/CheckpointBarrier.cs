namespace SqlCdc;

/// <summary>
/// Tracks the outstanding acknowledgements of a single batch. In
/// <see cref="CdcCheckpointMode.OnAcknowledgement"/> the poller waits on <see cref="Completion"/>
/// before persisting the batch's watermark, so a crash replays the batch instead of losing it.
/// </summary>
internal sealed class CheckpointBarrier
{
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Starts at one. The extra count stands for "the batch is still being published": without it
    // the counter would reach zero as soon as the consumer acknowledged the first change, while
    // the poller was still writing the rest of the batch to the channel.
    private int _outstanding = 1;

    /// <summary>Completes once every registered change has been acknowledged and the batch is sealed.</summary>
    public Task Completion => _completion.Task;

    /// <summary>
    /// Registers one change that must be acknowledged. Must be called before the change becomes
    /// visible to a consumer, otherwise the barrier can complete while changes are still in flight.
    /// </summary>
    public ChangeAcknowledgement Register()
    {
        Interlocked.Increment(ref _outstanding);
        return new ChangeAcknowledgement(this);
    }

    /// <summary>Signals that no further changes will be registered. An empty batch completes here.</summary>
    public void Seal() => Release();

    internal void Release()
    {
        if (Interlocked.Decrement(ref _outstanding) == 0)
        {
            _completion.TrySetResult();
        }
    }
}

/// <summary>
/// Per-change acknowledgement handle. Held by <see cref="CdcChange"/> as a reference so the
/// record keeps stable equality; acknowledging more than once counts once.
/// </summary>
internal sealed class ChangeAcknowledgement
{
    private readonly CheckpointBarrier _barrier;
    private int _acknowledged;

    internal ChangeAcknowledgement(CheckpointBarrier barrier) => _barrier = barrier;

    public void Acknowledge()
    {
        if (Interlocked.Exchange(ref _acknowledged, 1) == 0)
        {
            _barrier.Release();
        }
    }
}
