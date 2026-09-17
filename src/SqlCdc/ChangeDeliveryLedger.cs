namespace SqlCdc;

/// <summary>
/// Tracks delivery completion for one CDC batch. The watermark can advance only after the batch
/// has been sealed and every registered change has been acknowledged.
/// </summary>
internal sealed class ChangeDeliveryLedger
{
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _outstanding = 1;

    public Task Completion => _completion.Task;

    public int Outstanding => Math.Max(Volatile.Read(ref _outstanding), 0);

    public ChangeDeliveryReceipt Register()
    {
        Interlocked.Increment(ref _outstanding);
        return new ChangeDeliveryReceipt(this);
    }

    public void Seal() => Release();

    internal void Release()
    {
        if (Interlocked.Decrement(ref _outstanding) == 0)
        {
            _completion.TrySetResult();
        }
    }
}

/// <summary>Idempotent acknowledgement receipt held by a delivered <see cref="CdcChange"/>.</summary>
internal sealed class ChangeDeliveryReceipt
{
    private readonly ChangeDeliveryLedger _ledger;
    private int _acknowledged;

    internal ChangeDeliveryReceipt(ChangeDeliveryLedger ledger) => _ledger = ledger;

    public void Acknowledge()
    {
        if (Interlocked.Exchange(ref _acknowledged, 1) == 0)
        {
            _ledger.Release();
        }
    }
}