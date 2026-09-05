namespace SqlCdc;

/// <summary>A set of CDC rows read in one cycle, cut on a transaction (LSN) boundary.</summary>
/// <param name="Rows">Rows belonging to fully read LSN groups; safe to emit.</param>
/// <param name="FullyConsumedLsn">Highest LSN whose rows were all read, or <c>null</c> if none.</param>
/// <param name="HitCap">True when more rows are pending beyond this batch.</param>
/// <param name="PartialLsn">
/// LSN of the transaction that was cut by the cap and left out of <see cref="Rows"/>, or
/// <c>null</c> when every transaction read fitted in whole. Set with empty <see cref="Rows"/>, it
/// means the very first transaction in the range is larger than the batch size.
/// </param>
internal sealed record ChangeBatch(
    IReadOnlyList<RawRow> Rows,
    byte[]? FullyConsumedLsn,
    bool HitCap,
    byte[]? PartialLsn);

/// <summary>
/// Accumulates CDC rows and cuts a batch on a transaction boundary, so the before-image
/// (operation 3) and after-image (operation 4) of an update are never split across batches.
/// </summary>
/// <remarks>
/// The builder is fed at most one row past the batch size. That extra row is not kept: it proves
/// more data is pending, and its LSN tells whether the last transaction within the cap was read
/// completely (the extra row starts a new one) or was cut (it continues the same one). A cut
/// transaction is left out of the batch and reported through <see cref="ChangeBatch.PartialLsn"/>.
/// </remarks>
internal sealed class ChangeBatchBuilder
{
    private readonly int _batchSize;
    private readonly List<RawRow> _rows = new();
    private byte[]? _currentGroupLsn;
    private byte[]? _fullyConsumedLsn;
    private int _consumedBoundary;

    public ChangeBatchBuilder(int batchSize) => _batchSize = batchSize;

    /// <summary>True once a row past the batch size was seen, so more rows are pending.</summary>
    public bool HitCap { get; private set; }

    /// <summary>
    /// Adds a row. Returns <c>false</c> for the row past the batch size, which is not kept and
    /// marks the batch as capped; reading should stop there.
    /// </summary>
    public bool Add(RawRow row)
    {
        if (HitCap)
        {
            throw new InvalidOperationException("The batch is already capped; no further rows can be added.");
        }

        if (_currentGroupLsn is null)
        {
            _currentGroupLsn = row.Lsn;
        }
        else if (!row.Lsn.AsSpan().SequenceEqual(_currentGroupLsn))
        {
            _fullyConsumedLsn = _currentGroupLsn;
            _consumedBoundary = _rows.Count;
            _currentGroupLsn = row.Lsn;
        }

        if (_rows.Count >= _batchSize)
        {
            HitCap = true;
            return false;
        }

        _rows.Add(row);
        return true;
    }

    /// <summary>Closes the batch, dropping a trailing transaction the cap cut in the middle.</summary>
    public ChangeBatch Build()
    {
        if (!HitCap && _currentGroupLsn is not null)
        {
            _fullyConsumedLsn = _currentGroupLsn;
            _consumedBoundary = _rows.Count;
        }

        // With the cap hit, the rows after the boundary belong to the transaction the extra row
        // continued: it was cut and is re-read next time. No such rows means the extra row started
        // a new transaction and everything kept is complete.
        var partialLsn = HitCap && _consumedBoundary < _rows.Count ? _currentGroupLsn : null;

        return new ChangeBatch(_rows.Take(_consumedBoundary).ToList(), _fullyConsumedLsn, HitCap, partialLsn);
    }
}
