namespace SqlCdc;

/// <summary>A set of CDC rows read in one cycle, cut on a transaction (LSN) boundary.</summary>
/// <param name="Rows">Rows belonging to fully read LSN groups; safe to emit.</param>
/// <param name="FullyConsumedLsn">Highest LSN whose rows were all read, or <c>null</c> if none.</param>
/// <param name="HitCap">True when reading stopped on the batch size cap, so more rows are pending.</param>
internal sealed record ChangeBatch(
    IReadOnlyList<RawRow> Rows,
    byte[]? FullyConsumedLsn,
    bool HitCap);

/// <summary>
/// Accumulates CDC rows and cuts a batch on a transaction boundary, so the before-image
/// (operation 3) and after-image (operation 4) of an update are never split across batches.
/// </summary>
internal sealed class ChangeBatchBuilder
{
    private readonly int _batchSize;
    private readonly List<RawRow> _rows = new();
    private byte[]? _currentGroupLsn;
    private byte[]? _fullyConsumedLsn;
    private int _consumedBoundary;

    public ChangeBatchBuilder(int batchSize) => _batchSize = batchSize;

    /// <summary>True when the last <see cref="Add"/> stopped on the batch size cap.</summary>
    public bool HitCap { get; private set; }

    /// <summary>
    /// Adds a row. Returns <c>false</c> when the cap is reached and reading should stop.
    /// </summary>
    public bool Add(RawRow row)
    {
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

        _rows.Add(row);

        // The cap only applies once an LSN group is complete. Stopping earlier would produce an
        // empty batch and leave the watermark untouched, so a transaction larger than the batch
        // size would be re-read forever without progressing. The batch size is a soft cap: a
        // single transaction is always read in full.
        if (_rows.Count >= _batchSize && _fullyConsumedLsn is not null)
        {
            HitCap = true;
            return false;
        }

        return true;
    }

    /// <summary>Closes the batch, dropping any trailing partially read LSN group.</summary>
    public ChangeBatch Build()
    {
        if (!HitCap && _currentGroupLsn is not null)
        {
            _fullyConsumedLsn = _currentGroupLsn;
            _consumedBoundary = _rows.Count;
        }

        return new ChangeBatch(_rows.Take(_consumedBoundary).ToList(), _fullyConsumedLsn, HitCap);
    }
}
