namespace SqlCdc;

/// <summary>
/// A single change captured from a SQL Server CDC capture instance.
/// Emitted by <see cref="SqlCdcWatcher"/> onto its <c>Channel&lt;CdcChange&gt;</c>.
/// </summary>
public sealed record CdcChange
{
    /// <summary>The CDC capture instance that produced this change (e.g. <c>dbo_Orders</c>).</summary>
    public required string CaptureInstance { get; init; }

    /// <summary>Schema of the source table.</summary>
    public required string SourceSchema { get; init; }

    /// <summary>Name of the source table.</summary>
    public required string SourceTable { get; init; }

    /// <summary>Kind of operation (insert, update, delete).</summary>
    public required CdcOperationType Operation { get; init; }

    /// <summary>Transaction log sequence number (LSN) at which this change was committed.</summary>
    public required byte[] StartLsn { get; init; }

    /// <summary>Sequence value within the LSN that uniquely identifies the change.</summary>
    public required byte[] SeqVal { get; init; }

    /// <summary>Commit time of the transaction, mapped from the LSN.</summary>
    public DateTime CommitTime { get; init; }

    /// <summary>Column values as they were before the operation. Empty for inserts.</summary>
    public required IReadOnlyDictionary<string, object?> Before { get; init; }

    /// <summary>Column values as they are after the operation. Empty for deletes.</summary>
    public required IReadOnlyDictionary<string, object?> After { get; init; }

    /// <summary>For updates, which columns were actually modified (keyed by column name).</summary>
    public required IReadOnlyDictionary<string, bool> UpdateMask { get; init; }

    /// <summary>
    /// Acknowledgement handle, set only when the watcher runs in
    /// <see cref="CdcCheckpointMode.OnAcknowledgement"/>.
    /// </summary>
    internal ChangeAcknowledgement? Acknowledgement { get; init; }

    /// <summary>Stable per-change identifier, combining LSN and sequence value.</summary>
    public string Key => $"{Convert.ToHexString(StartLsn)}-{Convert.ToHexString(SeqVal)}";

    /// <summary>Fully qualified source table name, e.g. <c>[dbo].[Orders]</c>.</summary>
    public string TableName => $"[{SourceSchema}].[{SourceTable}]";

    /// <summary>
    /// Marks this change as processed. Required for every change when the watcher runs in
    /// <see cref="CdcCheckpointMode.OnAcknowledgement"/> — the watermark only advances past a
    /// batch once all of its changes are acknowledged, so a skipped call stalls polling. A no-op
    /// in <see cref="CdcCheckpointMode.OnEmit"/>, and acknowledging twice counts once.
    /// </summary>
    public void Acknowledge() => Acknowledgement?.Acknowledge();
}
