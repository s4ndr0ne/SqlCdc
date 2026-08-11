namespace SqlCdc;

/// <summary>
/// Pairs CDC log rows (operations 3+4) into single update events and maps
/// raw rows to <see cref="CdcChange"/> events.
/// </summary>
internal static class CdcChangePairer
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyValues =
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

    private static readonly IReadOnlyDictionary<string, bool> EmptyMask =
        new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

    public static IEnumerable<CdcChange> Pair(
        string schema,
        string table,
        string captureInstance,
        IReadOnlyList<string> capturedColumns,
        IReadOnlyList<RawRow> rows,
        IReadOnlyDictionary<string, DateTime> timeMap)
    {
        var pendingBefore = new Dictionary<string, RawRow>(StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            var key = Convert.ToHexString(row.SeqVal);
            var commitTime = timeMap.TryGetValue(Convert.ToHexString(row.Lsn), out var t) ? t : DateTime.MinValue;

            switch (row.Operation)
            {
                case 1: // delete
                    yield return Create(schema, table, captureInstance, row, CdcOperationType.Delete, row.Values, EmptyValues, EmptyMask, commitTime);
                    break;

                case 2: // insert
                    yield return Create(schema, table, captureInstance, row, CdcOperationType.Insert, EmptyValues, row.Values, EmptyMask, commitTime);
                    break;

                case 3: // update before-image
                    pendingBefore[key] = row;
                    break;

                case 4: // update after-image
                    pendingBefore.Remove(key, out var before);
                    yield return Create(
                        schema, table, captureInstance, row, CdcOperationType.Update,
                        before?.Values ?? EmptyValues,
                        row.Values,
                        ComputeMask(capturedColumns, row.UpdateMask),
                        commitTime);
                    break;
            }
        }
    }

    private static CdcChange Create(
        string schema,
        string table,
        string captureInstance,
        RawRow row,
        CdcOperationType operation,
        IReadOnlyDictionary<string, object?> before,
        IReadOnlyDictionary<string, object?> after,
        IReadOnlyDictionary<string, bool> mask,
        DateTime commitTime) =>
        new()
        {
            CaptureInstance = captureInstance,
            SourceSchema = schema,
            SourceTable = table,
            Operation = operation,
            // Copy the arrays: the row's LSN is also the batch's watermark reference, and the
            // change is handed to consumers as soon as it is written to the channel. Aliasing
            // would let a consumer mutate the persisted watermark through CdcChange.StartLsn.
            StartLsn = (byte[])row.Lsn.Clone(),
            SeqVal = (byte[])row.SeqVal.Clone(),
            CommitTime = commitTime,
            Before = before,
            After = after,
            UpdateMask = mask,
        };

    private static IReadOnlyDictionary<string, bool> ComputeMask(IReadOnlyList<string> columns, byte[] mask)
    {
        var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            var bitIndex = i % 8;
            var byteIndex = i / 8;
            var updated = byteIndex < mask.Length && ((mask[byteIndex] >> bitIndex) & 1) == 1;
            result[columns[i]] = updated;
        }

        return result;
    }
}

/// <summary>Internal representation of a single CDC log row.</summary>
internal sealed record RawRow(
    byte[] Lsn,
    byte[] SeqVal,
    int Operation,
    byte[] UpdateMask,
    IReadOnlyDictionary<string, object?> Values);
