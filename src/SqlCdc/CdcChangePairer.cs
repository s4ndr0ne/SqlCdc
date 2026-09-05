namespace SqlCdc;

using Microsoft.Extensions.Logging;

/// <summary>
/// Pairs CDC log rows (operations 3+4) into single update events and maps
/// raw rows to <see cref="CdcChange"/> events.
/// </summary>
/// <remarks>
/// Pairing does not depend on the order rows arrive in within a transaction: an after-image
/// that shows up before its before-image is held until the pair is complete. The rows of one
/// transaction only have to be in the same batch, which <see cref="ChangeBatchBuilder"/>
/// guarantees. This is what lets the poller order its query on the start LSN alone and read the
/// change table in index order rather than sorting the range on every poll.
/// </remarks>
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
        IReadOnlyDictionary<string, DateTime> timeMap,
        ILogger? logger = null)
    {
        var pendingBefore = new Dictionary<string, RawRow>(StringComparer.OrdinalIgnoreCase);
        var pendingAfter = new Dictionary<string, RawRow>(StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            var key = Convert.ToHexString(row.SeqVal);

            switch (row.Operation)
            {
                case 1: // delete
                    yield return Create(schema, table, captureInstance, row, CdcOperationType.Delete, row.Values, EmptyValues, EmptyMask, CommitTimeOf(row, timeMap));
                    break;

                case 2: // insert
                    yield return Create(schema, table, captureInstance, row, CdcOperationType.Insert, EmptyValues, row.Values, EmptyMask, CommitTimeOf(row, timeMap));
                    break;

                case 3: // update before-image
                    if (pendingAfter.Remove(key, out var after))
                    {
                        yield return CreateUpdate(schema, table, captureInstance, capturedColumns, row, after, timeMap);
                    }
                    else
                    {
                        pendingBefore[key] = row;
                    }

                    break;

                case 4: // update after-image
                    if (pendingBefore.Remove(key, out var before))
                    {
                        yield return CreateUpdate(schema, table, captureInstance, capturedColumns, before, row, timeMap);
                    }
                    else
                    {
                        pendingAfter[key] = row;
                    }

                    break;

                default:
                    throw new InvalidOperationException(
                        $"Capture instance '{captureInstance}' produced unsupported __$operation value {row.Operation}. " +
                        "The checkpoint was not advanced, so no change is silently lost.");
            }
        }

        // Both images of an update belong to the same transaction, and batches are cut on
        // transaction boundaries, so leftovers mean the source produced something unexpected.
        if (pendingBefore.Count > 0 || pendingAfter.Count > 0)
        {
            throw new InvalidOperationException(
                $"Capture instance '{captureInstance}' produced {pendingBefore.Count} update before-image row(s) " +
                $"without a matching after-image and {pendingAfter.Count} after-image row(s) without a matching " +
                "before-image. The checkpoint was not advanced, so no change is silently lost.");
        }
    }

    private static DateTime CommitTimeOf(RawRow row, IReadOnlyDictionary<string, DateTime> timeMap) =>
        timeMap.TryGetValue(Convert.ToHexString(row.Lsn), out var t) ? t : DateTime.MinValue;

    private static CdcChange CreateUpdate(
        string schema,
        string table,
        string captureInstance,
        IReadOnlyList<string> capturedColumns,
        RawRow before,
        RawRow after,
        IReadOnlyDictionary<string, DateTime> timeMap) =>
        Create(
            schema, table, captureInstance, after, CdcOperationType.Update,
            before.Values,
            after.Values,
            ComputeMask(capturedColumns, after.UpdateMask),
            CommitTimeOf(after, timeMap));

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
            // __$update_mask is indexed from the end: ordinal 1 is the least significant bit of
            // the last byte of the array (see sys.fn_cdc_is_bit_set).
            var byteIndex = mask.Length - 1 - (i / 8);
            var updated = byteIndex >= 0 && ((mask[byteIndex] >> (i % 8)) & 1) == 1;
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
