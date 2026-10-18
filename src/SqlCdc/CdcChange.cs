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
    internal ChangeDeliveryReceipt? Acknowledgement { get; init; }

    /// <summary>Stable per-change identifier, combining LSN and sequence value.</summary>
    public string Key => $"{Convert.ToHexString(StartLsn)}-{Convert.ToHexString(SeqVal)}";

    /// <summary>Fully qualified source table name, e.g. <c>[dbo].[Orders]</c>.</summary>
    public string TableName => $"[{SourceSchema}].[{SourceTable}]";

    /// <summary>
    /// OpenTelemetry activity context from the polling cycle that extracted this change,
    /// used to link the poller span to downstream handler spans.
    /// </summary>
    public System.Diagnostics.ActivityContext? TraceContext { get; init; }

    /// <summary>
    /// Deserializes the <see cref="After"/> column values into a strongly-typed instance of <typeparamref name="T"/>.
    /// Returns default/null for delete operations where <see cref="After"/> is empty.
    /// </summary>
    public T? GetAfter<T>() where T : class, new() => MapDictionary<T>(After);

    /// <summary>
    /// Deserializes the <see cref="Before"/> column values into a strongly-typed instance of <typeparamref name="T"/>.
    /// Returns default/null for insert operations where <see cref="Before"/> is empty.
    /// </summary>
    public T? GetBefore<T>() where T : class, new() => MapDictionary<T>(Before);

    private static T? MapDictionary<T>(IReadOnlyDictionary<string, object?> dict) where T : class, new()
    {
        if (dict.Count == 0)
        {
            return null;
        }

        var instance = new T();
        var properties = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        foreach (var prop in properties)
        {
            if (!prop.CanWrite)
            {
                continue;
            }

            if (!dict.TryGetValue(prop.Name, out var rawValue) || rawValue is null or DBNull)
            {
                continue;
            }

            var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            try
            {
                if (targetType.IsInstanceOfType(rawValue))
                {
                    prop.SetValue(instance, rawValue);
                }
                else if (targetType == typeof(Guid))
                {
                    if (rawValue is byte[] bytes && bytes.Length == 16)
                    {
                        prop.SetValue(instance, new Guid(bytes));
                    }
                    else if (rawValue is string s && Guid.TryParse(s, out var guid))
                    {
                        prop.SetValue(instance, guid);
                    }
                }
                else if (targetType.IsEnum)
                {
                    if (rawValue is string enumStr)
                    {
                        prop.SetValue(instance, Enum.Parse(targetType, enumStr, ignoreCase: true));
                    }
                    else
                    {
                        prop.SetValue(instance, Enum.ToObject(targetType, rawValue));
                    }
                }
                else if (targetType == typeof(DateTimeOffset))
                {
                    if (rawValue is DateTime dt)
                    {
                        prop.SetValue(instance, new DateTimeOffset(dt));
                    }
                    else if (rawValue is string s && DateTimeOffset.TryParse(s, out var dto))
                    {
                        prop.SetValue(instance, dto);
                    }
                }
                else if (targetType == typeof(DateTime))
                {
                    if (rawValue is DateTimeOffset dto)
                    {
                        prop.SetValue(instance, dto.DateTime);
                    }
                    else if (rawValue is string s && DateTime.TryParse(s, out var dt))
                    {
                        prop.SetValue(instance, dt);
                    }
                }
                else if (targetType == typeof(DateOnly))
                {
                    if (rawValue is DateTime dt)
                    {
                        prop.SetValue(instance, DateOnly.FromDateTime(dt));
                    }
                    else if (rawValue is string s && DateOnly.TryParse(s, out var dOnly))
                    {
                        prop.SetValue(instance, dOnly);
                    }
                }
                else if (targetType == typeof(TimeOnly))
                {
                    if (rawValue is TimeSpan ts)
                    {
                        prop.SetValue(instance, TimeOnly.FromTimeSpan(ts));
                    }
                    else if (rawValue is string s && TimeOnly.TryParse(s, out var tOnly))
                    {
                        prop.SetValue(instance, tOnly);
                    }
                }
                else if (rawValue is IConvertible)
                {
                    prop.SetValue(instance, Convert.ChangeType(rawValue, targetType));
                }
                else
                {
                    prop.SetValue(instance, rawValue);
                }
            }
            catch
            {
                // Leave default if conversion fails
            }
        }

        return instance;
    }

    /// <summary>
    /// Marks this change as processed. Required for every change when the watcher runs in
    /// <see cref="CdcCheckpointMode.OnAcknowledgement"/> — the watermark only advances past a
    /// batch once all of its changes are acknowledged, so a skipped call stalls polling. A no-op
    /// in <see cref="CdcCheckpointMode.OnEmit"/>, and acknowledging twice counts once.
    /// </summary>
    public void Acknowledge() => Acknowledgement?.Acknowledge();
}
