using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace SqlCdc;

/// <summary>
/// Dead-letter sink that stores failed changes in a SQL table, with the before/after images kept
/// as JSON so a change can be inspected and replayed. The table is created automatically.
/// </summary>
public sealed class SqlCdcDeadLetterSink : ICdcDeadLetterSink
{
    private static readonly JsonSerializerOptions PayloadOptions = new()
    {
        WriteIndented = false,
    };

    private readonly string _connectionString;
    private readonly string _schema;
    private readonly string _table;
    private readonly SemaphoreSlim _ensureLock = new(1, 1);
    private bool _tableEnsured;

    public SqlCdcDeadLetterSink(string connectionString, string schema = "dbo", string table = "cdc_dead_letter")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        _connectionString = connectionString;
        _schema = schema;
        _table = table;
    }

    private string TableName =>
        $"{SqlIdentifier.Quote(_schema, nameof(_schema))}.{SqlIdentifier.Quote(_table, nameof(_table))}";

    public async Task WriteAsync(CdcDeadLetter deadLetter, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(deadLetter);

        for (var attempt = 0; ; attempt++)
        {
            await EnsureTableAsync(cancellationToken);
            try
            {
                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(cancellationToken);

                var sql =
                    $"""
                     INSERT INTO {TableName}
                         (CaptureInstance, SourceSchema, SourceTable, Operation, ChangeKey,
                          CommitTime, Payload, HandlerName, Attempts, Error, FailedAt)
                     VALUES
                         (@captureInstance, @sourceSchema, @sourceTable, @operation, @changeKey,
                          @commitTime, @payload, @handlerName, @attempts, @error, @failedAt);
                     """;

                var change = deadLetter.Change;
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@captureInstance", change.CaptureInstance);
                cmd.Parameters.AddWithValue("@sourceSchema", change.SourceSchema);
                cmd.Parameters.AddWithValue("@sourceTable", change.SourceTable);
                cmd.Parameters.AddWithValue("@operation", change.Operation.ToString());
                cmd.Parameters.AddWithValue("@changeKey", change.Key);
                cmd.Parameters.Add("@commitTime", SqlDbType.DateTime2).Value =
                    change.CommitTime == default ? DBNull.Value : change.CommitTime;
                cmd.Parameters.Add("@payload", SqlDbType.NVarChar, -1).Value = SerializePayload(change);
                cmd.Parameters.AddWithValue("@handlerName", deadLetter.HandlerName);
                cmd.Parameters.AddWithValue("@attempts", deadLetter.Attempts);
                cmd.Parameters.Add("@error", SqlDbType.NVarChar, -1).Value = deadLetter.Error.ToString();
                cmd.Parameters.Add("@failedAt", SqlDbType.DateTime2).Value = deadLetter.FailedAt.UtcDateTime;

                await cmd.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (SqlException ex) when (ex.Number == 208 && attempt == 0)
            {
                // The table was dropped since it was last ensured; rebuild it and write again.
                Volatile.Write(ref _tableEnsured, false);
            }
        }
    }

    /// <summary>
    /// Renders the change as JSON. CDC values come off the reader as whatever CLR type the column
    /// maps to, and an exotic one must not cost the dead letter: the payload degrades to strings
    /// rather than throwing away the record.
    /// </summary>
    private static string SerializePayload(CdcChange change)
    {
        try
        {
            return JsonSerializer.Serialize(
                new
                {
                    change.Before,
                    change.After,
                    change.UpdateMask,
                },
                PayloadOptions);
        }
        catch (NotSupportedException)
        {
            return JsonSerializer.Serialize(
                new
                {
                    Before = Stringify(change.Before),
                    After = Stringify(change.After),
                    change.UpdateMask,
                },
                PayloadOptions);
        }
    }

    private static Dictionary<string, string?> Stringify(IReadOnlyDictionary<string, object?> values) =>
        values.ToDictionary(pair => pair.Key, pair => pair.Value?.ToString());

    private async Task EnsureTableAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _tableEnsured))
        {
            return;
        }

        await _ensureLock.WaitAsync(cancellationToken);
        try
        {
            if (_tableEnsured)
            {
                return;
            }

            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);
            var sql =
                $"""
                 IF OBJECT_ID(@tableName, N'U') IS NULL
                     CREATE TABLE {TableName}
                     (
                         Id bigint IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                         CaptureInstance nvarchar(128) NOT NULL,
                         SourceSchema nvarchar(128) NOT NULL,
                         SourceTable nvarchar(128) NOT NULL,
                         Operation nvarchar(20) NOT NULL,
                         ChangeKey nvarchar(64) NOT NULL,
                         CommitTime datetime2 NULL,
                         Payload nvarchar(max) NULL,
                         HandlerName nvarchar(256) NOT NULL,
                         Attempts int NOT NULL,
                         Error nvarchar(max) NULL,
                         FailedAt datetime2 NOT NULL
                     );
                 """;

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@tableName", TableName);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
            _tableEnsured = true;
        }
        finally
        {
            _ensureLock.Release();
        }
    }
}
