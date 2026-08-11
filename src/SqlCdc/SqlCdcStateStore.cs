using Microsoft.Data.SqlClient;

namespace SqlCdc;

/// <summary>
/// State store that persists the watermark LSN in a SQL table, so a restarted
/// watcher resumes exactly where it left off. The table is created automatically.
/// </summary>
public sealed class SqlCdcStateStore : ICdcStateStore
{
    private readonly string _connectionString;
    private readonly string _schema;
    private readonly string _table;
    private readonly SemaphoreSlim _ensureLock = new(1, 1);
    private bool _tableEnsured;

    public SqlCdcStateStore(string connectionString, string schema = "dbo", string table = "cdc_watermark")
    {
        _connectionString = connectionString;
        _schema = schema;
        _table = table;
    }

    private string TableName =>
        $"{SqlIdentifier.Quote(_schema, nameof(_schema))}.{SqlIdentifier.Quote(_table, nameof(_table))}";

    public async Task<byte[]?> GetLastLsnAsync(string captureInstance, CancellationToken cancellationToken = default)
    {
        for (var attempt = 0; ; attempt++)
        {
            await EnsureTableAsync(cancellationToken);
            try
            {
                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(cancellationToken);
                string sql =
                    $"SELECT LastLsn FROM {TableName} WHERE CaptureInstance = @ci;";
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@ci", captureInstance);
                var result = await cmd.ExecuteScalarAsync(cancellationToken);
                return result is byte[] lsn ? lsn : null;
            }
            catch (SqlException ex) when (ex.Number == 208 && attempt == 0)
            {
                InvalidateTableCache();
            }
        }
    }

    public async Task SaveLastLsnAsync(string captureInstance, byte[] lsn, CancellationToken cancellationToken = default)
    {
        for (var attempt = 0; ; attempt++)
        {
            await EnsureTableAsync(cancellationToken);
            try
            {
                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(cancellationToken);
                string sql =
                    $"""
                     IF EXISTS (SELECT 1 FROM {TableName} WHERE CaptureInstance = @ci)
                         UPDATE {TableName} SET LastLsn = @lsn, UpdatedAt = SYSUTCDATETIME() WHERE CaptureInstance = @ci;
                     ELSE
                         INSERT INTO {TableName} (CaptureInstance, LastLsn, UpdatedAt) VALUES (@ci, @lsn, SYSUTCDATETIME());
                     """;
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@ci", captureInstance);
                cmd.Parameters.Add("@lsn", System.Data.SqlDbType.Binary, 10).Value = lsn;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (SqlException ex) when (ex.Number == 208 && attempt == 0)
            {
                InvalidateTableCache();
            }
        }
    }

    private void InvalidateTableCache() => Volatile.Write(ref _tableEnsured, false);

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
            string sql =
                $"""
                 IF OBJECT_ID(@tableName, N'U') IS NULL
                     CREATE TABLE {TableName}
                     (
                         CaptureInstance nvarchar(128) NOT NULL PRIMARY KEY,
                         LastLsn binary(10) NOT NULL,
                         UpdatedAt datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
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
