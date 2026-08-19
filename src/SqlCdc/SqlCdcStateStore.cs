using Microsoft.Data.SqlClient;

namespace SqlCdc;

/// <summary>
/// State store that persists the watermark LSN in a SQL table, so a restarted
/// watcher resumes exactly where it left off. The table is created automatically.
/// </summary>
public sealed class SqlCdcStateStore : ICdcStateStore
{
    private readonly ICdcConnectionFactory _connections;
    private readonly string _schema;
    private readonly string _table;
    private readonly bool _createTableIfMissing;
    private readonly int _commandTimeoutSeconds;
    private readonly SemaphoreSlim _ensureLock = new(1, 1);
    private bool _tableEnsured;

    /// <inheritdoc cref="SqlCdcStateStore(ICdcConnectionFactory, string, string, bool, TimeSpan?)"/>
    public SqlCdcStateStore(
        string connectionString,
        string schema = "dbo",
        string table = "cdc_watermark",
        bool createTableIfMissing = true,
        TimeSpan? commandTimeout = null)
        : this(new SqlCdcConnectionFactory(connectionString), schema, table, createTableIfMissing, commandTimeout)
    {
    }

    /// <summary>
    /// Creates a state store over the given table.
    /// </summary>
    /// <param name="connections">
    /// Opens the connections, so watermarks are reached the same way as the CDC tables — same
    /// credentials, same token, same retry configuration.
    /// </param>
    /// <param name="schema">Schema of the watermark table.</param>
    /// <param name="table">Name of the watermark table.</param>
    /// <param name="createTableIfMissing">
    /// Creates the table on first use. Set to <c>false</c> where the application has no DDL
    /// rights at runtime and the table is provisioned by a migration —
    /// see <c>scripts/create-state-tables.sql</c>.
    /// </param>
    /// <param name="commandTimeout">Timeout per statement. Defaults to 30 seconds.</param>
    public SqlCdcStateStore(
        ICdcConnectionFactory connections,
        string schema = "dbo",
        string table = "cdc_watermark",
        bool createTableIfMissing = true,
        TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(connections);

        if (commandTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(commandTimeout), "Command timeout must be positive.");
        }

        _connections = connections;
        _schema = schema;
        _table = table;
        _createTableIfMissing = createTableIfMissing;
        _commandTimeoutSeconds = (int)Math.Clamp(
            Math.Ceiling((commandTimeout ?? TimeSpan.FromSeconds(30)).TotalSeconds), 1, int.MaxValue);
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
                await using var conn = await _connections.OpenConnectionAsync(cancellationToken);
                string sql =
                    $"SELECT LastLsn FROM {TableName} WHERE CaptureInstance = @ci;";
                await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = _commandTimeoutSeconds };
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

    /// <summary>
    /// Records the watermark, atomically and only ever forwards.
    /// </summary>
    /// <remarks>
    /// The update takes a key-range lock so two writers cannot both decide the row is missing and
    /// both insert it. The <c>LastLsn &lt; @lsn</c> guard makes a lower LSN a no-op rather than a
    /// rewind: a watcher that lost its lease but has a save already in flight cannot drag the new
    /// leader backwards and force it to replay.
    /// </remarks>
    public async Task SaveLastLsnAsync(string captureInstance, byte[] lsn, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connections.OpenConnectionAsync(cancellationToken);
        await SaveLastLsnAsync(connection, captureInstance, lsn, cancellationToken);
    }

    /// <summary>
    /// Persists a watermark using an already-open connection owned by the CDC polling operation.
    /// This avoids opening a second connection for every delivered batch.
    /// </summary>
    internal async Task SaveLastLsnAsync(
        SqlConnection connection,
        string captureInstance,
        byte[] lsn,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        for (var attempt = 0; ; attempt++)
        {
            await EnsureTableAsync(connection, cancellationToken);
            try
            {
                string sql =
                    $"""
                     SET XACT_ABORT ON;
                     BEGIN TRANSACTION;

                     UPDATE {TableName} WITH (UPDLOCK, SERIALIZABLE)
                        SET LastLsn = @lsn, UpdatedAt = SYSUTCDATETIME()
                      WHERE CaptureInstance = @ci AND LastLsn < @lsn;

                     IF @@ROWCOUNT = 0 AND NOT EXISTS (
                             SELECT 1 FROM {TableName} WITH (UPDLOCK, SERIALIZABLE)
                             WHERE CaptureInstance = @ci)
                     BEGIN
                         INSERT INTO {TableName} (CaptureInstance, LastLsn, UpdatedAt)
                         VALUES (@ci, @lsn, SYSUTCDATETIME());
                     END

                     COMMIT TRANSACTION;
                     """;
                 await using var cmd = new SqlCommand(sql, connection) { CommandTimeout = _commandTimeoutSeconds };
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
        await using var connection = await _connections.OpenConnectionAsync(cancellationToken);
        await EnsureTableAsync(connection, cancellationToken);
    }

    private async Task EnsureTableAsync(SqlConnection connection, CancellationToken cancellationToken)
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

            if (!_createTableIfMissing)
            {
                await using var check = new SqlCommand(
                    "SELECT CASE WHEN OBJECT_ID(@tableName, N'U') IS NULL THEN 0 ELSE 1 END;", connection)
                {
                    CommandTimeout = _commandTimeoutSeconds,
                };
                check.Parameters.AddWithValue("@tableName", TableName);

                if ((int)(await check.ExecuteScalarAsync(cancellationToken))! == 0)
                {
                    throw new InvalidOperationException(
                        $"The watermark table {TableName} does not exist and this state store is configured not " +
                        "to create it. Provision it with scripts/create-state-tables.sql, or allow creation.");
                }

                _tableEnsured = true;
                return;
            }

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
            await using var cmd = new SqlCommand(sql, connection) { CommandTimeout = _commandTimeoutSeconds };
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
