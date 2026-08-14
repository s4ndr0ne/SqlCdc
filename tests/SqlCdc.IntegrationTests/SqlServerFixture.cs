using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace SqlCdc.IntegrationTests;

[CollectionDefinition(SqlServerCollection.Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "SqlServer";
}

/// <summary>
/// Starts one SQL Server container for the whole assembly, with SQL Server Agent enabled
/// (the CDC capture job needs it) and CDC turned on for a dedicated test database.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private const string DatabaseName = "CdcTests";

    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .WithEnvironment("MSSQL_PID", "Developer")
        .WithEnvironment("MSSQL_AGENT_ENABLED", "true")
        .Build();

    private bool _captureJobTuned;

    /// <summary>Connection string to the CDC-enabled test database.</summary>
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        await ExecuteAsync(_container.GetConnectionString(), $"CREATE DATABASE [{DatabaseName}];");
        ConnectionString = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            InitialCatalog = DatabaseName,
        }.ConnectionString;

        await ExecuteAsync("EXEC sys.sp_cdc_enable_db;");
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>
    /// Creates a table, enables CDC on it and waits until its capture instance is queryable.
    /// Every test uses its own table so they do not observe each other's changes.
    /// </summary>
    public async Task<string> CreateCdcTableAsync(string tableName)
    {
        await ExecuteAsync(
            $"""
             CREATE TABLE dbo.[{tableName}]
             (
                 Id int NOT NULL PRIMARY KEY,
                 Name nvarchar(100) NULL,
                 Price decimal(10, 2) NULL
             );
             """);

        await ExecuteWithRetryAsync(
            $"""
             EXEC sys.sp_cdc_enable_table
                  @source_schema = N'dbo',
                  @source_name   = N'{tableName}',
                  @role_name     = NULL;
             """,
            IsSqlServerAgentStartingError);

        await TuneCaptureJobAsync();

        var captureInstance = $"dbo_{tableName}";
        await WaitForCaptureInstanceAsync(captureInstance);
        return captureInstance;
    }

    /// <summary>
    /// Adds a second capture instance to an already captured table, which is how a schema change
    /// is rolled out without stopping capture. SQL Server allows at most two per table.
    /// </summary>
    public async Task<string> AddCaptureInstanceAsync(string tableName, string captureInstance)
    {
        await ExecuteWithRetryAsync(
            $"""
             EXEC sys.sp_cdc_enable_table
                  @source_schema    = N'dbo',
                  @source_name      = N'{tableName}',
                  @capture_instance = N'{captureInstance}',
                  @role_name        = NULL;
             """,
            IsSqlServerAgentStartingError);

        await WaitForCaptureInstanceAsync(captureInstance);
        return captureInstance;
    }

    /// <summary>Drops a capture instance, which is how a watched table "breaks" at runtime.</summary>
    public Task DisableCdcAsync(string tableName) => ExecuteAsync(
        $"""
         EXEC sys.sp_cdc_disable_table
              @source_schema    = N'dbo',
              @source_name      = N'{tableName}',
              @capture_instance = N'dbo_{tableName}';
         """);

    /// <summary>
    /// Runs the CDC cleanup up to the current end of the log, which is what the cleanup job does
    /// on its retention schedule. Afterwards fn_cdc_get_min_lsn is past any older watermark.
    /// </summary>
    public Task CleanupChangeTableAsync(string captureInstance) => ExecuteAsync(
        $"""
         DECLARE @lwm binary(10) = sys.fn_cdc_get_max_lsn();
         EXEC sys.sp_cdc_cleanup_change_table
              @capture_instance = N'{captureInstance}',
              @low_water_mark   = @lwm;
         """);

    public Task ExecuteAsync(string sql) => ExecuteAsync(ConnectionString, sql);

    public async Task<byte[]?> GetMinLsnAsync(string captureInstance)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT sys.fn_cdc_get_min_lsn(@ci);", conn);
        cmd.Parameters.AddWithValue("@ci", captureInstance);
        return await cmd.ExecuteScalarAsync() as byte[];
    }

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task ExecuteWithRetryAsync(
        string sql,
        Func<SqlException, bool> isTransient,
        int maxAttempts = 30)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await ExecuteAsync(sql);
                return;
            }
            catch (SqlException ex) when (attempt < maxAttempts && isTransient(ex))
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }

    private static bool IsSqlServerAgentStartingError(SqlException exception)
    {
        if (exception.Errors.Cast<SqlError>().Any(error => error.Number is 14258 or 22836))
        {
            return true;
        }

        var message = exception.ToString();
        return message.Contains("SQLServerAgent is starting", StringComparison.OrdinalIgnoreCase)
            || message.Contains("14258", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Drops the capture job polling interval from its 5 second default to 1 second, so tests wait
    /// seconds rather than tens of seconds. The job only exists once a table has been enabled.
    /// </summary>
    private async Task TuneCaptureJobAsync()
    {
        if (_captureJobTuned)
        {
            return;
        }

        // Set before doing the work: a failure here must not make every later test retry it.
        _captureJobTuned = true;

        await ExecuteAsync("EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 1;");

        // The new interval only takes effect once the job restarts. SQL Server Agent rejects a stop
        // or start that races with the job's own state, and those errors are raised outside T-SQL
        // TRY/CATCH, so both are handled here instead.
        await TryExecuteAsync("EXEC sys.sp_cdc_stop_job @job_type = N'capture';");
        await Task.Delay(500);

        for (var attempt = 1; attempt <= 5; attempt++)
        {
            var error = await TryExecuteAsync("EXEC sys.sp_cdc_start_job @job_type = N'capture';");
            if (error is null || error.Message.Contains("already running", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (attempt == 5)
            {
                throw new InvalidOperationException(
                    "The CDC capture job could not be restarted; no changes would be captured.", error);
            }

            await Task.Delay(1000);
        }
    }

    private static async Task<SqlException?> TryExecuteAsync(string connectionString, string sql)
    {
        try
        {
            await ExecuteAsync(connectionString, sql);
            return null;
        }
        catch (SqlException ex)
        {
            return ex;
        }
    }

    private Task<SqlException?> TryExecuteAsync(string sql) => TryExecuteAsync(ConnectionString, sql);

    /// <summary>
    /// Waits until the capture job has processed the enable, which is when the capture instance
    /// gets a start LSN. Until then a "from beginning" watcher has nothing to anchor on.
    /// </summary>
    private async Task WaitForCaptureInstanceAsync(string captureInstance)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        while (DateTime.UtcNow < deadline)
        {
            if (await GetMinLsnAsync(captureInstance) is not null)
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException(
            $"The capture instance '{captureInstance}' was not ready in time. Is SQL Server Agent running?");
    }
}
