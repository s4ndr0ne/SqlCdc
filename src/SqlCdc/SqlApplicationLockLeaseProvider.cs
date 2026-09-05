using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SqlCdc;

/// <summary>
/// Elects a single active watcher with a SQL Server session-scoped application lock
/// (<c>sp_getapplock</c>) held on a dedicated connection.
/// </summary>
/// <remarks>
/// SQL Server drops the lock as soon as that connection goes away, so a crashed, killed or
/// network-partitioned instance loses the lease on its own. There is no expiry to tune and no
/// clock to keep in sync between instances, and two instances can never hold the lease at once:
/// the failover window is exactly how long SQL Server takes to notice the dead session.
/// </remarks>
public sealed class SqlApplicationLockLeaseProvider : ICdcLeaseProvider
{
    /// <summary>Lease name used when none is given. Instances sharing a name elect one leader.</summary>
    public const string DefaultLeaseName = "SqlCdc";

    /// <summary>Application lock resource names are limited to 255 characters by SQL Server.</summary>
    private const int MaxResourceLength = 255;

    private readonly ICdcConnectionFactory _connections;
    private readonly string _resource;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly int _commandTimeoutSeconds;
    private SqlConnection? _connection;
    private bool _disposed;

    /// <summary>
    /// Creates a lease provider for the database the connection string points at. The lease is
    /// scoped to that database, so watchers on different databases never contend.
    /// </summary>
    /// <param name="connectionString">Connection string to the CDC-enabled database.</param>
    /// <param name="leaseName">Name shared by the instances that elect a leader between them.</param>
    /// <param name="logger">Optional logger for lease transitions.</param>
    public SqlApplicationLockLeaseProvider(
        string connectionString,
        string leaseName = DefaultLeaseName,
        ILogger? logger = null)
        : this(new SqlCdcConnectionFactory(connectionString), leaseName, logger)
    {
    }

    /// <summary>
    /// Creates a lease provider that opens its connection through the given factory, so the lease
    /// authenticates the same way as the rest of the pipeline.
    /// </summary>
    /// <param name="connections">Factory for the connection the lease is held on.</param>
    /// <param name="leaseName">Name shared by the instances that elect a leader between them.</param>
    /// <param name="logger">Optional logger for lease transitions.</param>
    public SqlApplicationLockLeaseProvider(
        ICdcConnectionFactory connections,
        string leaseName = DefaultLeaseName,
        ILogger? logger = null)
        : this(connections, leaseName, logger, commandTimeout: null)
    {
    }

    /// <summary>
    /// Internal overload so the watcher can share its configured command timeout with the lease's
    /// own SQL round-trips. <paramref name="commandTimeout"/> is in seconds; <c>null</c> uses a
    /// default bounded timeout so a hung server cannot stall shutdown forever.
    /// </summary>
    internal SqlApplicationLockLeaseProvider(
        ICdcConnectionFactory connections,
        string leaseName,
        ILogger? logger,
        TimeSpan? commandTimeout)
    {
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseName);

        _resource = $"SqlCdc:{leaseName}";
        if (_resource.Length > MaxResourceLength)
        {
            throw new ArgumentException(
                $"The lease name is too long: the resource name '{_resource}' exceeds {MaxResourceLength} characters.",
                nameof(leaseName));
        }

        _commandTimeoutSeconds = commandTimeout is { } timeout
            ? (int)Math.Clamp(Math.Ceiling(timeout.TotalSeconds), 1, int.MaxValue)
            // No configured timeout: bound it so a DB that stops answering cannot hang a shutdown
            // through the lease-release round-trip, which StopAsync waits on without a token.
            : 30;

        // Pooling is turned off for this one connection: a pooled connection only releases its
        // session locks when the pool resets it on the next use, which would delay failover after
        // a graceful stop. Unpooled, closing the connection ends the session immediately. A custom
        // factory owns its own connection string, so there the explicit release has to carry it.
        _connections = connections is SqlCdcConnectionFactory sqlFactory
            ? sqlFactory.WithConnectionString(b => b.Pooling = false)
            : connections;
        _logger = logger ?? NullLogger<SqlApplicationLockLeaseProvider>.Instance;

    }

    /// <summary>The application lock resource this provider contends on.</summary>
    public string ResourceName => _resource;

    public async Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is not null)
            {
                return true;
            }

            var connection = await _connections.OpenConnectionAsync(cancellationToken);
            try
            {
                await using var cmd = new SqlCommand("sys.sp_getapplock", connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = _commandTimeoutSeconds,
                };
                cmd.Parameters.AddWithValue("@Resource", _resource);
                cmd.Parameters.AddWithValue("@LockMode", "Exclusive");
                cmd.Parameters.AddWithValue("@LockOwner", "Session");
                cmd.Parameters.AddWithValue("@LockTimeout", 0);
                var returnValue = cmd.Parameters.Add("@Result", SqlDbType.Int);
                returnValue.Direction = ParameterDirection.ReturnValue;

                await cmd.ExecuteNonQueryAsync(cancellationToken);

                // 0 granted, 1 granted after waiting. Anything negative means the lock was not
                // taken, which for @LockTimeout = 0 is simply "another instance holds it".
                var granted = returnValue.Value is int result && result >= 0;
                if (!granted)
                {
                    await connection.DisposeAsync();
                    return false;
                }

                _connection = connection;
                _logger.LogDebug("Acquired the application lock {Resource}", _resource);
                return true;
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> IsHeldAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is null)
            {
                return false;
            }

            if (_connection.State != ConnectionState.Open)
            {
                await DropConnectionAsync();
                return false;
            }

            try
            {
                // Doubles as a keepalive: a broken connection surfaces here rather than on the
                // next poll, and APPLOCK_MODE reports what this very session holds.
                await using var cmd = new SqlCommand(
                    "SELECT APPLOCK_MODE('public', @resource, 'Session');", _connection)
                {
                    CommandTimeout = _commandTimeoutSeconds,
                };
                cmd.Parameters.AddWithValue("@resource", _resource);

                var mode = await cmd.ExecuteScalarAsync(cancellationToken) as string;
                if (string.Equals(mode, "Exclusive", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                _logger.LogWarning(
                    "The application lock {Resource} is no longer held by this session (mode {Mode})",
                    _resource, mode ?? "NoLock");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // The lease lives on this connection: if it is unusable the lease is gone, whatever
                // the reason. Reporting it as lost is what lets another instance take over.
                _logger.LogWarning(ex, "The connection holding the application lock {Resource} failed", _resource);
            }

            await DropConnectionAsync();
            return false;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            await ReleaseCoreAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _gate.WaitAsync();
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            await ReleaseCoreAsync(CancellationToken.None);
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async Task ReleaseCoreAsync(CancellationToken cancellationToken)
    {
        if (_connection is null)
        {
            return;
        }

        try
        {
            if (_connection.State == ConnectionState.Open)
            {
                await using var cmd = new SqlCommand("sys.sp_releaseapplock", _connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = _commandTimeoutSeconds,
                };
                cmd.Parameters.AddWithValue("@Resource", _resource);
                cmd.Parameters.AddWithValue("@LockOwner", "Session");
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            // Closing an unpooled connection releases the lock anyway. A pooled one — a custom
            // factory may hand those out — would go back to the pool still holding the session
            // lock; clearing its pool makes the dispose below end the session for real, so the
            // lock dies with it instead of lingering until the pool reuses the connection.
            _logger.LogDebug(ex, "Releasing the application lock {Resource} failed", _resource);
            try
            {
                SqlConnection.ClearPool(_connection);
            }
            catch
            {
                // Best effort: the connection may be in a state ClearPool rejects.
            }
        }
        finally
        {
            await DropConnectionAsync();
        }
    }

    private async Task DropConnectionAsync()
    {
        var connection = _connection;
        _connection = null;
        if (connection is not null)
        {
            // A custom factory can hand out a pooled connection. Clear its pool before returning
            // it, so a session-scoped application lock can never survive disposal in an idle pool.
            try
            {
                SqlConnection.ClearPool(connection);
            }
            catch
            {
                // Best effort for broken connections; DisposeAsync below still releases normal ones.
            }
            await connection.DisposeAsync();
        }
    }
}
