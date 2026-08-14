using Microsoft.Data.SqlClient;

namespace SqlCdc;

/// <summary>
/// Opens the connections used to read CDC and to persist watermarks. Everything that talks to SQL
/// Server goes through this, which is the seam for Entra ID tokens, a custom retry provider, or
/// any other per-connection configuration a deployment needs.
/// </summary>
public interface ICdcConnectionFactory
{
    /// <summary>
    /// Returns a connection that is already open. The caller disposes it, so an implementation
    /// must hand out a new connection each time rather than share one.
    /// </summary>
    Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// The default factory: opens a connection from a connection string, optionally acquiring an
/// access token per connection.
/// </summary>
/// <remarks>
/// A connection string alone already covers Entra ID — <c>Authentication=Active Directory
/// Managed Identity</c> and friends are handled by Microsoft.Data.SqlClient. The token callback
/// is for when the application wants to own token acquisition, for example to reuse a configured
/// <c>TokenCredential</c> with its own caching.
/// </remarks>
public sealed class SqlCdcConnectionFactory : ICdcConnectionFactory
{
    private readonly string _connectionString;
    private readonly Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>>? _accessTokenCallback;

    public SqlCdcConnectionFactory(
        string connectionString,
        Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>>? accessTokenCallback = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        _connectionString = connectionString;
        _accessTokenCallback = accessTokenCallback;
    }

    /// <summary>The connection string this factory opens connections from.</summary>
    public string ConnectionString => _connectionString;

    public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        if (_accessTokenCallback is not null)
        {
            connection.AccessTokenCallback = _accessTokenCallback;
        }

        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Copies this factory onto a modified connection string, keeping the token callback. Used by
    /// the lease provider, which needs its own unpooled connection.
    /// </summary>
    internal SqlCdcConnectionFactory WithConnectionString(Action<SqlConnectionStringBuilder> configure)
    {
        var builder = new SqlConnectionStringBuilder(_connectionString);
        configure(builder);
        return new SqlCdcConnectionFactory(builder.ConnectionString, _accessTokenCallback);
    }
}

/// <summary>Adapts a delegate to <see cref="ICdcConnectionFactory"/>.</summary>
internal sealed class DelegateCdcConnectionFactory : ICdcConnectionFactory
{
    private readonly Func<CancellationToken, Task<SqlConnection>> _open;

    public DelegateCdcConnectionFactory(Func<CancellationToken, Task<SqlConnection>> open) => _open = open;

    public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await _open(cancellationToken)
            ?? throw new InvalidOperationException("The connection factory returned null.");

        // The contract asks for an open connection, but handing back a configured-but-closed one
        // is an easy mistake and cheap to absorb.
        if (connection.State != System.Data.ConnectionState.Open)
        {
            try
            {
                await connection.OpenAsync(cancellationToken);
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }

        return connection;
    }
}
