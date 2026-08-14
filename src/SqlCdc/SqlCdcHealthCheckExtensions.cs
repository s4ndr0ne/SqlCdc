using Microsoft.Extensions.Diagnostics.HealthChecks;
using SqlCdc;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the CDC health check.
/// </summary>
public static class SqlCdcHealthCheckExtensions
{
    /// <summary>Name the check is registered under when none is given.</summary>
    public const string DefaultName = "sqlcdc";

    /// <summary>
    /// Adds a health check reporting whether the <see cref="SqlCdcWatcher"/> registered by
    /// <c>AddSqlCdc</c> is actually delivering changes.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">Name of the check. Defaults to <c>sqlcdc</c>.</param>
    /// <param name="failureStatus">Status reported on failure. Defaults to Unhealthy.</param>
    /// <param name="tags">Tags used to filter the check, for example <c>ready</c>.</param>
    /// <param name="configureOptions">Adjusts the thresholds.</param>
    public static IHealthChecksBuilder AddSqlCdc(
        this IHealthChecksBuilder builder,
        string name = DefaultName,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        Action<SqlCdcHealthCheckOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new SqlCdcHealthCheckOptions();
        configureOptions?.Invoke(options);

        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new SqlCdcHealthCheck(sp.GetRequiredService<SqlCdcWatcher>(), options),
            failureStatus,
            tags));
    }
}
