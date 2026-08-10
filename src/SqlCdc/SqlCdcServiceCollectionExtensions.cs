using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlCdc;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers a <see cref="SqlCdcWatcher"/> and the hosted service that runs it.
/// </summary>
public static class SqlCdcServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="SqlCdcWatcher"/> plus an <see cref="IHostedService"/> that
    /// starts it with the host and stops it on shutdown.
    /// </summary>
    /// <remarks>
    /// The logger and, when registered, the <see cref="ICdcStateStore"/> are taken from the service
    /// provider; anything set inside <paramref name="configure"/> overrides them. Register handlers
    /// with <see cref="AddCdcChangeHandler{THandler}"/> to have events dispatched automatically,
    /// otherwise inject <see cref="SqlCdcWatcher"/> and read its <see cref="SqlCdcWatcher.Changes"/>.
    /// </remarks>
    public static IServiceCollection AddSqlCdc(this IServiceCollection services, Action<SqlCdcWatcherBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddSingleton(sp =>
        {
            var builder = SqlCdcWatcherBuilder.Create();

            var loggerFactory = sp.GetService<ILoggerFactory>();
            if (loggerFactory is not null)
            {
                builder.UseLogger(loggerFactory.CreateLogger<SqlCdcWatcher>());
            }

            var stateStore = sp.GetService<ICdcStateStore>();
            if (stateStore is not null)
            {
                builder.UseStateStore(stateStore);
            }

            // Applied last so explicit configuration wins over what was resolved from the container.
            configure(builder);
            return builder.Build();
        });

        services.AddHostedService<SqlCdcHostedService>();
        return services;
    }

    /// <summary>
    /// Registers a scoped <see cref="ICdcChangeHandler"/>. Every registered handler receives
    /// every change, in registration order.
    /// </summary>
    public static IServiceCollection AddCdcChangeHandler<THandler>(this IServiceCollection services)
        where THandler : class, ICdcChangeHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddScoped<ICdcChangeHandler, THandler>();
        return services;
    }
}
