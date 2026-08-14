using Microsoft.Extensions.Configuration;
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
        ArgumentNullException.ThrowIfNull(configure);

        return AddSqlCdcCore(services, configuration: null, configure);
    }

    /// <summary>
    /// Registers the watcher from a configuration section, so connection strings and tuning live
    /// in appsettings, environment variables or a secret store rather than in code.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">
    /// The <c>SqlCdc</c> section, for example <c>builder.Configuration.GetSection("SqlCdc")</c>.
    /// See <see cref="SqlCdcConfiguration"/> for the shape it is bound to.
    /// </param>
    /// <param name="configure">
    /// Optional extra configuration, applied after the section — use it for anything that cannot
    /// be expressed in configuration, such as a state store instance. Settings made here win.
    /// </param>
    /// <remarks>
    /// The watcher is built when the host starts, so a section that is missing a connection
    /// string or a table fails startup rather than failing quietly at the first poll.
    /// </remarks>
    public static IServiceCollection AddSqlCdc(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<SqlCdcWatcherBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (configuration is IConfigurationSection section && !section.Exists())
        {
            throw new InvalidOperationException(
                $"The configuration section '{section.Path}' does not exist. Add it, or configure SqlCdc in code.");
        }

        return AddSqlCdcCore(services, configuration, configure);
    }

    private static IServiceCollection AddSqlCdcCore(
        IServiceCollection services,
        IConfiguration? configuration,
        Action<SqlCdcWatcherBuilder>? configure)
    {
        ArgumentNullException.ThrowIfNull(services);

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

            var leaseProvider = sp.GetService<ICdcLeaseProvider>();
            if (leaseProvider is not null)
            {
                builder.UseLeaseProvider(leaseProvider);
            }

            var connectionFactory = sp.GetService<ICdcConnectionFactory>();
            if (connectionFactory is not null)
            {
                builder.UseConnectionFactory(connectionFactory);
            }

            // Configuration first, then the delegate: code wins over the section, and both win
            // over what was resolved from the container.
            configuration?.Get<SqlCdcConfiguration>()?.ApplyTo(builder);
            configure?.Invoke(builder);
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

    /// <summary>
    /// Registers where changes go when a handler has used up its attempts. Without a sink they are
    /// logged and dropped. Pair it with <see cref="SqlCdcWatcherBuilder.WithHandlerRetry"/>.
    /// </summary>
    public static IServiceCollection AddCdcDeadLetterSink<TSink>(this IServiceCollection services)
        where TSink : class, ICdcDeadLetterSink
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<ICdcDeadLetterSink, TSink>();
        return services;
    }

    /// <inheritdoc cref="AddCdcDeadLetterSink{TSink}(IServiceCollection)"/>
    public static IServiceCollection AddCdcDeadLetterSink(this IServiceCollection services, ICdcDeadLetterSink sink)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(sink);

        services.TryAddSingleton(sink);
        return services;
    }
}
