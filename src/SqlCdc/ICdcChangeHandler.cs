namespace SqlCdc;

/// <summary>
/// Handles a single change event. Register with
/// <see cref="Microsoft.Extensions.DependencyInjection.SqlCdcServiceCollectionExtensions.AddCdcChangeHandler{THandler}(Microsoft.Extensions.DependencyInjection.IServiceCollection)"/>
/// to have the hosted service dispatch events to it.
/// </summary>
/// <remarks>
/// Handlers are resolved from a dedicated scope per change, so scoped dependencies
/// (a <c>DbContext</c>, for instance) can be injected safely.
/// </remarks>
public interface ICdcChangeHandler
{
    /// <summary>Processes one change event.</summary>
    Task HandleAsync(CdcChange change, CancellationToken cancellationToken = default);
}

/// <summary>
/// A change handler targeted at a specific table. Handlers implementing this interface
/// will only be invoked for changes originating from the designated table.
/// </summary>
public interface ICdcTableChangeHandler : ICdcChangeHandler
{
    /// <summary>Schema of the source table, or <c>null</c> to match any schema.</summary>
    string? SourceSchema => null;

    /// <summary>Name of the source table.</summary>
    string SourceTable { get; }
}

/// <summary>
/// Attributes a change handler to only receive events from a specific table.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
public sealed class CdcTableAttribute : Attribute
{
    /// <summary>Schema of the source table, or <c>null</c> if any schema is matched.</summary>
    public string? Schema { get; }

    /// <summary>Name of the source table.</summary>
    public string Table { get; }

    /// <summary>Matches changes to the table in any schema.</summary>
    public CdcTableAttribute(string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        Table = table;
    }

    /// <summary>Matches changes to the table in the specified schema.</summary>
    public CdcTableAttribute(string schema, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        Schema = schema;
        Table = table;
    }
}

/// <summary>
/// Handles a batch of change events together. Register with
/// <see cref="Microsoft.Extensions.DependencyInjection.SqlCdcServiceCollectionExtensions.AddCdcBatchChangeHandler{THandler}"/>.
/// </summary>
public interface ICdcBatchChangeHandler
{
    /// <summary>Processes a batch of change events.</summary>
    Task HandleBatchAsync(IReadOnlyList<CdcChange> changes, CancellationToken cancellationToken = default);
}
