namespace SqlCdc;

/// <summary>
/// Handles a single change event. Register with
/// <see cref="Microsoft.Extensions.DependencyInjection.SqlCdcServiceCollectionExtensions.AddCdcChangeHandler{THandler}"/>
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
