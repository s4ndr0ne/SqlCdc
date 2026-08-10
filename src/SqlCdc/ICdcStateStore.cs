namespace SqlCdc;

/// <summary>
/// Persists the last processed LSN per capture instance so processing can
/// resume from where it stopped, rather than restarting from scratch.
/// </summary>
public interface ICdcStateStore
{
    /// <summary>Returns the last processed LSN for a capture instance, or <c>null</c> if none.</summary>
    Task<byte[]?> GetLastLsnAsync(string captureInstance, CancellationToken cancellationToken = default);

    /// <summary>Persists the last processed LSN for a capture instance.</summary>
    Task SaveLastLsnAsync(string captureInstance, byte[] lsn, CancellationToken cancellationToken = default);
}
