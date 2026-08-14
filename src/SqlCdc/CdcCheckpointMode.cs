namespace SqlCdc;

/// <summary>
/// When the watermark LSN is persisted, which decides what is redelivered after a crash.
/// </summary>
public enum CdcCheckpointMode
{
    /// <summary>
    /// The watermark advances as soon as a batch has been written to the channel. Cheapest, but
    /// changes still sitting in the channel (or being handled) when the process dies are never
    /// redelivered: delivery is at-least-once up to the channel and at-most-once past it.
    /// </summary>
    OnEmit,

    /// <summary>
    /// The watermark advances only once every change in the batch has been acknowledged, which
    /// makes delivery at-least-once end to end. Polling pauses at the batch boundary until the
    /// consumer catches up, so throughput follows the slowest consumer.
    /// </summary>
    /// <remarks>
    /// Consumers reading <see cref="SqlCdcWatcher.Changes"/> directly must call
    /// <see cref="CdcChange.Acknowledge"/> for every change, otherwise polling stalls. The hosted
    /// service registered by <c>AddSqlCdc</c> acknowledges automatically after dispatch.
    /// </remarks>
    OnAcknowledgement,
}
