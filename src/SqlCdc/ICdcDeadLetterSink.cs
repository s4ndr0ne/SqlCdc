namespace SqlCdc;

/// <summary>
/// A change that a handler could not process after all of its attempts.
/// </summary>
/// <param name="Change">The change that failed.</param>
/// <param name="HandlerName">Type name of the handler that gave up on it.</param>
/// <param name="Attempts">How many times the handler was called before it was dead-lettered.</param>
/// <param name="Error">The exception thrown by the last attempt.</param>
/// <param name="FailedAt">When the last attempt failed.</param>
public sealed record CdcDeadLetter(
    CdcChange Change,
    string HandlerName,
    int Attempts,
    Exception Error,
    DateTimeOffset FailedAt);

/// <summary>
/// Receives changes that exhausted their handler attempts, so they can be inspected and replayed
/// instead of being lost to a log line.
/// </summary>
/// <remarks>
/// The sink is on the delivery path: it is awaited before the next change is dispatched. A sink
/// that throws is retried with backoff until it accepts the write, so an unavailable sink pauses
/// delivery rather than losing the dead letter. In <see cref="CdcCheckpointMode.OnAcknowledgement"/>
/// the change also stays unacknowledged, so a restart during the outage replays it. Keep the
/// write cheap.
/// </remarks>
public interface ICdcDeadLetterSink
{
    /// <summary>Records a change that could not be handled.</summary>
    Task WriteAsync(CdcDeadLetter deadLetter, CancellationToken cancellationToken = default);
}
