namespace Atya.Messaging.Outbox;

/// <summary>
/// Describes the outcome of one outbox relay pass.
/// </summary>
public readonly struct OutboxRelayResult
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxRelayResult"/> struct.
    /// </summary>
    /// <param name="attemptedCount">The number of pending messages attempted.</param>
    /// <param name="publishedCount">The number of messages published successfully.</param>
    /// <param name="failedCount">The number of messages that failed to publish.</param>
    public OutboxRelayResult(
        int attemptedCount,
        int publishedCount,
        int failedCount)
    {
        AttemptedCount = attemptedCount;
        PublishedCount = publishedCount;
        FailedCount = failedCount;
    }

    /// <summary>
    /// Gets the number of pending messages attempted.
    /// </summary>
    public int AttemptedCount { get; }

    /// <summary>
    /// Gets the number of messages published successfully.
    /// </summary>
    public int PublishedCount { get; }

    /// <summary>
    /// Gets the number of messages that failed to publish.
    /// </summary>
    public int FailedCount { get; }
}
