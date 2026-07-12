namespace Atya.Messaging.Outbox;

/// <summary>
/// Stores outbox messages until a relay publishes them.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Adds a pending outbox message. Durable store implementations must participate in the caller's transaction so
    /// message persistence commits atomically with the state change that produced the message.
    /// </summary>
    /// <param name="message">The message to store.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that completes when the message has been stored.</returns>
    public ValueTask AddAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads pending outbox messages in relay order.
    /// </summary>
    /// <param name="maxCount">The maximum number of messages to load.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The pending messages.</returns>
    public ValueTask<IReadOnlyList<OutboxMessage>> LoadPendingAsync(
        int maxCount,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a stored message as published.
    /// </summary>
    /// <param name="id">The outbox message identifier.</param>
    /// <param name="publishedAt">The UTC time when the message was published.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that completes when the message has been updated.</returns>
    public ValueTask MarkPublishedAsync(
        string id,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a failed publish attempt.
    /// </summary>
    /// <param name="id">The outbox message identifier.</param>
    /// <param name="attemptedAt">The UTC time when the publish attempt failed.</param>
    /// <param name="failure">The failure description.</param>
    /// <param name="terminal">A value indicating whether the message exhausted retry attempts.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that completes when the message has been updated.</returns>
    public ValueTask MarkFailedAsync(
        string id,
        DateTimeOffset attemptedAt,
        string failure,
        bool terminal = false,
        CancellationToken cancellationToken = default);
}
