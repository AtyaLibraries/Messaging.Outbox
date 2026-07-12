using Atya.Foundation.Guards;
using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;

namespace Atya.Messaging.Outbox;

/// <summary>
/// Publishes pending outbox messages through a transport publisher.
/// </summary>
public sealed class OutboxRelay
{
    private readonly IOutboxStore _store;
    private readonly IMessagePublisher _publisher;
    private readonly IClock _clock;
    private readonly int _maxAttempts;
    private readonly OutboxMessageSerializer _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxRelay"/> class.
    /// </summary>
    /// <param name="store">The outbox store.</param>
    /// <param name="publisher">The type-erased message publisher.</param>
    /// <param name="clock">The UTC clock.</param>
    /// <param name="maxAttempts">The maximum publish attempts before a message is marked failed permanently.</param>
    /// <param name="serializer">Optional outbox message serializer.</param>
    public OutboxRelay(
        IOutboxStore store,
        IMessagePublisher publisher,
        IClock clock,
        int maxAttempts = 3,
        OutboxMessageSerializer? serializer = null)
    {
        _store = Guard.AgainstNull(store);
        _publisher = Guard.AgainstNull(publisher);
        _clock = Guard.AgainstNull(clock);
        _maxAttempts = Guard.AgainstZeroOrNegative(maxAttempts);
        _serializer = serializer ?? new OutboxMessageSerializer();
    }

    /// <summary>
    /// Publishes pending outbox messages.
    /// </summary>
    /// <param name="maxCount">The maximum number of messages to publish.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The relay result.</returns>
    public async ValueTask<OutboxRelayResult> PublishPendingAsync(
        int maxCount,
        CancellationToken cancellationToken = default)
    {
        Guard.AgainstZeroOrNegative(maxCount);
        cancellationToken.ThrowIfCancellationRequested();

        var messages = await _store.LoadPendingAsync(maxCount, cancellationToken).ConfigureAwait(false);
        var published = 0;
        var failed = 0;

        foreach (var message in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var messageType = OutboxMessageSerializer.ResolveMessageType(message);
                var payload = _serializer.Deserialize(message);
                var options = new MessagePublishOptions(
                    message.MessageId,
                    message.CorrelationId,
                    message.Headers);

                await _publisher.PublishAsync(messageType, payload, options, cancellationToken).ConfigureAwait(false);
                await _store.MarkPublishedAsync(message.Id, _clock.UtcNowOffset, cancellationToken).ConfigureAwait(false);
                published++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var terminal = message.AttemptCount + 1 >= _maxAttempts;
                await _store.MarkFailedAsync(message.Id, _clock.UtcNowOffset, ex.Message, terminal, cancellationToken).ConfigureAwait(false);
                failed++;
            }
        }

        return new OutboxRelayResult(messages.Count, published, failed);
    }
}
