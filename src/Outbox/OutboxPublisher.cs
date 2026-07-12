using Atya.Foundation.Guards;
using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;

namespace Atya.Messaging.Outbox;

/// <summary>
/// Stores published messages in an outbox instead of sending them directly to a transport.
/// </summary>
/// <typeparam name="TMessage">The message payload type.</typeparam>
public sealed class OutboxPublisher<TMessage> : IMessagePublisher<TMessage>
{
    private readonly IOutboxStore _store;
    private readonly IClock _clock;
    private readonly OutboxMessageSerializer _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxPublisher{TMessage}"/> class.
    /// </summary>
    /// <param name="store">The outbox store.</param>
    /// <param name="clock">The UTC clock.</param>
    /// <param name="serializer">Optional outbox message serializer.</param>
    public OutboxPublisher(
        IOutboxStore store,
        IClock clock,
        OutboxMessageSerializer? serializer = null)
    {
        _store = Guard.AgainstNull(store);
        _clock = Guard.AgainstNull(clock);
        _serializer = serializer ?? new OutboxMessageSerializer();
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        TMessage message,
        MessagePublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var envelope = MessageEnvelope.Create(message, options);
        var outboxMessage = _serializer.Serialize(envelope, _clock.UtcNowOffset);
        await _store.AddAsync(outboxMessage, cancellationToken).ConfigureAwait(false);
    }
}
