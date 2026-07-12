using System.Text.Json;
using Atya.Foundation.Guards;
using Atya.Foundation.Serialization;
using Atya.Messaging.Abstractions;

namespace Atya.Messaging.Outbox;

/// <summary>
/// Serializes and deserializes outbox message payloads using Atya Foundation serialization defaults unless options are supplied.
/// </summary>
public sealed class OutboxMessageSerializer
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxMessageSerializer"/> class.
    /// </summary>
    /// <param name="jsonSerializerOptions">Optional JSON serializer options. When omitted, the shared Atya Foundation serialization configuration is used.</param>
    public OutboxMessageSerializer(JsonSerializerOptions? jsonSerializerOptions = null)
    {
        _jsonSerializerOptions = jsonSerializerOptions is null
            ? JsonSerializerConfiguration.CreateDefault()
            : new JsonSerializerOptions(jsonSerializerOptions);
    }

    /// <summary>
    /// Resolves the runtime message type stored in an outbox message from its assembly-qualified type name.
    /// </summary>
    /// <param name="message">The outbox message.</param>
    /// <returns>The runtime message type.</returns>
    public static Type ResolveMessageType(OutboxMessage message)
    {
        Guard.AgainstNull(message);

        return Type.GetType(message.MessageTypeName, throwOnError: true)!;
    }

    /// <summary>
    /// Serializes a message envelope into an outbox message.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="envelope">The message envelope.</param>
    /// <param name="occurredAt">The UTC time when the message was added to the outbox.</param>
    /// <returns>The serialized outbox message.</returns>
    public OutboxMessage Serialize<TMessage>(
        MessageEnvelope<TMessage> envelope,
        DateTimeOffset occurredAt)
    {
        Guard.AgainstNull(envelope);

        var messageType = typeof(TMessage);
        var messageTypeName = messageType.AssemblyQualifiedName
            ?? throw new InvalidOperationException($"Message type '{messageType.FullName}' has no assembly-qualified name.");
        var payload = JsonSerializer.Serialize(envelope.Message, _jsonSerializerOptions);

        return new OutboxMessage(
            Guid.NewGuid().ToString("N"),
            messageTypeName,
            payload,
            envelope.MessageId,
            envelope.CorrelationId,
            envelope.Headers,
            occurredAt);
    }

    /// <summary>
    /// Deserializes the stored payload using the message's runtime type.
    /// </summary>
    /// <param name="message">The outbox message.</param>
    /// <returns>The deserialized message payload.</returns>
    public object Deserialize(OutboxMessage message)
    {
        var messageType = ResolveMessageType(message);
        return JsonSerializer.Deserialize(message.Payload, messageType, _jsonSerializerOptions)
            ?? throw new InvalidOperationException($"Outbox message '{message.Id}' deserialized to null.");
    }
}
