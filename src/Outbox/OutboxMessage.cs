using System.Collections.ObjectModel;
using Atya.Foundation.Guards;

namespace Atya.Messaging.Outbox;

/// <summary>
/// Represents a serialized message stored in an outbox.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxMessage"/> class.
    /// </summary>
    /// <param name="id">The outbox record identifier.</param>
    /// <param name="messageTypeName">The assembly-qualified message payload type name used by <see cref="Type.GetType(string, bool)"/> during relay.</param>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="messageId">The transport-neutral message identifier.</param>
    /// <param name="correlationId">The optional correlation identifier.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="occurredAt">The UTC time when the message was added to the outbox.</param>
    /// <param name="status">The current outbox status.</param>
    /// <param name="attemptCount">The number of publish attempts.</param>
    /// <param name="publishedAt">The UTC time when the message was published.</param>
    /// <param name="lastAttemptAt">The UTC time of the most recent publish attempt.</param>
    /// <param name="lastError">The most recent publish failure.</param>
    public OutboxMessage(
        string id,
        string messageTypeName,
        string payload,
        string messageId,
        string? correlationId,
        IReadOnlyDictionary<string, string>? headers,
        DateTimeOffset occurredAt,
        OutboxMessageStatus status = OutboxMessageStatus.Pending,
        int attemptCount = 0,
        DateTimeOffset? publishedAt = null,
        DateTimeOffset? lastAttemptAt = null,
        string? lastError = null)
    {
        Id = Guard.AgainstNullOrWhiteSpace(id);
        MessageTypeName = Guard.AgainstNullOrWhiteSpace(messageTypeName);
        Payload = Guard.AgainstNullOrWhiteSpace(payload);
        MessageId = Guard.AgainstNullOrWhiteSpace(messageId);
        CorrelationId = string.IsNullOrWhiteSpace(correlationId) ? null : correlationId;
        Headers = CopyHeaders(headers);
        OccurredAt = EnsureUtc(occurredAt, nameof(occurredAt));
        Status = status;
        AttemptCount = Guard.AgainstNegative(attemptCount);
        PublishedAt = publishedAt is null ? null : EnsureUtc(publishedAt.Value, nameof(publishedAt));
        LastAttemptAt = lastAttemptAt is null ? null : EnsureUtc(lastAttemptAt.Value, nameof(lastAttemptAt));
        LastError = string.IsNullOrWhiteSpace(lastError) ? null : lastError;
    }

    /// <summary>
    /// Gets the outbox record identifier.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Gets the assembly-qualified message payload type name used by <see cref="Type.GetType(string, bool)"/> during relay.
    /// </summary>
    public string MessageTypeName { get; }

    /// <summary>
    /// Gets the serialized message payload.
    /// </summary>
    public string Payload { get; }

    /// <summary>
    /// Gets the transport-neutral message identifier.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the optional correlation identifier.
    /// </summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// Gets the message headers.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; }

    /// <summary>
    /// Gets the UTC time when the message was added to the outbox.
    /// </summary>
    public DateTimeOffset OccurredAt { get; }

    /// <summary>
    /// Gets the current outbox status.
    /// </summary>
    public OutboxMessageStatus Status { get; }

    /// <summary>
    /// Gets the number of publish attempts.
    /// </summary>
    public int AttemptCount { get; }

    /// <summary>
    /// Gets the UTC time when the message was published.
    /// </summary>
    public DateTimeOffset? PublishedAt { get; }

    /// <summary>
    /// Gets the UTC time of the most recent publish attempt.
    /// </summary>
    public DateTimeOffset? LastAttemptAt { get; }

    /// <summary>
    /// Gets the most recent publish failure.
    /// </summary>
    public string? LastError { get; }

    /// <summary>
    /// Creates a copy marked as published.
    /// </summary>
    /// <param name="publishedAt">The UTC time when publishing succeeded.</param>
    /// <returns>The updated message.</returns>
    public OutboxMessage MarkPublished(DateTimeOffset publishedAt)
    {
        return new OutboxMessage(
            Id,
            MessageTypeName,
            Payload,
            MessageId,
            CorrelationId,
            Headers,
            OccurredAt,
            OutboxMessageStatus.Published,
            AttemptCount + 1,
            publishedAt,
            publishedAt,
            null);
    }

    /// <summary>
    /// Creates a copy with a failed publish attempt recorded.
    /// </summary>
    /// <param name="attemptedAt">The UTC time when publishing failed.</param>
    /// <param name="failure">The failure description.</param>
    /// <param name="terminal">A value indicating whether the message exhausted retry attempts.</param>
    /// <returns>The updated message.</returns>
    public OutboxMessage MarkFailed(
        DateTimeOffset attemptedAt,
        string failure,
        bool terminal = false)
    {
        return new OutboxMessage(
            Id,
            MessageTypeName,
            Payload,
            MessageId,
            CorrelationId,
            Headers,
            OccurredAt,
            terminal ? OutboxMessageStatus.Failed : OutboxMessageStatus.Pending,
            AttemptCount + 1,
            null,
            attemptedAt,
            Guard.AgainstNullOrWhiteSpace(failure));
    }

    private static DateTimeOffset EnsureUtc(DateTimeOffset value, string paramName)
    {
        if (value.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("The value must use a UTC offset.", paramName);
        }

        return value;
    }

    private static ReadOnlyDictionary<string, string> CopyHeaders(
        IReadOnlyDictionary<string, string>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return ReadOnlyDictionary<string, string>.Empty;
        }

        var copy = new Dictionary<string, string>(headers.Count, StringComparer.Ordinal);
        foreach (var (key, value) in headers)
        {
            copy.Add(Guard.AgainstNullOrWhiteSpace(key), Guard.AgainstNull(value));
        }

        return new ReadOnlyDictionary<string, string>(copy);
    }
}
