using Atya.Foundation.Guards;

namespace Atya.Messaging.Outbox;

/// <summary>
/// Stores outbox messages in memory.
/// </summary>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly object _sync = new();
    private readonly List<OutboxMessage> _messages = [];

    /// <summary>
    /// Gets a snapshot of all stored messages.
    /// </summary>
    public IReadOnlyList<OutboxMessage> Messages
    {
        get
        {
            lock (_sync)
            {
                return _messages.ToArray();
            }
        }
    }

    /// <inheritdoc />
    public ValueTask AddAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.AgainstNull(message);

        lock (_sync)
        {
            if (_messages.Any(candidate => string.Equals(candidate.Id, message.Id, StringComparison.Ordinal)))
            {
                throw new ArgumentException($"Outbox message '{message.Id}' already exists.", nameof(message));
            }

            _messages.Add(message);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<OutboxMessage>> LoadPendingAsync(
        int maxCount,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.AgainstZeroOrNegative(maxCount);

        lock (_sync)
        {
            IReadOnlyList<OutboxMessage> messages = _messages
                .Where(message => message.Status == OutboxMessageStatus.Pending)
                .OrderBy(message => message.OccurredAt)
                .ThenBy(message => message.Id, StringComparer.Ordinal)
                .Take(maxCount)
                .ToArray();

            return ValueTask.FromResult(messages);
        }
    }

    /// <inheritdoc />
    public ValueTask MarkPublishedAsync(
        string id,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Update(id, message => message.MarkPublished(publishedAt));
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask MarkFailedAsync(
        string id,
        DateTimeOffset attemptedAt,
        string failure,
        bool terminal = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Update(id, message => message.MarkFailed(attemptedAt, failure, terminal));
        return ValueTask.CompletedTask;
    }

    private void Update(string id, Func<OutboxMessage, OutboxMessage> update)
    {
        Guard.AgainstNullOrWhiteSpace(id);
        Guard.AgainstNull(update);

        lock (_sync)
        {
            var index = _messages.FindIndex(message => string.Equals(message.Id, id, StringComparison.Ordinal));
            if (index < 0)
            {
                throw new KeyNotFoundException($"Outbox message '{id}' was not found.");
            }

            _messages[index] = update(_messages[index]);
        }
    }
}
