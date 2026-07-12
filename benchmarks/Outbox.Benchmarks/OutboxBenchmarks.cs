using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;
using BenchmarkDotNet.Attributes;

namespace Atya.Messaging.Outbox.Benchmarks;

/// <summary>
/// Benchmarks representative outbox publish operations.
/// </summary>
[MemoryDiagnoser]
public class OutboxBenchmarks
{
    private readonly SystemClock _clock = new();
    private InMemoryOutboxStore _store = null!;
    private OutboxPublisher<BenchmarkMessage> _publisher = null!;

    /// <summary>
    /// Gets or sets the number of messages to write per benchmark operation.
    /// </summary>
    [Params(1, 10, 100)]
    public int Count { get; set; }

    /// <summary>
    /// Creates benchmark fixtures.
    /// </summary>
    [IterationSetup]
    public void Setup()
    {
        _store = new InMemoryOutboxStore();
        _publisher = new OutboxPublisher<BenchmarkMessage>(_store, _clock);
    }

    /// <summary>
    /// Stores messages through the outbox publisher.
    /// </summary>
    /// <returns>The number of stored messages.</returns>
    [Benchmark]
    public async ValueTask<int> PublishToOutboxAsync()
    {
        for (var i = 0; i < Count; i++)
        {
            await _publisher.PublishAsync(
                new BenchmarkMessage(i, $"message-{i}"),
                new MessagePublishOptions($"message-{i}", $"correlation-{i}"));
        }

        return _store.Messages.Count;
    }

    private sealed record BenchmarkMessage(int Id, string Name);
}
