using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;

namespace Atya.Messaging.Outbox.Samples.ConsoleApp;

/// <summary>
/// Runs the sample console application.
/// </summary>
public static class Program
{
    /// <summary>
    /// Demonstrates storing a message in the outbox and relaying it later.
    /// </summary>
    /// <returns>A task that completes when the sample has run.</returns>
    public static async Task Main()
    {
        var store = new InMemoryOutboxStore();
        var clock = new SystemClock();
        var outboxPublisher = new OutboxPublisher<OrderSubmitted>(store, clock);

        await outboxPublisher.PublishAsync(
            new OrderSubmitted("order-123"),
            new MessagePublishOptions(
                "message-123",
                "correlation-123"));

        var transportPublisher = new ConsolePublisher();
        var relay = new OutboxRelay(store, transportPublisher, clock);

        var result = await relay.PublishPendingAsync(10);

        Console.WriteLine($"Attempted: {result.AttemptedCount}");
        Console.WriteLine($"Published: {result.PublishedCount}");
        Console.WriteLine($"Failed: {result.FailedCount}");
    }

    private sealed record OrderSubmitted(string OrderId);

    private sealed class ConsolePublisher : IMessagePublisher
    {
        public ValueTask PublishAsync(
            Type messageType,
            object message,
            MessagePublishOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            Console.WriteLine($"Published {messageType.Name} with id {options?.MessageId}.");
            return ValueTask.CompletedTask;
        }
    }
}
