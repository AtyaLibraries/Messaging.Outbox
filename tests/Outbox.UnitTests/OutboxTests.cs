using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;

namespace Atya.Messaging.Outbox.UnitTests;

public sealed class OutboxTests
{
    [Fact]
    public async Task Publisher_Stores_Serialized_Pending_Message()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.Zero));
        var store = new InMemoryOutboxStore();
        var publisher = new OutboxPublisher<TestMessage>(store, clock);
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new MessagePublishOptions(
            "message-1",
            "correlation-1",
            new Dictionary<string, string>
            {
                ["tenant"] = "test",
            });

        await publisher.PublishAsync(new TestMessage("created"), options, cancellationToken);

        var stored = store.Messages.Should().ContainSingle().Subject;
        stored.Status.Should().Be(OutboxMessageStatus.Pending);
        stored.MessageId.Should().Be("message-1");
        stored.CorrelationId.Should().Be("correlation-1");
        stored.Headers.Should().Contain("tenant", "test");
        stored.OccurredAt.Should().Be(clock.UtcNowOffset);
        stored.MessageTypeName.Should().Contain(nameof(TestMessage));
        stored.Payload.Should().Contain("created");
    }

    [Fact]
    public async Task Relay_Publishes_Runtime_Message_Type_And_Marks_Published()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.Zero));
        var store = new InMemoryOutboxStore();
        var source = new OutboxPublisher<TestMessage>(store, clock);
        var cancellationToken = TestContext.Current.CancellationToken;
        await source.PublishAsync(
            new TestMessage("created"),
            new MessagePublishOptions("message-1", "correlation-1"),
            cancellationToken);
        var publishedAt = clock.UtcNowOffset.AddMinutes(1);
        clock.UtcNowOffset = publishedAt;
        var transport = new RecordingPublisher();
        var relay = new OutboxRelay(store, transport, clock);

        var result = await relay.PublishPendingAsync(10, cancellationToken);

        result.AttemptedCount.Should().Be(1);
        result.PublishedCount.Should().Be(1);
        result.FailedCount.Should().Be(0);
        transport.Published.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new PublishedMessage(typeof(TestMessage), "created", "message-1", "correlation-1"));
        var stored = store.Messages.Should().ContainSingle().Subject;
        stored.Status.Should().Be(OutboxMessageStatus.Published);
        stored.PublishedAt.Should().Be(publishedAt);
        stored.LastError.Should().BeNull();
    }

    [Fact]
    public async Task Relay_Records_Failure_And_Keeps_Message_Pending()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.Zero));
        var store = new InMemoryOutboxStore();
        var source = new OutboxPublisher<TestMessage>(store, clock);
        var cancellationToken = TestContext.Current.CancellationToken;
        await source.PublishAsync(new TestMessage("created"), cancellationToken: cancellationToken);
        var failedAt = clock.UtcNowOffset.AddMinutes(1);
        clock.UtcNowOffset = failedAt;
        var relay = new OutboxRelay(store, new ThrowingPublisher(), clock);

        var result = await relay.PublishPendingAsync(10, cancellationToken);

        result.AttemptedCount.Should().Be(1);
        result.PublishedCount.Should().Be(0);
        result.FailedCount.Should().Be(1);
        var stored = store.Messages.Should().ContainSingle().Subject;
        stored.Status.Should().Be(OutboxMessageStatus.Pending);
        stored.AttemptCount.Should().Be(1);
        stored.LastAttemptAt.Should().Be(failedAt);
        stored.LastError.Should().Be("transport unavailable");
    }

    [Fact]
    public async Task Relay_Marks_Message_Failed_After_Max_Attempts_And_Stops_Reloading()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.Zero));
        var store = new InMemoryOutboxStore();
        var source = new OutboxPublisher<TestMessage>(store, clock);
        var cancellationToken = TestContext.Current.CancellationToken;
        await source.PublishAsync(new TestMessage("created"), cancellationToken: cancellationToken);
        var relay = new OutboxRelay(store, new ThrowingPublisher(), clock, maxAttempts: 2);

        var firstResult = await relay.PublishPendingAsync(10, cancellationToken);
        clock.UtcNowOffset = clock.UtcNowOffset.AddMinutes(1);
        var secondResult = await relay.PublishPendingAsync(10, cancellationToken);
        var pending = await store.LoadPendingAsync(10, cancellationToken);
        var thirdResult = await relay.PublishPendingAsync(10, cancellationToken);

        firstResult.AttemptedCount.Should().Be(1);
        secondResult.AttemptedCount.Should().Be(1);
        thirdResult.AttemptedCount.Should().Be(0);
        pending.Should().BeEmpty();
        var stored = store.Messages.Should().ContainSingle().Subject;
        stored.Status.Should().Be(OutboxMessageStatus.Failed);
        stored.AttemptCount.Should().Be(2);
        stored.LastError.Should().Be("transport unavailable");
    }

    [Fact]
    public async Task Store_Loads_Pending_Messages_In_Occurred_Order()
    {
        var store = new InMemoryOutboxStore();
        var cancellationToken = TestContext.Current.CancellationToken;
        await store.AddAsync(CreateMessage("c", 3), cancellationToken);
        await store.AddAsync(CreateMessage("a", 1), cancellationToken);
        await store.AddAsync(CreateMessage("b", 2), cancellationToken);

        var messages = await store.LoadPendingAsync(2, cancellationToken);

        messages.Select(message => message.Id).Should().Equal("a", "b");
    }

    [Fact]
    public async Task Store_Does_Not_Load_Published_Or_Failed_Messages()
    {
        var store = new InMemoryOutboxStore();
        var cancellationToken = TestContext.Current.CancellationToken;
        await store.AddAsync(CreateMessage("pending", 1), cancellationToken);
        await store.AddAsync(CreateMessage("published", 2), cancellationToken);
        await store.AddAsync(CreateMessage("failed", 3), cancellationToken);

        await store.MarkPublishedAsync(
            "published",
            new DateTimeOffset(2026, 7, 12, 20, 1, 0, TimeSpan.Zero),
            cancellationToken);
        await store.MarkFailedAsync(
            "failed",
            new DateTimeOffset(2026, 7, 12, 20, 1, 0, TimeSpan.Zero),
            "transport unavailable",
            terminal: true,
            cancellationToken);

        var messages = await store.LoadPendingAsync(10, cancellationToken);

        messages.Should().ContainSingle()
            .Which.Id.Should().Be("pending");
    }

    [Fact]
    public void Message_Copies_Empty_Headers_And_Normalizes_Blank_Correlation_And_Error()
    {
        var message = new OutboxMessage(
            "message",
            typeof(TestMessage).AssemblyQualifiedName!,
            """{"name":"created"}""",
            "message-1",
            " ",
            null,
            new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.Zero),
            lastError: " ");

        message.CorrelationId.Should().BeNull();
        message.Headers.Should().BeEmpty();
        message.LastError.Should().BeNull();
    }

    [Fact]
    public void Message_Requires_Utc_Timestamps()
    {
        var create = () => new OutboxMessage(
            "message",
            typeof(TestMessage).AssemblyQualifiedName!,
            """{"name":"created"}""",
            "message-1",
            null,
            null,
            new DateTimeOffset(2026, 7, 12, 20, 0, 0, TimeSpan.FromHours(1)));

        create.Should().Throw<ArgumentException>()
            .WithMessage("The value must use a UTC offset.*");
    }

    [Fact]
    public async Task Store_Rejects_Duplicate_Message_Ids()
    {
        var store = new InMemoryOutboxStore();
        var cancellationToken = TestContext.Current.CancellationToken;
        await store.AddAsync(CreateMessage("duplicate", 1), cancellationToken);

        var addDuplicate = async () => await store.AddAsync(CreateMessage("duplicate", 2), cancellationToken);

        await addDuplicate.Should().ThrowAsync<ArgumentException>()
            .WithMessage("Outbox message 'duplicate' already exists.*");
    }

    private static OutboxMessage CreateMessage(string id, int seconds)
        => new(
            id,
            typeof(TestMessage).AssemblyQualifiedName!,
            """{"name":"created"}""",
            $"message-{id}",
            null,
            new Dictionary<string, string>(),
            new DateTimeOffset(2026, 7, 12, 20, 0, seconds, TimeSpan.Zero));

    private sealed record TestMessage(string Name);

    private sealed class TestClock(DateTimeOffset utcNowOffset) : IClock
    {
        public DateTime UtcNow => UtcNowOffset.UtcDateTime;

        public DateTimeOffset UtcNowOffset { get; set; } = utcNowOffset;

        public DateOnly TodayUtc => DateOnly.FromDateTime(UtcNow);
    }

    private sealed class RecordingPublisher : IMessagePublisher
    {
        public List<PublishedMessage> Published { get; } = [];

        public ValueTask PublishAsync(
            Type messageType,
            object message,
            MessagePublishOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            var testMessage = message.Should().BeOfType<TestMessage>().Subject;
            Published.Add(new PublishedMessage(
                messageType,
                testMessage.Name,
                options?.MessageId,
                options?.CorrelationId));
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingPublisher : IMessagePublisher
    {
        public ValueTask PublishAsync(
            Type messageType,
            object message,
            MessagePublishOptions? options = null,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("transport unavailable");
    }

    private sealed record PublishedMessage(
        Type MessageType,
        string Name,
        string? MessageId,
        string? CorrelationId);
}
