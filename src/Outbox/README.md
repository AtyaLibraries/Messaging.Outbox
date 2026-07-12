# Atya.Messaging.Outbox

Transactional outbox store and relay primitives for reliable Atya messaging.

[![NuGet](https://img.shields.io/nuget/v/Atya.Messaging.Outbox)](https://www.nuget.org/packages/Atya.Messaging.Outbox)
[![Build](https://github.com/AtyaLibraries/Messaging.Outbox/actions/workflows/ci.yml/badge.svg?branch=development)](https://github.com/AtyaLibraries/Messaging.Outbox/actions)

## Overview

`Atya.Messaging.Outbox` captures messages in an outbox store through the existing `IMessagePublisher<TMessage>` contract, then relays pending messages through a transport publisher later. The package includes contracts, serialization helpers, an in-memory store for tests and simple hosts, and a relay that preserves message id, correlation id, headers, and runtime message type. Repeated publish failures are retried until the configured attempt budget is exhausted, then the message is marked `Failed` and is no longer loaded by the relay.

Entity Framework and broker-specific providers are intentionally out of scope for this package.

## Installation

```bash
dotnet add package Atya.Messaging.Outbox
```

## Quick Start

```csharp
using Atya.Foundation.Time;
using Atya.Messaging.Abstractions;
using Atya.Messaging.Outbox;

var store = new InMemoryOutboxStore();
var clock = new SystemClock();
var outboxPublisher = new OutboxPublisher<OrderSubmitted>(store, clock);

await outboxPublisher.PublishAsync(
    new OrderSubmitted("order-123"),
    new MessagePublishOptions(
        messageId: "message-123",
        correlationId: "correlation-123"));

IMessagePublisher transportPublisher = /* adapter implemented by a transport package */;
var relay = new OutboxRelay(store, transportPublisher, clock, maxAttempts: 3);
OutboxRelayResult result = await relay.PublishPendingAsync(maxCount: 100);

public sealed record OrderSubmitted(string OrderId);
```

## Public Surface

- `IOutboxStore` stores pending messages and records publish outcomes. Durable implementations must participate in the caller's transaction.
- `OutboxMessage` represents a serialized message, metadata, status, and retry state.
- `OutboxMessageStatus` has `Pending`, `Published`, and terminal `Failed` states.
- `OutboxMessageSerializer` serializes `MessageEnvelope<TMessage>` values and restores runtime payloads from assembly-qualified type names using Atya Foundation serialization defaults unless options are supplied.
- `OutboxPublisher<TMessage>` implements `IMessagePublisher<TMessage>` by writing to the outbox.
- `OutboxRelay` republishes pending outbox messages through the type-erased `IMessagePublisher` contract and marks messages terminally failed after `maxAttempts`.
- `InMemoryOutboxStore` provides an in-memory implementation for tests, samples, and simple hosts.

## Testing

```bash
dotnet test
```

## Benchmarks

```bash
dotnet run -c Release --project benchmarks/Outbox.Benchmarks
```

## License

MIT. See [LICENSE](https://github.com/AtyaLibraries/Messaging.Outbox/blob/development/LICENSE).
