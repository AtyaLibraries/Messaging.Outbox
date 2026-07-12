namespace Atya.Messaging.Outbox;

/// <summary>
/// Describes the lifecycle state of an outbox message.
/// </summary>
public enum OutboxMessageStatus
{
    /// <summary>
    /// The message is waiting to be published.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// The message was published successfully.
    /// </summary>
    Published = 1,

    /// <summary>
    /// The message exhausted retry attempts and will not be loaded for publishing again.
    /// </summary>
    Failed = 2,
}
