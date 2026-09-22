namespace EventSourcing.Events;

public record DomainEvent(
    Guid EventId,
    string StreamId,
    string EventType,
    string Data,
    DateTimeOffset OccurredAt
)
{
    public static DomainEvent Create(string streamId, string eventType, string data) =>
        new(Guid.NewGuid(), streamId, eventType, data, DateTimeOffset.UtcNow);
}
