using EventSourcing.Events;

namespace EventSourcing.Kafka;

public interface IEventPublisher
{
    Task PublishAsync(DomainEvent domainEvent, CancellationToken cancellationToken = default);
}
