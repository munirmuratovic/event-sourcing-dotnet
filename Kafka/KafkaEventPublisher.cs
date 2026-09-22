using System.Text.Json;
using Confluent.Kafka;
using EventSourcing.Events;
using Microsoft.Extensions.Options;

namespace EventSourcing.Kafka;

public class KafkaEventPublisher : IEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaEventPublisher(IOptions<KafkaOptions> options)
    {
        _topic = options.Value.EventsTopic;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        }).Build();
    }

    public async Task PublishAsync(DomainEvent domainEvent, CancellationToken cancellationToken = default)
    {
        var message = new Message<string, string>
        {
            Key = domainEvent.StreamId,
            Value = JsonSerializer.Serialize(domainEvent),
            Headers = new Headers { { "event-type", System.Text.Encoding.UTF8.GetBytes(domainEvent.EventType) } },
        };

        await _producer.ProduceAsync(_topic, message, cancellationToken);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
