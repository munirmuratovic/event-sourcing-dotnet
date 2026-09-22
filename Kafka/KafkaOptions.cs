namespace EventSourcing.Kafka;

public class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:29092";
    public string EventsTopic { get; set; } = "domain-events";
    public string ConsumerGroupId { get; set; } = "event-sourcing-projections";
}
