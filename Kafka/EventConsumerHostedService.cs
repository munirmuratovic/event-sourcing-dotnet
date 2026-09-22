using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace EventSourcing.Kafka;

public class EventConsumerHostedService : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly ILogger<EventConsumerHostedService> _logger;

    public EventConsumerHostedService(IOptions<KafkaOptions> options, ILogger<EventConsumerHostedService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.ConsumerGroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                AllowAutoCreateTopics = true,
            }).Build();

            consumer.Subscribe(_options.EventsTopic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);
                        _logger.LogInformation(
                            "Consumed event {EventType} for stream {Key} at offset {Offset}",
                            result.Message.Headers.TryGetLastBytes("event-type", out var bytes)
                                ? System.Text.Encoding.UTF8.GetString(bytes)
                                : "unknown",
                            result.Message.Key,
                            result.Offset);

                        // TODO: dispatch result.Message.Value to a projection/read-model handler.
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(ex, "Transient error consuming from Kafka, retrying");
                        Thread.Sleep(1000);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }, stoppingToken);
    }
}
