using EventSourcing.Events;
using EventSourcing.Kafka;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
builder.Services.AddHostedService<EventConsumerHostedService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();

app.MapPost("/events", async (PublishEventRequest request, IEventPublisher publisher) =>
{
    var domainEvent = DomainEvent.Create(request.StreamId, request.EventType, request.Data);
    await publisher.PublishAsync(domainEvent);
    return Results.Accepted(value: domainEvent);
})
.WithName("PublishEvent");

app.Run();

record PublishEventRequest(string StreamId, string EventType, string Data);
