using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

// Run this with
// dotnet run --project thisproject.csproj

// NOTE The tutorial uses the "args" from startup of the project as binding keys
// These projects are as of 14-09-2k26 made with specific bindings instead

Console.WriteLine("Hello, BackOfficers!");

const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
const string exchangeName = "logs_topic";

ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tutorial-receivelogstopic")
    .Build();

IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    IManagement management = connection.Management();
    IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
    await exchangeSpec.DeclareAsync();

    IQueueSpecification tempQueue = management.Queue().Exclusive(true).AutoDelete(true);
    IQueueInfo queueInfo = await tempQueue.DeclareAsync();
    string queueName = queueInfo.Name();

    foreach (var bindingKey in new string[] { "tour.*" }) // Note, no reason for array, because we're doing tour.<EVERYTHING> with the asterisk - but here as example
    {
        IBindingSpecification binding = management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueName)
            .Key(bindingKey);
        await binding.BindAsync();
    }

    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .MessageHandler((ctx, message) =>
        {
            string body = message.BodyAsString();
            string routingKey = RoutingKey(message);
            Console.WriteLine($" [x] Received '{routingKey}':'{body}'");
            ctx.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync();

    try
    {
        Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        await Task.Delay(Timeout.Infinite, cts.Token);
    }
    catch (OperationCanceledException)
    {
    }
    finally
    {
        await consumer.CloseAsync();
    }
}
finally
{
    await connection.CloseAsync();
    await environment.CloseAsync();
}

static string RoutingKey(IMessage message)
{
    object? rk = message.Annotation("x-routing-key");
    if (rk != null)
    {
        return rk.ToString() ?? "";
    }

    return message.Subject() ?? "";
}

