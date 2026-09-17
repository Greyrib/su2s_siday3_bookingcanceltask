using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Xml.Linq;

// Run this with
// dotnet run --project thisproject.csproj

Console.WriteLine("Hello, Emailers!");

const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
const string exchangeName = "logs_topic";

const string queueNameEmail = "emailQueue";
const string emailBindingKey = "tour.book";

const string dlxName = "tour-dlx";
const string dlqName = "tour-dlq";
const string dlqRoutingKey = "tour.dead";

ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tutorial-receivelogstopic")
    .Build();

IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    IManagement management = connection.Management();

    //DLX Exchange
    IExchangeSpecification dlxSpec = management.Exchange(dlxName).Type(ExchangeType.FANOUT);
    await dlxSpec.DeclareAsync();

    //DLX Queue
    IQueueSpecification dlqSpec = management.Queue(dlqName).Type(QueueType.QUORUM);
    await dlqSpec.DeclareAsync();

    //Bind DLQ to DLX
    await management.Binding()
        .SourceExchange(dlxSpec)
        .DestinationQueue(dlqName)
        .BindAsync();

    IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
    await exchangeSpec.DeclareAsync();

    IQueueSpecification tempQueue = management.Queue(queueNameEmail)/*.Exclusive(true).AutoDelete(true)*/;
    IQueueInfo queueInfo = await tempQueue.DeclareAsync();
    string queueName = queueInfo.Name();

    IBindingSpecification binding = management.Binding()
        .SourceExchange(exchangeSpec)
        .DestinationQueue(queueName)
        .Key(emailBindingKey);
    await binding.BindAsync();

    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .MessageHandler((ctx, message) =>
        {
            string body = message.BodyAsString();
            string routingKey = RoutingKey(message);

            try
            {
                Console.WriteLine($" [x] Received '{routingKey}':'{body}'");
                ctx.Accept();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"[!] Bad Message '{routingKey}':'{body}' - {e.Message}");
                ctx.Discard();
            }
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

