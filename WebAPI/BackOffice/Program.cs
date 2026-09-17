using BackOffice;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text.Json;

// Run this with
// dotnet run --project thisproject.csproj

Console.WriteLine("Hello, BackOfficers!");

const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
const string exchangeName = "logs_topic";

const string queueNameBackOffice = "backOfficeQueue";
const string backOfficeBindingKey = "tour.*";

const string dlxName = "backOffice-dlx";
const string dlqName = "backOffice-dlq";
const string dlqRoutingKey = "backOffice.dead";

const string queueNameInvalid = queueNameBackOffice + ".invalid";

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
    IExchangeSpecification dlxSpec = management.Exchange(dlxName).Type(ExchangeType.DIRECT);
    await dlxSpec.DeclareAsync();

    //DLX Queue
    IQueueSpecification dlqSpec = management.Queue(dlqName).Type(QueueType.QUORUM);
    await dlqSpec.DeclareAsync();

    //Bind DLQ to DLX
    await management.Binding()
        .SourceExchange(dlxSpec)
        .DestinationQueue(dlqName)
        .Key(dlqRoutingKey)
        .BindAsync();

    //-------------------------------------------------------

    //Declaring Invalid Message Queue
    await management.Queue(queueNameInvalid)
        .Type(QueueType.QUORUM)
        .DeclareAsync();

    IPublisher invalidChannelPublisher = await connection.PublisherBuilder()
        .Queue(queueNameInvalid)
        .BuildAsync();

    //-------------------------------------------------------

    //Declaring Exchange
    IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
    await exchangeSpec.DeclareAsync();

    //Declaring Regular Queue, adding Dead Letter functionality
    IQueueSpecification BackOfficeQueue = management.Queue(queueNameBackOffice)
        .Type(QueueType.QUORUM)
        .DeadLetterExchange(dlxName)
        .DeadLetterRoutingKey(dlqRoutingKey)
        .MessageTtl(TimeSpan.FromSeconds(30))/*.Exclusive(true).AutoDelete(true)*/;
    IQueueInfo queueInfo = await BackOfficeQueue.DeclareAsync();
    string queueName = queueInfo.Name();

    //Binding Regular Queue to Exchange
    foreach (var bindingKey in new string[] { backOfficeBindingKey }) // Note, no reason for array, because we're doing tour.<EVERYTHING> with the asterisk - but here as example
    {
        IBindingSpecification binding = management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueName)
            .Key(bindingKey);
        await binding.BindAsync();
    }

    //-------------------------------------------------------

    async Task SendToInvalidChannelAsync(IContext ctx, string body, string reason)
    {
        IMessage invalidMessage = new AmqpMessage(body)
            .Property("x-validation-error", reason)
            .Property("x-validation-error", queueName);

        await invalidChannelPublisher.PublishAsync(invalidMessage);
        ctx.Accept();
        Console.WriteLine($" [Invalid] {reason}");
    }


    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .MessageHandler(async (ctx, message) =>
        {
            string body = message.BodyAsString();
            string routingKey = RoutingKey(message);

            TourInput? tour;
            try
            {
                tour = JsonSerializer.Deserialize<TourInput>(body);
            } catch (JsonException ex)
            {
                await SendToInvalidChannelAsync(ctx, body, $"Malformed JSON: {ex.Message}");
                return;
            }

            if (tour is null || !Validate(tour))
            {
                await SendToInvalidChannelAsync(ctx, body, "Message has null values");
                return;
            }


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
            //return Task.CompletedTask;
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

static bool Validate(TourInput input)
{
    if (input.name != null && input.email != null && input.trip != null && input.tourtype != null)
        return true;
    else return false;
}