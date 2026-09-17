using Microsoft.AspNetCore.Mvc;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

namespace WebAPI.Controllers;

public class TourInput
{
    public string name { get; set; }
    public string email { get; set; }
    public string trip { get; set; }
    public string tourtype { get; set; }
}


[ApiController]
[Route("api/[controller]")]
public class TripController : ControllerBase
{
    [HttpPost("book")]
    public async Task<IActionResult> Book([FromBody] TourInput ti)
    {
        string routingKey = "tour." + ti.tourtype; // tourtype should be 'book' or 'cancel'

        string message = ti.name + ti.email;

        await Send_Message(message, routingKey);

        return Ok("Received and did something.");
    }

    const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string exchangeName = "logs_topic";

    const string queueNameBackOffice = "backOfficeQueue";
    const string backOfficeBindingKey = "tour.*";
            
    const string queueNameEmail = "emailQueue";
    const string emailBindingKey = "tour.book";

    //const string dlxName = "tour-dlx";
    //const string dlqName = "tour-dlq";
    //const string dlqRoutingKey = "tour.dead";

    async Task Send_Message(string message, string routingKey)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(brokerUri))
            .ContainerId("tutorial-emitlogtopic")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);
        IConnection connection = await environment.CreateConnectionAsync();
    
        try
        {
            IManagement management = connection.Management();

            ////DLX Exchange
            //IExchangeSpecification dlxSpec = management.Exchange(dlxName).Type(ExchangeType.FANOUT);
            //await dlxSpec.DeclareAsync();

            ////DLX Queue
            //IQueueSpecification dlqSpec = management.Queue(dlqName).Type(QueueType.QUORUM);
            //await dlqSpec.DeclareAsync();

            ////Bind DLQ to DLX
            //await management.Binding()
            //    .SourceExchange(dlxSpec)
            //    .DestinationQueue(dlqName)
            //    .BindAsync();

            //Main Exchange
            IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("topic");
            await exchangeSpec.DeclareAsync();

            ////Create BackOffice Queue
            //IQueueSpecification queueBackOffice = management.Queue(queueNameBackOffice)
            //    .Type(QueueType.QUORUM)
            //    .DeadLetterExchange(dlxName)
            //    .DeadLetterRoutingKey(dlqRoutingKey)
            //    .MessageTtl(TimeSpan.FromSeconds(30))/*.Exclusive(true).AutoDelete(true)*/;
            //IQueueInfo queueInfoBackOffice = await queueBackOffice.DeclareAsync();
            
            ////Create Email Queue
            //IQueueSpecification queueEmail = management.Queue(queueNameEmail)/*.Exclusive(true).AutoDelete(true)*/;
            //IQueueInfo queueInfoEmail = await queueEmail.DeclareAsync();

            ////Bind BackOffice Queue
            //IBindingSpecification bindingBackOffice = management.Binding()
            //.SourceExchange(exchangeSpec)
            //.DestinationQueue(queueNameBackOffice)
            //.Key(backOfficeBindingKey);
            //await bindingBackOffice.BindAsync();

            ////Bind BackOffice Queue
            //IBindingSpecification bindingEmail = management.Binding()
            //.SourceExchange(exchangeSpec)
            //.DestinationQueue(queueNameEmail)
            //.Key(emailBindingKey);
            //await bindingEmail.BindAsync();

            IPublisher publisher = await connection.PublisherBuilder().Exchange(exchangeName).Key(routingKey).BuildAsync();
            try
            {
                var amqpMessage = new AmqpMessage(Encoding.UTF8.GetBytes(message));
                PublishResult pr = await publisher.PublishAsync(amqpMessage);
                switch (pr.Outcome.State)
                {
                    case OutcomeState.Accepted:
                        break;
                    case OutcomeState.Released:
                        Console.Error.WriteLine($"Released message: {pr.Message.BodyAsString()}");
                        Environment.Exit(1);
                        break;
                    case OutcomeState.Rejected:
                        Console.Error.WriteLine($"[Publisher] Message: {pr.Message.BodyAsString()} rejected with error: {pr.Outcome.Error}");
                        Environment.Exit(1);
                        break;
                    default:
                        Console.Error.WriteLine($"Unexpected publish outcome: {pr.Outcome.State}");
                        Environment.Exit(1);
                        break;
                }

                Console.WriteLine($" [x] Sent '{routingKey}':'{message}'");
            }
            finally
            {
                await publisher.CloseAsync();
            }
        }
        finally
        {
            await connection.CloseAsync();
            await environment.CloseAsync();
        }
    }

    static string GetRouting(string[] strings) => strings.Length < 1 ? "anonymous.info" : strings[0];

    static string GetMessage(string[] strings)
    {
        if (strings.Length < 2)
        {
            return "Hello World!";
        }

        return string.Join(" ", strings.Skip(1));
    }

}
