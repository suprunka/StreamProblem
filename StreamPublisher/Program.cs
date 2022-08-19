// See https://aka.ms/new-console-template for more information
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Net;
using System.Text.Json;

Console.WriteLine("Start");
var hostname = "";
var username = "";
var password = "";
var streamName = "testOffset";

var addresses = Dns.GetHostAddresses(hostname);
var lbAddressResolver = new AddressResolver(new IPEndPoint(addresses[0], 5552));
var streamConfig = new StreamSystemConfig
{
    UserName = username,
    Password = password,
    VirtualHost = "mobo",
    AddressResolver = lbAddressResolver,
    Endpoints = new List<EndPoint> { lbAddressResolver.EndPoint },
    ClientProvidedName = "test"
};
var System = await StreamSystem.Create(streamConfig);
await System.CreateStream(new StreamSpec(streamName) { MaxAge = TimeSpan.FromMinutes(65), MaxSegmentSizeBytes = 50000000, MaxLengthBytes = 100000000 });

var producer = await ReliableProducer.CreateReliableProducer(
    new ReliableProducerConfig
    {
        StreamSystem = System,
        Stream = streamName,
        ConfirmationHandler = confirmation =>
        {
            if (confirmation.Status != ConfirmationStatus.Confirmed)
                Console.WriteLine("Message is not confirmed");
            return Task.CompletedTask;
        }
    });

for (int id = 1; id < 100; id++)
{
    var message = new Message(new byte[1024]) { Properties = new RabbitMQ.Stream.Client.AMQP.Properties() { Subject = id.ToString() , CreationTime = DateTime.UtcNow} };
    await producer.Send(message);
    Console.WriteLine($"Send message with id {id}");
    await Task.Delay(1000);
}