// See https://aka.ms/new-console-template for more information
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Net;

Console.WriteLine("Hello, World!");
var hostname = "";
var username = "";
var password = "";
var streamName = "testOffset";
var offset = TimeSpan.FromMinutes(1);

var addresses = await Dns.GetHostAddressesAsync(hostname);
var lbAddressResolver = new AddressResolver(new IPEndPoint(addresses[0], 5552));
var config = new StreamSystemConfig()
{
    UserName = username,
    Password = password,
    VirtualHost = "mobo",
    AddressResolver = lbAddressResolver,
    Endpoints = new List<EndPoint> { lbAddressResolver.EndPoint },
    ClientProvidedName = $"test-{Guid.NewGuid()}"
};
var system = await StreamSystem.Create(config);
var timeAgo = (long)DateTime.UtcNow.AddMinutes(-1).Subtract(new DateTime(1970, 1, 1)).TotalSeconds;

await system.CreateStream(new StreamSpec(streamName) { MaxAge = TimeSpan.FromMinutes(65), MaxSegmentSizeBytes = 50000000, MaxLengthBytes = 100000000 });

var rConsumer = await ReliableConsumer.CreateReliableConsumer(new ReliableConsumerConfig()
{
    Stream = streamName,
    StreamSystem = system,
    OffsetSpec = new OffsetTypeTimestamp(timeAgo),
    ClientProvidedName = $"test{Guid.NewGuid()}",
    MessageHandler = (c, mc, message) =>
    {
        Console.WriteLine($"Received message with {message.Properties.CreationTime}");
        
        return Task.CompletedTask;
    },
});
Console.ReadLine();

