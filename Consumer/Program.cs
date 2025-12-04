using Consumer.Clients;
using Consumer.Config;
using Consumer.Consumers;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddLogging();
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection(nameof(KafkaSettings)));
builder.Services.AddHostedService<OmsOrderCreatedConsumer>();
builder.Services.AddHostedService<OmsOrderStatusChangedConsumer>();
builder.Services.AddHttpClient<OmsClient>(c => c.BaseAddress = new Uri(builder.Configuration["HttpClient:Oms:BaseAddress"]));

builder.Services.Configure<HostOptions>(options =>
{
    options.ServicesStartConcurrently = true;
    options.ServicesStopConcurrently = true;
});

var app = builder.Build();
await app.RunAsync();