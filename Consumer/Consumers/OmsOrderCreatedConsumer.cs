using Consumer.Base;
using Consumer.Clients;
using Consumer.Config;
using Messages;
using Microsoft.Extensions.Options;
using Models.Dto.V1.Requests;

namespace Consumer.Consumers;

public class OmsOrderCreatedConsumer(
    IOptions<KafkaSettings> kafkaSettings,
    ILogger<BaseKafkaConsumer<OmsOrderCreatedMessage>> logger,
    IServiceProvider serviceProvider)
    : BaseKafkaConsumer<OmsOrderCreatedMessage>(kafkaSettings, kafkaSettings.Value.OmsOrderCreatedTopic, logger)
{
    public enum OrderStatus
    {
        Created,
        Processing,
        Completed,
        Cancelled
    }
    
    protected override async Task ProcessMessages(Message<OmsOrderCreatedMessage>[] messages)
    {
        using var scope = serviceProvider.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<OmsClient>();
        
        await client.LogOrder(new V1AuditLogOrderRequest
        {
            Orders = messages.SelectMany(order => order.Body.OrderItems.Select(ol => 
                new V1AuditLogOrderRequest.LogOrder
                {
                    OrderId = order.Body.Id,
                    OrderItemId = ol.Id,
                    CustomerId = order.Body.CustomerId,
                    OrderStatus = nameof(OrderStatus.Created)
                })).ToArray()
        }, CancellationToken.None);
    }
}