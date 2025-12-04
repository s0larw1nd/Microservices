using Consumer.Base;
using Consumer.Clients;
using Messages;
using Microsoft.Extensions.Options;
using Models.Dto.V1.Requests;
using WebApi.Config;

namespace Consumer.Consumers;

public class BatchOmsOrderCreatedConsumer(
    IOptions<RabbitMqSettings> rabbitMqSettings,
    IServiceProvider serviceProvider)
    : BaseBatchMessageConsumer<OmsOrderCreatedMessage>(rabbitMqSettings.Value, s => s.OrderCreated)
{
    public enum OrderStatus
    {
        Created,
        Processing,
        Completed,
        Cancelled
    }

    private int counter = 0;
    
    protected override async Task ProcessMessages(OmsOrderCreatedMessage[] messages)
    {
        if (counter % 5 == 0) throw new ArgumentException("Got 5 stacks");
        using var scope = serviceProvider.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<OmsClient>();
        
        await client.LogOrder(new V1AuditLogOrderRequest
        {
            Orders = messages.SelectMany(order => order.OrderItems.Select(ol => 
                new V1AuditLogOrderRequest.LogOrder
                {
                    OrderId = order.Id,
                    OrderItemId = ol.Id,
                    CustomerId = order.CustomerId,
                    OrderStatus = nameof(OrderStatus.Created)
                })).ToArray()
        }, CancellationToken.None);
        
        counter++;
    }
}