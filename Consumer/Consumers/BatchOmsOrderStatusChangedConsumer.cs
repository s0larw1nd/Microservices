using Consumer.Base;
using Consumer.Clients;
using Messages;
using Microsoft.Extensions.Options;
using Models.Dto.V1.Requests;
using WebApi.Config;

namespace Consumer.Consumers;

public class BatchOmsOrderStatusChangedConsumer(
    IOptions<RabbitMqSettings> rabbitMqSettings,
    IServiceProvider serviceProvider)
    : BaseBatchMessageConsumer<OmsOrderStatusChangedMessage>(rabbitMqSettings.Value, s => s.OrderStatusChanged)
{
    protected override async Task ProcessMessages(OmsOrderStatusChangedMessage[] messages)
    {
        using var scope = serviceProvider.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<OmsClient>();

        await client.LogOrder(new V1AuditLogOrderRequest
        {
            Orders = messages.Select(m => new V1AuditLogOrderRequest.LogOrder
            {
                OrderId = m.OrderId,
                OrderItemId = m.OrderItemId,
                CustomerId = m.CustomerId,
                OrderStatus = m.OrderStatus
            }).ToArray() 
        }, CancellationToken.None);
    }
}