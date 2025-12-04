using Consumer.Base;
using Consumer.Clients;
using Consumer.Config;
using Messages;
using Microsoft.Extensions.Options;
using Models.Dto.V1.Requests;

namespace Consumer.Consumers;

public class OmsOrderStatusChangedConsumer(
    IOptions<KafkaSettings> kafkaSettings,
    ILogger<BaseKafkaConsumer<OmsOrderStatusChangedMessage>> logger,
    IServiceProvider serviceProvider)
    : BaseKafkaConsumer<OmsOrderStatusChangedMessage>(kafkaSettings, kafkaSettings.Value.OmsOrderStatusChangedTopic, logger)
{
    protected override async Task ProcessMessages(Message<OmsOrderStatusChangedMessage>[] messages)
    {
        using var scope = serviceProvider.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<OmsClient>();

        await client.LogOrder(new V1AuditLogOrderRequest
        {
            Orders = messages.Select(m => new V1AuditLogOrderRequest.LogOrder
            {
                OrderId = m.Body.OrderId,
                OrderItemId = m.Body.OrderItemId,
                CustomerId = m.Body.CustomerId,
                OrderStatus = m.Body.OrderStatus
            }).ToArray() 
        }, CancellationToken.None);
    }
}