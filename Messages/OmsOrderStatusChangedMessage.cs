using Models.Dto.Common;

namespace Messages;

public class OmsOrderStatusChangedMessage : BaseMessage
{
    public long OrderId { get; set; }
    public long CustomerId { get; set; }
    public long OrderItemId { get; set; }
    public string OrderStatus { get; set; }
    public override string RoutingKey { get; } = "order.status.changed";
}