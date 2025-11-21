using Models.Dto.Common;

namespace Messages;

public class OmsOrderCreatedMessage : BaseMessage
{
    public long Id { get; set; }
    
    public long CustomerId { get; set; }

    public string DeliveryAddress { get; set; }

    public long TotalPriceCents { get; set; }

    public string TotalPriceCurrency { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    
    public DateTimeOffset UpdatedAt { get; set; }

    public OrderItemUnit[] OrderItems { get; set; }

    public override string RoutingKey { get; } = "order.created";
}