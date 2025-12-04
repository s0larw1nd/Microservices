namespace WebApi.DAL;

public class V1UpdateOrderDal
{
    public long OrderId { get; set; }
    public long OrderItemId { get; set; }
    public long CustomerId { get; set; }
    public string OrderStatus { get; set; }
}