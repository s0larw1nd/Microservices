using Microsoft.Extensions.Options;
using WebApi.BLL.Models;
using WebApi.Config;
using WebApi.DAL;
using WebApi.DAL.Interfaces;
using WebApi.DAL.Models;
using WebApi.DAL.Repositories;
using Messages;
using ModelsDtoCommon = Models.Dto.Common;

namespace WebApi.BLL.Services;

public class OrderService(UnitOfWork unitOfWork, IOrderRepository orderRepository, IOrderItemRepository orderItemRepository,
    RabbitMqService _rabbitMqService, IOptions<RabbitMqSettings> settings)
{
    /// <summary>
    /// Метод создания заказов
    /// </summary>
    public async Task<OrderUnit[]> BatchInsert(OrderUnit[] orderUnits, CancellationToken token)
    {
        var now = DateTimeOffset.UtcNow;
        await using var transaction = await unitOfWork.BeginTransactionAsync(token);

        try
        {
            // тут ваш бизнес код по инсерту данных в БД
            // нужно положить в БД заказы(orders), а потом их позиции (orderItems)
            // помните, что каждый orderItem содержит ссылку на order (столбец order_id)
            // OrderItem-ов может быть несколько

            V1OrderDal[] orderDals = orderUnits.Select(o => new V1OrderDal
            {
                Id = o.Id,
                CustomerId = o.CustomerId,
                DeliveryAddress = o.DeliveryAddress,
                TotalPriceCents = o.TotalPriceCents,
                TotalPriceCurrency = o.TotalPriceCurrency,
                CreatedAt = now,
                UpdatedAt = now
            }).ToArray();
            var orders = await orderRepository.BulkInsert(orderDals, token);

            V1OrderItemDal[] orderItemDals = orderUnits.SelectMany((o, index) => o.OrderItems.Select(i => new V1OrderItemDal
            {
                Id = i.Id,
                OrderId = orders[index].Id,
                ProductId = i.ProductId,
                Quantity = i.Quantity,
                ProductTitle = i.ProductTitle,
                ProductUrl = i.ProductUrl,
                PriceCents = i.PriceCents,
                PriceCurrency = i.PriceCurrency,
                CreatedAt = now,
                UpdatedAt = now
            })).ToArray();
            
            var orderItems = await orderItemRepository.BulkInsert(orderItemDals, token);
            
            ILookup<long, V1OrderItemDal> orderItemLookup = orderItems.ToLookup(x => x.OrderId);
            
            OrderCreatedMessage[] messages = orders.Select(o=> new OrderCreatedMessage
            {
                Id = o.Id,
                CustomerId = o.CustomerId,
                DeliveryAddress = o.DeliveryAddress,
                TotalPriceCents = o.TotalPriceCents,
                TotalPriceCurrency = o.TotalPriceCurrency,
                CreatedAt = o.CreatedAt,
                UpdatedAt = o.UpdatedAt,
                OrderItems = orderItems.Select(i => new ModelsDtoCommon.OrderItemUnit
                {
                    Id = i.Id,
                    OrderId = i.OrderId,
                    ProductId = i.ProductId,
                    Quantity = i.Quantity,
                    ProductTitle = i.ProductTitle,
                    ProductUrl = i.ProductUrl,
                    PriceCents = i.PriceCents,
                    PriceCurrency = i.PriceCurrency,
                    CreatedAt = i.CreatedAt,
                    UpdatedAt = i.UpdatedAt
                }).ToArray()
            }).ToArray();
            
            await _rabbitMqService.Publish(messages, settings.Value.OrderCreatedQueue, token);            
            await transaction.CommitAsync(token);
            return Map(orders, orderItemLookup);
        }
        catch (Exception e) 
        {
            await transaction.RollbackAsync(token);
            throw;
        }
    }
    
    /// <summary>
    /// Метод получения заказов
    /// </summary>
    public async Task<OrderUnit[]> GetOrders(QueryOrderItemsModel model, CancellationToken token)
    {
        var orders = await orderRepository.Query(new QueryOrdersDalModel
        {
            Ids = model.Ids,
            CustomerIds = model.CustomerIds,
            Limit = model.PageSize,
            Offset = (model.Page - 1) * model.PageSize
        }, token);

        if (orders.Length is 0)
        {
            return [];
        }
        
        ILookup<long, V1OrderItemDal> orderItemLookup = null;
        if (model.IncludeOrderItems)
        {
            var orderItems = await orderItemRepository.Query(new QueryOrderItemsDalModel
            {
                OrderIds = orders.Select(x => x.Id).ToArray(),
            }, token);

            orderItemLookup = orderItems.ToLookup(x => x.OrderId);
        }

        return Map(orders, orderItemLookup);
    }
    
    private OrderUnit[] Map(V1OrderDal[] orders, ILookup<long, V1OrderItemDal> orderItemLookup = null)
    {
        return orders.Select(x => new OrderUnit
        {
            Id = x.Id,
            CustomerId = x.CustomerId,
            DeliveryAddress = x.DeliveryAddress,
            TotalPriceCents = x.TotalPriceCents,
            TotalPriceCurrency = x.TotalPriceCurrency,
            CreatedAt = x.CreatedAt,
            UpdatedAt = x.UpdatedAt,
            OrderItems = orderItemLookup?[x.Id].Select(o => new OrderItemUnit
            {
                Id = o.Id,
                OrderId = o.OrderId,
                ProductId = o.ProductId,
                Quantity = o.Quantity,
                ProductTitle = o.ProductTitle,
                ProductUrl = o.ProductUrl,
                PriceCents = o.PriceCents,
                PriceCurrency = o.PriceCurrency,
                CreatedAt = o.CreatedAt,
                UpdatedAt = o.UpdatedAt
            }).ToArray() ?? []
        }).ToArray();
    }
}