using Messages;
using Microsoft.Extensions.Options;
using WebApi.BLL.Models;
using WebApi.Config;
using WebApi.DAL;
using WebApi.DAL.Interfaces;
using WebApi.DAL.Models;

namespace WebApi.BLL.Services;

public class AuditLogOrderService(UnitOfWork unitOfWork, IAuditLogOrderRepository logRepository,
    KafkaProducer _kafkaProducer, IOptions<KafkaSettings> settings)
{
    public async Task<AuditLogOrderUnit[]> BatchInsert(AuditLogOrderUnit[] auditUnits, CancellationToken token)
    {
        var now = DateTimeOffset.UtcNow;
        await using var transaction = await unitOfWork.BeginTransactionAsync(token);

        try
        {   
            V1AuditLogOrderDal[] auditLogOrderDals = auditUnits.Select(a => new V1AuditLogOrderDal
            {
                Id = a.Id,
                OrderId = a.OrderId,
                OrderItemId = a.OrderItemId,
                CustomerId =  a.CustomerId,
                OrderStatus = a.OrderStatus,
                CreatedAt = now,
                UpdatedAt = now
            }).ToArray();
            var auditLogs = await logRepository.BulkInsert(auditLogOrderDals, token);
            
            var result = auditLogs.Select(a=> new AuditLogOrderUnit
            {
                Id = a.Id,
                OrderId = a.OrderId,
                OrderItemId = a.OrderItemId,
                CustomerId =  a.CustomerId,
                OrderStatus = a.OrderStatus
            }).ToArray();
            
            //await _rabbitMqService.Publish(messages, settings.Value.OrderCreatedQueue, token);            
            await transaction.CommitAsync(token);
            return result;
        }
        catch (Exception e) 
        {
            await transaction.RollbackAsync(token);
            throw;
        }
    }

    public async Task<UpdateStatusUnit[]> BatchUpdate(UpdateStatusUnit[] updateStatusUnits, CancellationToken token)
    {
        await using var transaction = await unitOfWork.BeginTransactionAsync(token);

        try
        {
            V1UpdateOrderDal[] updateOrderDals = updateStatusUnits.Select(a => new V1UpdateOrderDal
            {
                OrderId = a.OrderId,
                OrderStatus = a.OrderStatus,
            }).ToArray();
            var updatedLogs = await logRepository.BulkUpdate(updateOrderDals, token);
            
            OmsOrderStatusChangedMessage[] messages = updatedLogs.Select(u => new OmsOrderStatusChangedMessage
            {
                OrderId =  u.OrderId,
                CustomerId =  u.CustomerId,
                OrderItemId = u.OrderItemId,
                OrderStatus = u.OrderStatus,
            }).ToArray();
            
            await _kafkaProducer.Produce(settings.Value.OmsOrderStatusChangedTopic, messages.Select(m => (m.CustomerId.ToString(), m)).ToArray(), token);
            await transaction.CommitAsync(token);
            return null;
        }
        catch (Exception e)
        {
            await transaction.RollbackAsync(token);
            throw;
        }
    }

    public async Task<AuditLogOrderUnit[]> GetLogs(QueryAuditLogOrderModel model, CancellationToken token)
    {
        var logs = await logRepository.Query(new QueryAuditLogOrderDalModel
        {
            Ids = model.Ids,
            OrderIds = model.OrderIds,
            OrderItemIds = model.OrderItemIds,
            Limit = model.PageSize,
            Offset = (model.Page - 1) * model.PageSize
        }, token);
        
        if (logs.Length is 0)
        {
            return [];
        }
        
        var result = logs.Select(a=> new AuditLogOrderUnit
        {
            Id = a.Id,
            OrderId = a.OrderId,
            OrderItemId = a.OrderItemId,
            CustomerId =  a.CustomerId,
            OrderStatus = a.OrderStatus
        }).ToArray();

        return result;
    }
}