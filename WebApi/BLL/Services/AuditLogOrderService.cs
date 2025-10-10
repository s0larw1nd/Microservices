using Microsoft.Extensions.Options;
using WebApi.BLL.Models;
using WebApi.Config;
using WebApi.DAL;
using WebApi.DAL.Interfaces;
using WebApi.DAL.Models;

namespace WebApi.BLL.Services;

public class AuditLogOrderService(UnitOfWork unitOfWork, IAuditLogOrderRepository logRepository,
    RabbitMqService _rabbitMqService, IOptions<RabbitMqSettings> settings)
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