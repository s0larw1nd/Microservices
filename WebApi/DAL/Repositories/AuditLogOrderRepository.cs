using System.Text;
using Dapper;
using WebApi.DAL.Interfaces;
using WebApi.DAL.Models;

namespace WebApi.DAL.Repositories;

public class AuditLogOrderRepository(UnitOfWork unitOfWork) : IAuditLogOrderRepository
{
    public async Task<V1AuditLogOrderDal[]> BulkInsert(V1AuditLogOrderDal[] model, CancellationToken token)
    {
        var sql = @"
            insert into audit_log_order 
            (
                 order_id,
                 order_item_id,
                 customer_id,
                 order_status,
                 created_at,
                 updated_at
             )
            select
                 order_id,
                 order_item_id,
                 customer_id,
                 order_status,
                 created_at,
                 updated_at
            from unnest(@AuditLogOrders)
            returning
                 id,
                 order_id,
                 order_item_id,
                 customer_id,
                 order_status,
                 created_at,
                 updated_at;
        ";
        
        var conn = await unitOfWork.GetConnection(token);
        var res = await conn.QueryAsync<V1AuditLogOrderDal>(new CommandDefinition(
            sql, new {AuditLogOrders = model}, cancellationToken: token));
        
        return res.ToArray();
    }

    public async Task<V1UpdateOrderDal[]> BulkUpdate(V1UpdateOrderDal[] model, CancellationToken token)
    {
        var sql = @"
            UPDATE audit_log_order AS t
            SET order_status = u.orderstatus
            FROM unnest(@AuditLogOrdersUpdates) AS u(orderid, orderitemid, customerid, orderstatus)
            WHERE t.order_id = u.orderid
            RETURNING
                t.order_id,
                t.order_item_id,
                t.customer_id,
                t.order_status
        ";
        var conn = await unitOfWork.GetConnection(token);
        var res = await conn.QueryAsync<V1UpdateOrderDal>(new CommandDefinition(
            sql, new {AuditLogOrdersUpdates = model}, cancellationToken: token));
        
        return res.ToArray();
    }

    public async Task<V1AuditLogOrderDal[]> Query(QueryAuditLogOrderDalModel model, CancellationToken token)
    {
        var sql = new StringBuilder(@"
            select 
                id,
                order_id,
                order_item_id,
                customer_id,
                order_status,
                created_at,
                updated_at
            from audit_log_order
        ");
        
        var param = new DynamicParameters();
        
        var conditions = new List<string>();

        if (model.Ids?.Length > 0)
        {
            // добавляем в динамический тип данные по айдишкам
            param.Add("Ids", model.Ids);
            conditions.Add("id = ANY(@Ids)");
        }
        
        if (model.OrderIds?.Length > 0)
        {
            param.Add("OrderIds", model.OrderIds);
            conditions.Add("order_id = ANY(@OrderIds)");
        }
        
        if (model.OrderItemIds?.Length > 0)
        {
            param.Add("OrderItemIds", model.OrderItemIds);
            conditions.Add("order_item_id = ANY(@OrderItemIds)");
        }

        if (conditions.Count > 0)
        {
            sql.Append(" where " + string.Join(" and ", conditions));
        }

        if (model.Limit > 0)
        {
            sql.Append(" limit @Limit");
            param.Add("Limit", model.Limit);
        }

        if (model.Offset > 0)
        {
            sql.Append(" offset @Offset");
            param.Add("Offset", model.Offset);
        }
        
        var conn = await unitOfWork.GetConnection(token);
        var res = await conn.QueryAsync<V1AuditLogOrderDal>(new CommandDefinition(
            sql.ToString(), param, cancellationToken: token));
        
        return res.ToArray();
    }
}