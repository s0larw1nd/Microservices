using System.Text;
using Dapper;
using WebApi.DAL.Interfaces;
using WebApi.DAL.Models;

namespace WebApi.DAL.Repositories;

public class OrderItemRepository(UnitOfWork unitOfWork) : IOrderItemRepository
{
    public async Task<V1OrderItemDal[]> BulkInsert(V1OrderItemDal[] models, CancellationToken token)
    {
        var sql = @"
        insert into order_items
        (
            order_id,
            product_id,
            quantity,
            product_title,
            product_url,
            price_cents,
            price_currency,
            created_at,
            updated_at
        )
        select
            order_id,
            product_id,
            quantity,
            product_title,
            product_url,
            price_cents,
            price_currency,
            created_at,
            updated_at
        from unnest(@Order_items)
        returning
            id,
            order_id,
            product_id,
            quantity,
            product_title,
            product_url,
            price_cents,
            price_currency,
            created_at,
            updated_at
        ";
        
        var conn = await unitOfWork.GetConnection(token);
        
        var res = await conn.QueryAsync<V1OrderItemDal>(new CommandDefinition(
            sql, new {Order_items = models}, cancellationToken: token));
        
        return res.ToArray();
    }
    
    public async Task<V1OrderItemDal[]> Query(QueryOrderItemsDalModel model, CancellationToken token)
    {
        var sql = new StringBuilder(@"
            select
                id,
                order_id,
                product_id,
                quantity,
                product_title,
                product_url,
                price_cents,
                price_currency,
                created_at,
                updated_at
            from order_items
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
            // добавляем в динамический тип данные по айдишкам
            param.Add("OrderIds", model.OrderIds);
            conditions.Add("order_id = ANY(@OrderIds)");
        }
        
        if (conditions.Count > 0)
        {
            // если условия есть, то добавляем в sql
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
        var res = await conn.QueryAsync<V1OrderItemDal>(new CommandDefinition(
            sql.ToString(), param, cancellationToken: token));

        return res.ToArray();
    }
}