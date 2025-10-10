using Microsoft.AspNetCore.Mvc;
using Models.Dto.V1.Requests;
using Models.Dto.V1.Responses;
using WebApi.BLL.Models;
using WebApi.BLL.Services;
using WebApi.Validators;

namespace WebApi.Controllers.V1;

[Route("api/v1/audit")]
[ApiController]
public class AuditLogOrderController(AuditLogOrderService auditService, ValidatorFactory validatorFactory): ControllerBase
{
    [HttpPost("log-order")]
    public async Task<ActionResult<V1AuditLogOrderResponse>> V1BatchCreate([FromBody] V1AuditLogOrderRequest request,
        CancellationToken token)
    {
        var validationResult = await validatorFactory.GetValidator<V1AuditLogOrderRequest>().ValidateAsync(request, token);
        if (!validationResult.IsValid)
        {
            return BadRequest(validationResult.ToDictionary());
        }

        var res = await auditService.BatchInsert(request.Orders.Select(x => new AuditLogOrderUnit
        {
            OrderId = x.OrderId,
            OrderItemId = x.OrderItemId,
            CustomerId = x.CustomerId,
            OrderStatus = x.OrderStatus
        }).ToArray(), token);
        
        return Ok(new V1AuditLogOrderResponse()
        {
            Orders = Map(res)
        });
    }

    private Models.Dto.Common.AuditLogOrderUnit[] Map(AuditLogOrderUnit[] audits)
    {
        return audits.Select(x => new Models.Dto.Common.AuditLogOrderUnit
        {
            Id = x.Id,
            OrderId = x.OrderId,
            OrderItemId = x.OrderItemId,
            CustomerId = x.CustomerId,
            OrderStatus = x.OrderStatus
        }).ToArray();
    }
}