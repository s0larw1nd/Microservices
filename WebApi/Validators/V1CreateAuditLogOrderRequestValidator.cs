using FluentValidation;
using Models.Dto.V1.Requests;

namespace WebApi.Validators;

public class V1CreateAuditLogOrderRequestValidator: AbstractValidator<V1AuditLogOrderRequest>
{
    public V1CreateAuditLogOrderRequestValidator()
    {
        RuleFor(x => x.Orders)
            .NotEmpty();
        
        RuleForEach(x => x.Orders)
            .SetValidator(new LogOrderValidator())
            .When(x => x.Orders is not null);
    }

    public class LogOrderValidator : AbstractValidator<V1AuditLogOrderRequest.LogOrder>
    {
        public LogOrderValidator()
        {
            RuleFor(x => x.OrderId)
                .GreaterThan(0);
            
            RuleFor(x => x.OrderItemId)
                .GreaterThan(0);
            
            RuleFor(x => x.CustomerId)
                .GreaterThan(0);
            
            RuleFor(x => x.OrderStatus)
                .NotEmpty();
        }
    }
}