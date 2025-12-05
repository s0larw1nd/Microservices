using FluentValidation;
using Models.Dto.V1.Requests;

namespace WebApi.Validators;

public class V1UpdateOrdersStatusRequestValidator: AbstractValidator<V1UpdateOrdersStatusRequest>
{
    public enum OrderStatusEnum
    {
        Created,
        Processing,
        Completed,
        Cancelled
    }
    
    public V1UpdateOrdersStatusRequestValidator()
    {
        RuleFor(x => x.OrderIds)
            .NotEmpty();
        
        RuleForEach(x => x.OrderIds)
            .GreaterThan(0);
        
        RuleFor(x => x.NewStatus)
            .NotEmpty()
            .Must(s => Enum.IsDefined(typeof(OrderStatusEnum), s))
            .WithMessage("Order status in [CREATED, PROCESSING, COMPLETED, CANCELLED]");;
    }
}