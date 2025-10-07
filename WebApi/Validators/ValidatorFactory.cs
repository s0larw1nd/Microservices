using FluentValidation;
using Microsoft.Extensions.DependencyInjection;

namespace WebApi.Validators;

public class ValidatorFactory(IServiceProvider serviceProvider)
{
    public IValidator<T> GetValidator<T>()
    {
        return serviceProvider.GetRequiredService<IValidator<T>>()!;
    }
}