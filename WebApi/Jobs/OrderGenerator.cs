using AutoFixture;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WebApi.BLL.Services;
using WebApi.BLL.Models;

namespace WebApi.Jobs;

public class OrderGenerator(IServiceProvider serviceProvider): BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var fixture = new Fixture();
        using var scope = serviceProvider.CreateScope();
        var orderService = scope.ServiceProvider.GetRequiredService<OrderService>();
        var logService = scope.ServiceProvider.GetRequiredService<AuditLogOrderService>();
        var states = new[] {"Created", "Processing",  "Completed", "Cancelled"};
        
        var created = -1;
        Random rnd = new Random();
        
        while (!stoppingToken.IsCancellationRequested)
        {
            var orders = Enumerable.Range(1, 50)
                .Select(_ =>
                {
                    var orderItem = fixture.Build<OrderItemUnit>()
                        .With(x => x.PriceCurrency, "RUB")
                        .With(x => x.PriceCents, 1000)
                        .Create();

                    var order = fixture.Build<OrderUnit>()
                        .With(x => x.TotalPriceCurrency, "RUB")
                        .With(x => x.TotalPriceCents, 1000)
                        .With(x => x.OrderItems, [orderItem])
                        .With(x => x.CustomerId, rnd.Next(1, 6))
                        .Create();

                    return order;
                })
                .ToArray();
            
            Console.WriteLine("Created batch of orders");
            
            await orderService.BatchInsert(orders, stoppingToken);
            created += 50;
            
            var updates = Enumerable.Range(1, rnd.Next(1,51))
                .Select(_ =>
                {
                    var update = fixture.Build<UpdateStatusUnit>()
                        .With(x => x.OrderId, rnd.Next(1, created+1))
                        .With(x => x.OrderStatus, states[rnd.Next(1, states.Length)])
                        .Create();
                    
                    return update;
                })
                .ToArray();
            
            await logService.BatchUpdate(updates, stoppingToken);
            
            await Task.Delay(250, stoppingToken);
        }
    }
}