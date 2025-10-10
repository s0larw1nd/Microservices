using Models.Dto.Common;

namespace Models.Dto.V1.Responses;

public class V1AuditLogOrderResponse
{
    public AuditLogOrderUnit[] Orders { get; set; }
}