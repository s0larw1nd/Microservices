namespace Migrations.Scripts;

using FluentMigrator;

[Migration(3)]
public class InitLogTable: Migration {
    public override void Up()
    {
        var sql = @"
            create table if not exists audit_log_order (
                id bigserial not null primary key,
                order_id bigint not null,
                order_item_id bigint not null,
                customer_id bigint not null,
                order_status text not null,
                created_at timestamp with time zone not null,
                updated_at timestamp with time zone not null
            );

            create index if not exists idx_audit_log_order_order_id on audit_log_order(order_id);

            create type v1_audit_log_order as (
                id bigint,
                order_id bigint,
                order_item_id bigint,
                customer_id bigint,
                order_status text,
                created_at timestamp with time zone,
                updated_at timestamp with time zone
            );

            create type v1_update_log_order AS (
                order_id bigint,
                order_item_id bigint,
                customer_id bigint,
                order_status text
            );
        ";
        
        Execute.Sql(sql);
    }

    public override void Down()
    {
        throw new NotImplementedException();
    }
}