CREATE DATABASE retail;
GO
USE retail;
GO
EXEC sys.sp_cdc_enable_db;
GO
CREATE TABLE dbo.orders (
    order_id INT NOT NULL PRIMARY KEY,
    store_id INT NOT NULL,
    item_id INT NOT NULL,
    quantity INT NOT NULL,
    status NVARCHAR(20) NOT NULL
);
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL;
GO
INSERT INTO dbo.orders VALUES (1, 10, 100, 5, 'new'), (2, 10, 101, 3, 'new'), (3, 11, 100, 7, 'new');
GO
