-- Abilita Change Data Capture per la demo del package SqlCdc.
-- Eseguire con un login che disponga di sysadmin o db_owner.

USE MyDb;
GO

-- 1. Abilita CDC sul database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 0)
    EXEC sys.sp_cdc_enable_db;
GO

-- 2. Crea tabelle demo (se non esistono)
IF OBJECT_ID(N'dbo.Orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders
    (
        Id     int          NOT NULL IDENTITY(1,1) PRIMARY KEY,
        CustomerName nvarchar(100) NULL,
        Amount decimal(18,2) NOT NULL,
        CreatedAt datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'dbo.Customers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers
    (
        Id   int           NOT NULL IDENTITY(1,1) PRIMARY KEY,
        Name nvarchar(100) NOT NULL,
        Email nvarchar(200) NULL
    );
END;
GO

-- 3. Abilita CDC sulle tabelle (la capture instance sarà dbo_Orders / dbo_Customers)
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.Orders'))
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Orders', @role_name = NULL;

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.Customers'))
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Customers', @role_name = NULL;
GO

-- 4. Verifica
SELECT source_object_id, capture_instance, start_lsn, stop_lsn
FROM cdc.change_tables;
GO

-- 5. Genera un po' di traffico da osservare dal sample
INSERT INTO dbo.Orders (CustomerName, Amount) VALUES (N'Mario Rossi', 99.50);
INSERT INTO dbo.Orders (CustomerName, Amount) VALUES (N'Giulia Bianchi', 12.00);
UPDATE dbo.Orders SET Amount = 105.00 WHERE Id = 1;
DELETE FROM dbo.Orders WHERE Id = 2;
INSERT INTO dbo.Customers (Name, Email) VALUES (N'Luca Verdi', N'luca@example.com');
GO
