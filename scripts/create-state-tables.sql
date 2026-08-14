-- Tables SqlCdc keeps its own state in.
--
-- Both are created automatically on first use. Run this script instead when the application has
-- no DDL rights at runtime, which is the usual arrangement in a locked-down environment, and
-- construct the store and the sink with createTableIfMissing: false.
--
--   new SqlCdcStateStore(connectionString, createTableIfMissing: false)
--   new SqlCdcDeadLetterSink(connectionString, createTableIfMissing: false)
--
-- Change the schema and table names here if you pass different ones to the constructors.
-- Minimum rights for the application afterwards: SELECT, INSERT and UPDATE on cdc_watermark,
-- INSERT on cdc_dead_letter, plus SELECT on the cdc schema for reading the change tables.

IF OBJECT_ID(N'[dbo].[cdc_watermark]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[cdc_watermark]
    (
        CaptureInstance nvarchar(128) NOT NULL PRIMARY KEY,
        LastLsn         binary(10)    NOT NULL,
        UpdatedAt       datetime2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID(N'[dbo].[cdc_dead_letter]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[cdc_dead_letter]
    (
        Id              bigint IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        CaptureInstance nvarchar(128)  NOT NULL,
        SourceSchema    nvarchar(128)  NOT NULL,
        SourceTable     nvarchar(128)  NOT NULL,
        Operation       nvarchar(20)   NOT NULL,
        ChangeKey       nvarchar(64)   NOT NULL,
        CommitTime      datetime2      NULL,
        Payload         nvarchar(max)  NULL,
        HandlerName     nvarchar(256)  NOT NULL,
        Attempts        int            NOT NULL,
        Error           nvarchar(max)  NULL,
        FailedAt        datetime2      NOT NULL
    );

    -- Dead letters are read by table and by age when someone goes looking for what failed.
    CREATE INDEX IX_cdc_dead_letter_SourceTable_FailedAt
        ON [dbo].[cdc_dead_letter] (SourceTable, FailedAt);
END
GO
