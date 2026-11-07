-- Azure SQL control table read by the ADF Lookup activity.
CREATE SCHEMA etl;
GO
CREATE TABLE etl.source_control (
    source_id        VARCHAR(100) NOT NULL PRIMARY KEY,
    system           VARCHAR(100) NOT NULL,
    source_table     VARCHAR(128) NOT NULL,
    target_path      VARCHAR(400) NOT NULL,
    watermark_column VARCHAR(128) NOT NULL,
    key_columns      VARCHAR(400) NOT NULL,
    enabled          BIT          NOT NULL DEFAULT 1,
    last_watermark   DATETIME2    NOT NULL DEFAULT '1900-01-01'
);
GO
CREATE PROCEDURE etl.set_watermark @source_id VARCHAR(100), @watermark DATETIME2
AS
    UPDATE etl.source_control SET last_watermark = @watermark WHERE source_id = @source_id;
GO
