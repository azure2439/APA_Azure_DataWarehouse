CREATE TABLE [etl].[executionlog] (
    [pipeline_name] NVARCHAR (60) NULL,
    [source_table]  NVARCHAR (60) NULL,
    [target_table]  NVARCHAR (60) NULL,
    [insert_count]  INT           NULL,
    [update_count]  INT           NULL,
    [rows_read]     INT           NULL,
    [datetimestamp] DATETIME      CONSTRAINT [DF_executionlog_datetimestamp] DEFAULT (getdate()) NULL,
    [Id_Identity]   INT           IDENTITY (1, 1) NOT NULL
);

