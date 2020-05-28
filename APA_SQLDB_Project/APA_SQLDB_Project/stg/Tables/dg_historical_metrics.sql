CREATE TABLE [stg].[dg_historical_metrics] (
    [MetricName]    VARCHAR (100) NULL,
    [Count]         INT           NULL,
    [Date]          DATETIME      NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [df_governance_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [df_governance_LastModified] DEFAULT (getdate()) NULL
);

