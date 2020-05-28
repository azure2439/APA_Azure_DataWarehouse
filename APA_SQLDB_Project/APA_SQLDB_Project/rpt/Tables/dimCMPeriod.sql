CREATE TABLE [rpt].[dimCMPeriod] (
    [CMPeriodKey]   INT          IDENTITY (1, 1) NOT NULL,
    [CM_PeriodCode] VARCHAR (30) NULL,
    [IsActive]      BIT          NULL,
    [LastModified]  DATETIME     DEFAULT (getdate()) NULL,
    [LastUpdatedby] VARCHAR (40) DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([CMPeriodKey] ASC)
);

