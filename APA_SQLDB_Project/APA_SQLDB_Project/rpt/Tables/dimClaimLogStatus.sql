CREATE TABLE [rpt].[dimClaimLogStatus] (
    [LogStatusKey]  INT          IDENTITY (1, 1) NOT NULL,
    [Log_Status]    VARCHAR (20) NULL,
    [IsActive]      BIT          NULL,
    [LastModified]  DATETIME     DEFAULT (getdate()) NULL,
    [LastUpdatedby] VARCHAR (40) DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([LogStatusKey] ASC)
);

