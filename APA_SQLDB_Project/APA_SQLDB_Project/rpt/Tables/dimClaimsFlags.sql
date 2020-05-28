CREATE TABLE [rpt].[dimClaimsFlags] (
    [FlagKey]       INT          IDENTITY (1, 1) NOT NULL,
    [Verified]      BIT          NULL,
    [Is_Speaker]    BIT          NULL,
    [Is_Author]     BIT          NULL,
    [Self_Reported] BIT          NULL,
    [Is_CarryOver]  BIT          NULL,
    [Is_Probono]    BIT          NULL,
    [Is_Deleted]    BIT          NULL,
    [IsActive]      BIT          NULL,
    [LastModified]  DATETIME     DEFAULT (getdate()) NULL,
    [LastUpdatedby] VARCHAR (40) DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([FlagKey] ASC)
);

