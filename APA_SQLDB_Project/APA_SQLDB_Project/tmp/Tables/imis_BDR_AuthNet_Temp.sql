CREATE TABLE [tmp].[imis_BDR_AuthNet_Temp] (
    [ID]          VARCHAR (10) NOT NULL,
    [Profile_Key] VARCHAR (30) DEFAULT ('') NOT NULL,
    [Payment_Key] VARCHAR (30) DEFAULT ('') NOT NULL,
    [TIME_STAMP]  ROWVERSION   NULL,
    CONSTRAINT [PK_BDR_AuthNet_Temp] PRIMARY KEY CLUSTERED ([ID] ASC)
);

