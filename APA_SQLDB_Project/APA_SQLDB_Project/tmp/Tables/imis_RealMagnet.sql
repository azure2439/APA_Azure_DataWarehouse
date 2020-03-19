CREATE TABLE [tmp].[imis_RealMagnet] (
    [ID]               VARCHAR (10)   NOT NULL,
    [SEQN]             INT            DEFAULT ((0)) NOT NULL,
    [ActivityCode]     VARCHAR (50)   DEFAULT ('') NOT NULL,
    [ActivitySubcoded] VARCHAR (50)   DEFAULT ('') NOT NULL,
    [EmailAddress]     VARCHAR (100)  DEFAULT ('') NOT NULL,
    [RecipientID]      VARCHAR (32)   DEFAULT ('') NOT NULL,
    [DateStampUTC]     DATETIME       NULL,
    [CategoryName]     VARCHAR (50)   DEFAULT ('') NOT NULL,
    [GroupName]        VARCHAR (50)   DEFAULT ('') NOT NULL,
    [MessageName]      VARCHAR (50)   DEFAULT ('') NOT NULL,
    [LinkUrl]          VARCHAR (1500) NULL,
    [LinkLabel]        VARCHAR (50)   DEFAULT ('') NOT NULL,
    [TIME_STAMP]       ROWVERSION     NULL,
    CONSTRAINT [PK_RealMagnet] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

