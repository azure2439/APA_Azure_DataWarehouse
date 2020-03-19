CREATE TABLE [rpt].[dimOrg] (
    [OrgKey]        INT           IDENTITY (1, 1) NOT NULL,
    [OrgCode]       VARCHAR (120) NULL,
    [Description]   VARCHAR (255) NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [DF_dimOrg_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [DF_dimOrg_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT           CONSTRAINT [DF_dimOrg_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME      CONSTRAINT [DF_dimOrg_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME      CONSTRAINT [DF_dimOrg_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([OrgKey] ASC)
);

