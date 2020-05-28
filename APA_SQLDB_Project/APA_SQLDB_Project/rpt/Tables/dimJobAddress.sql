CREATE TABLE [rpt].[dimJobAddress] (
    [JobAddressKey] INT           IDENTITY (1, 1) NOT NULL,
    [Job_ID]        INT           NOT NULL,
    [City]          VARCHAR (40)  NULL,
    [State]         VARCHAR (100) NULL,
    [Country]       VARCHAR (100) NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [DF_dimJobAddress_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [DF_dimJobAddress_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT           CONSTRAINT [DF_dimJobAddress_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME      CONSTRAINT [DF_dimJobAddress_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME      CONSTRAINT [DF_dimJobAddress_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([JobAddressKey] ASC)
);

