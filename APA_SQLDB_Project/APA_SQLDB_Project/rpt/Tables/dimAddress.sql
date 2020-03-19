CREATE TABLE [rpt].[dimAddress] (
    [Address_Key]   INT            IDENTITY (1, 1) NOT NULL,
    [Address_Num]   BIGINT         NULL,
    [Purpose]       VARCHAR (40)   NULL,
    [Company]       VARCHAR (200)  NULL,
    [FullAddress]   VARCHAR (1000) NULL,
    [Street]        VARCHAR (80)   NULL,
    [City]          VARCHAR (40)   NULL,
    [State]         VARCHAR (15)   NULL,
    [Country]       VARCHAR (25)   NULL,
    [Zip]           VARCHAR (15)   NULL,
    [County]        VARCHAR (80)   NULL,
    [LastUpdatedBy] VARCHAR (40)   CONSTRAINT [DF_dimAddress_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME       CONSTRAINT [DF_dimAddress_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT            CONSTRAINT [DF_dimAddress_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME       CONSTRAINT [DF_dimAddress_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME       CONSTRAINT [DF_dimAddress_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([Address_Key] ASC)
);

