CREATE TABLE [dbo].[zzz_dimAddress] (
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
    [LastUpdatedBy] VARCHAR (40)   NULL,
    [LastModified]  DATETIME       NULL,
    [IsActive]      BIT            NULL,
    [StartDate]     DATETIME       NULL,
    [EndDate]       DATETIME       NULL
);

