CREATE TABLE [stg].[imis_BDR_AuthNet_Temp] (
    [ID]                VARCHAR (10) DEFAULT ('') NOT NULL,
    [Profile_Key]       VARCHAR (30) DEFAULT ('') NOT NULL,
    [Payment_Key]       VARCHAR (30) DEFAULT ('') NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_bdrLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_bdrLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_bdrStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_bdrEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_BDR_AuthNet_Temp] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

