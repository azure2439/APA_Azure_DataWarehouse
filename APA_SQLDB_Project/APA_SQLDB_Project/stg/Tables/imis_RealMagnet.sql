CREATE TABLE [stg].[imis_RealMagnet] (
    [ID]                VARCHAR (10)   DEFAULT ('') NOT NULL,
    [SEQN]              INT            DEFAULT ((0)) NOT NULL,
    [ActivityCode]      VARCHAR (50)   DEFAULT ('') NOT NULL,
    [ActivitySubcoded]  VARCHAR (50)   DEFAULT ('') NOT NULL,
    [EmailAddress]      VARCHAR (100)  DEFAULT ('') NOT NULL,
    [RecipientID]       VARCHAR (32)   DEFAULT ('') NOT NULL,
    [DateStampUTC]      DATETIME       NULL,
    [CategoryName]      VARCHAR (50)   DEFAULT ('') NOT NULL,
    [GroupName]         VARCHAR (50)   DEFAULT ('') NOT NULL,
    [MessageName]       VARCHAR (50)   DEFAULT ('') NOT NULL,
    [LinkUrl]           VARCHAR (1500) NULL,
    [LinkLabel]         VARCHAR (50)   DEFAULT ('') NOT NULL,
    [TIME_STAMP]        BIGINT         NULL,
    [Id_Identitycolumn] INT            IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40)   CONSTRAINT [df_realmagLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME       CONSTRAINT [df_realmagLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT            NULL,
    [StartDate]         DATETIME       CONSTRAINT [df_realmagStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME       CONSTRAINT [df_realmagEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_RealMagnet] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

