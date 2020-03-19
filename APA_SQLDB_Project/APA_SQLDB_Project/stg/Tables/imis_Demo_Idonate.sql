CREATE TABLE [stg].[imis_Demo_Idonate] (
    [ID]                VARCHAR (10) DEFAULT ('') NOT NULL,
    [SEQN]              INT          DEFAULT ((0)) NOT NULL,
    [QuestionCode]      VARCHAR (30) DEFAULT ('') NOT NULL,
    [SelectedAnswer]    VARCHAR (60) DEFAULT ('') NOT NULL,
    [InvoiceNumber]     INT          DEFAULT ((0)) NOT NULL,
    [Create_Date]       DATETIME     NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_demoLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_demoLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_demoStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_demoEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Demo_Idonate] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

