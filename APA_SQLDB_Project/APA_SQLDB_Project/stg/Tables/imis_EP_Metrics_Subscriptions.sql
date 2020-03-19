CREATE TABLE [stg].[imis_EP_Metrics_Subscriptions] (
    [ID]                      VARCHAR (10) DEFAULT ('') NOT NULL,
    [SEQN]                    INT          DEFAULT ((0)) NOT NULL,
    [EP_METRICS_GENERAL_SEQN] INT          DEFAULT ((0)) NOT NULL,
    [PRODUCT_CODE]            VARCHAR (30) DEFAULT ('') NOT NULL,
    [PAID_THRU]               DATETIME     NULL,
    [PROD_TYPE]               VARCHAR (10) DEFAULT ('') NOT NULL,
    [STATUS]                  VARCHAR (2)  DEFAULT ('') NOT NULL,
    [PROGRAM_YEAR]            INT          DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]              BIGINT       NULL,
    [Id_Identitycolumn]       INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]           VARCHAR (40) CONSTRAINT [df_subsLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]            DATETIME     CONSTRAINT [df_subsLastModified] DEFAULT (getdate()) NULL,
    [IsActive]                BIT          NULL,
    [StartDate]               DATETIME     CONSTRAINT [df_subsStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                 DATETIME     CONSTRAINT [df_subsEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_EP_Metrics_Subscriptions] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

