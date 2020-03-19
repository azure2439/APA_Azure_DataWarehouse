CREATE TABLE [tmp].[imis_EP_Metrics_Subscriptions] (
    [ID]                      VARCHAR (10) NOT NULL,
    [SEQN]                    INT          DEFAULT ((0)) NOT NULL,
    [EP_METRICS_GENERAL_SEQN] INT          DEFAULT ((0)) NOT NULL,
    [PRODUCT_CODE]            VARCHAR (30) DEFAULT ('') NOT NULL,
    [PAID_THRU]               DATETIME     NULL,
    [PROD_TYPE]               VARCHAR (10) DEFAULT ('') NOT NULL,
    [STATUS]                  VARCHAR (2)  DEFAULT ('') NOT NULL,
    [PROGRAM_YEAR]            INT          DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]              ROWVERSION   NULL,
    CONSTRAINT [PK_EP_Metrics_Subscriptions] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

