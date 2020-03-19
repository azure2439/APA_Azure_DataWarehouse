﻿CREATE TABLE [dbo].[Activity_2020Q1] (
    [SEQN]                  INT             CONSTRAINT [DF_Activity_SEQN] DEFAULT ((0)) NOT NULL,
    [ID]                    VARCHAR (10)    CONSTRAINT [DF_Activity_ID] DEFAULT ('') NOT NULL,
    [ACTIVITY_TYPE]         VARCHAR (10)    CONSTRAINT [DF_Activity_ACTIVITY_TYPE] DEFAULT ('') NOT NULL,
    [TRANSACTION_DATE]      DATETIME        NULL,
    [EFFECTIVE_DATE]        DATETIME        NULL,
    [PRODUCT_CODE]          VARCHAR (31)    CONSTRAINT [DF_Activity_PRODUCT_CODE] DEFAULT ('') NOT NULL,
    [OTHER_CODE]            VARCHAR (30)    CONSTRAINT [DF_Activity_OTHER_CODE] DEFAULT ('') NOT NULL,
    [DESCRIPTION]           VARCHAR (255)   CONSTRAINT [DF_Activity_DESCRIPTION] DEFAULT ('') NOT NULL,
    [SOURCE_SYSTEM]         VARCHAR (10)    CONSTRAINT [DF_Activity_SOURCE_SYSTEM] DEFAULT ('') NOT NULL,
    [SOURCE_CODE]           VARCHAR (40)    CONSTRAINT [DF_Activity_SOURCE_CODE] DEFAULT ('') NOT NULL,
    [QUANTITY]              NUMERIC (15, 2) CONSTRAINT [DF_Activity_QUANTITY] DEFAULT ((0)) NOT NULL,
    [AMOUNT]                MONEY           CONSTRAINT [DF_Activity_AMOUNT] DEFAULT ((0)) NOT NULL,
    [CATEGORY]              VARCHAR (15)    CONSTRAINT [DF_Activity_CATEGORY] DEFAULT ('') NOT NULL,
    [UNITS]                 NUMERIC (15, 2) CONSTRAINT [DF_Activity_UNITS] DEFAULT ((0)) NOT NULL,
    [THRU_DATE]             DATETIME        NULL,
    [MEMBER_TYPE]           VARCHAR (5)     CONSTRAINT [DF_Activity_MEMBER_TYPE] DEFAULT ('') NOT NULL,
    [ACTION_CODES]          VARCHAR (255)   CONSTRAINT [DF_Activity_ACTION_CODES] DEFAULT ('') NOT NULL,
    [PAY_METHOD]            VARCHAR (50)    CONSTRAINT [DF_Activity_PAY_METHOD] DEFAULT ('') NOT NULL,
    [TICKLER_DATE]          DATETIME        NULL,
    [NOTE]                  TEXT            NULL,
    [NOTE_2]                TEXT            NULL,
    [BATCH_NUM]             VARCHAR (15)    CONSTRAINT [DF_Activity_BATCH_NUM] DEFAULT ('') NOT NULL,
    [CO_ID]                 VARCHAR (10)    CONSTRAINT [DF_Activity_CO_ID] DEFAULT ('') NOT NULL,
    [OBJECT]                IMAGE           NULL,
    [INTENT_TO_EDIT]        VARCHAR (80)    CONSTRAINT [DF_Activity_INTENT_TO_EDIT] DEFAULT ('') NOT NULL,
    [UF_1]                  VARCHAR (255)   CONSTRAINT [DF_Activity_UF_1] DEFAULT ('') NOT NULL,
    [UF_2]                  VARCHAR (255)   CONSTRAINT [DF_Activity_UF_2] DEFAULT ('') NOT NULL,
    [UF_3]                  VARCHAR (255)   CONSTRAINT [DF_Activity_UF_3] DEFAULT ('') NOT NULL,
    [UF_4]                  NUMERIC (15, 4) CONSTRAINT [DF_Activity_UF_4] DEFAULT ((0)) NOT NULL,
    [UF_5]                  NUMERIC (15, 4) CONSTRAINT [DF_Activity_UF_5] DEFAULT ((0)) NOT NULL,
    [UF_6]                  DATETIME        NULL,
    [UF_7]                  DATETIME        NULL,
    [ORIGINATING_TRANS_NUM] INT             CONSTRAINT [DF_Activity_ORIGINATING_TRANS_NUM] DEFAULT ((0)) NOT NULL,
    [ORG_CODE]              VARCHAR (5)     CONSTRAINT [DF_Activity_org_CODE] DEFAULT ('') NOT NULL,
    [CAMPAIGN_CODE]         VARCHAR (10)    CONSTRAINT [DF_Activity_CAMPAIGN_CODE] DEFAULT ('') NOT NULL,
    [OTHER_ID]              VARCHAR (10)    CONSTRAINT [DF_Activity_OTHER_ID] DEFAULT ('') NOT NULL,
    [SOLICITOR_ID]          VARCHAR (10)    CONSTRAINT [DF_Activity_SOLICITOR_ID] DEFAULT ('') NOT NULL,
    [TAXABLE_VALUE]         MONEY           CONSTRAINT [DF_Activity_TAXABLE_VALUE] DEFAULT ((0)) NOT NULL,
    [ATTACH_SEQN]           INT             CONSTRAINT [DF_Activity_ATTACH_SEQN] DEFAULT ((0)) NOT NULL,
    [ATTACH_TOTAL]          INT             CONSTRAINT [DF_Activity_ATTACH_TOTAL] DEFAULT ((0)) NOT NULL,
    [RECURRING_REQUEST]     BIT             CONSTRAINT [DF_Activity_RECURRING_REQUEST] DEFAULT ((0)) NOT NULL,
    [STATUS_CODE]           VARCHAR (1)     NULL,
    [NEXT_INSTALL_DATE]     DATETIME        NULL,
    [GRACE_PERIOD]          INT             NULL,
    [MEM_TRIB_CODE]         VARCHAR (10)    CONSTRAINT [DF_Activity_MEM_TRIB_CODE] DEFAULT ('') NOT NULL,
    [TIME_STAMP]            ROWVERSION      NULL,
    CONSTRAINT [PK_Activity] PRIMARY KEY NONCLUSTERED ([SEQN] ASC)
);

