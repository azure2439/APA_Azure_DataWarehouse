﻿CREATE TABLE [tmp].[imis_Trans] (
    [TRANS_NUMBER]          INT             CONSTRAINT [DF_Trans_TRANS_NUMBER] DEFAULT ((0)) NOT NULL,
    [LINE_NUMBER]           INT             CONSTRAINT [DF_Trans_LINE_NUMBER] DEFAULT ((0)) NOT NULL,
    [BATCH_NUM]             VARCHAR (15)    CONSTRAINT [DF_Trans_BATCH_NUM] DEFAULT ('') NOT NULL,
    [OWNER_ORG_CODE]        VARCHAR (10)    CONSTRAINT [DF_Trans_OWNER_ORG_CODE] DEFAULT ('') NOT NULL,
    [SOURCE_SYSTEM]         VARCHAR (10)    CONSTRAINT [DF_Trans_SOURCE_SYSTEM] DEFAULT ('') NOT NULL,
    [JOURNAL_TYPE]          VARCHAR (5)     CONSTRAINT [DF_Trans_JOURNAL_TYPE] DEFAULT ('') NOT NULL,
    [TRANSACTION_TYPE]      VARCHAR (5)     CONSTRAINT [DF_Trans_TRANSACTION_TYPE] DEFAULT ('') NOT NULL,
    [TRANSACTION_DATE]      DATETIME        NOT NULL,
    [BT_ID]                 VARCHAR (10)    CONSTRAINT [DF_Trans_BT_ID] DEFAULT ('') NOT NULL,
    [ST_ID]                 VARCHAR (10)    CONSTRAINT [DF_Trans_ST_ID] DEFAULT ('') NOT NULL,
    [INVOICE_REFERENCE_NUM] INT             CONSTRAINT [DF_Trans_INVOICE_REFERENCE_NUM] DEFAULT ((0)) NOT NULL,
    [DESCRIPTION]           VARCHAR (255)   CONSTRAINT [DF_Trans_DESCRIPTION] DEFAULT ('') NOT NULL,
    [CUSTOMER_NAME]         VARCHAR (60)    CONSTRAINT [DF_Trans_CUSTOMER_NAME] DEFAULT ('') NOT NULL,
    [CUSTOMER_REFERENCE]    VARCHAR (40)    CONSTRAINT [DF_Trans_CUSTOMER_REFERENCE] DEFAULT ('') NOT NULL,
    [REFERENCE_1]           VARCHAR (50)    CONSTRAINT [DF_Trans_REFERENCE_1] DEFAULT ('') NOT NULL,
    [SOURCE_CODE]           VARCHAR (40)    CONSTRAINT [DF_Trans_SOURCE_CODE] DEFAULT ('') NOT NULL,
    [PRODUCT_CODE]          VARCHAR (31)    CONSTRAINT [DF_Trans_PRODUCT_CODE] DEFAULT ('') NOT NULL,
    [EFFECTIVE_DATE]        DATETIME        NULL,
    [PAID_THRU]             DATETIME        NULL,
    [MONTHS_PAID]           INT             CONSTRAINT [DF_Trans_MONTHS_PAID] DEFAULT ((0)) NOT NULL,
    [FISCAL_PERIOD]         INT             CONSTRAINT [DF_Trans_FISCAL_PERIOD] DEFAULT ((0)) NOT NULL,
    [DEFERRAL_MONTHS]       INT             CONSTRAINT [DF_Trans_DEFERRAL_MONTHS] DEFAULT ((0)) NOT NULL,
    [AMOUNT]                MONEY           CONSTRAINT [DF_Trans_AMOUNT] DEFAULT ((0)) NOT NULL,
    [ADJUSTMENT_AMOUNT]     MONEY           CONSTRAINT [DF_Trans_ADJUSTMENT_AMOUNT] DEFAULT ((0)) NOT NULL,
    [PSEUDO_ACCOUNT]        VARCHAR (50)    CONSTRAINT [DF_Trans_PSEUDO_ACCOUNT] DEFAULT ('') NOT NULL,
    [GL_ACCT_ORG_CODE]      VARCHAR (5)     CONSTRAINT [DF_Trans_GL_ACCT_ORG_CODE] DEFAULT ('') NOT NULL,
    [GL_ACCOUNT]            VARCHAR (50)    CONSTRAINT [DF_Trans_GL_ACCOUNT] DEFAULT ('') NOT NULL,
    [DEFERRED_GL_ACCOUNT]   VARCHAR (50)    CONSTRAINT [DF_Trans_DEFERRED_GL_ACCOUNT] DEFAULT ('') NOT NULL,
    [INVOICE_CHARGES]       MONEY           CONSTRAINT [DF_Trans_INVOICE_CHARGES] DEFAULT ((0)) NOT NULL,
    [INVOICE_CREDITS]       MONEY           CONSTRAINT [DF_Trans_INVOICE_CREDITS] DEFAULT ((0)) NOT NULL,
    [QUANTITY]              NUMERIC (15, 4) CONSTRAINT [DF_Trans_QUANTITY] DEFAULT ((0)) NOT NULL,
    [UNIT_PRICE]            MONEY           CONSTRAINT [DF_Trans_UNIT_PRICE] DEFAULT ((0)) NOT NULL,
    [PAYMENT_TYPE]          VARCHAR (10)    CONSTRAINT [DF_Trans_PAYMENT_TYPE] DEFAULT ('') NOT NULL,
    [CHECK_NUMBER]          VARCHAR (10)    CONSTRAINT [DF_Trans_CHECK_NUMBER] DEFAULT ('') NOT NULL,
    [CC_NUMBER]             VARCHAR (25)    CONSTRAINT [DF_Trans_CC_NUMBER] DEFAULT ('') NOT NULL,
    [CC_EXPIRE]             VARCHAR (10)    CONSTRAINT [DF_Trans_CC_EXPIRE] DEFAULT ('') NOT NULL,
    [CC_AUTHORIZE]          VARCHAR (40)    CONSTRAINT [DF_Trans_CC_AUTHORIZE] DEFAULT ('') NOT NULL,
    [CC_NAME]               VARCHAR (40)    CONSTRAINT [DF_Trans_CC_NAME] DEFAULT ('') NOT NULL,
    [TERMS_CODE]            VARCHAR (5)     CONSTRAINT [DF_Trans_TERMS_CODE] DEFAULT ('') NOT NULL,
    [ACTIVITY_SEQN]         INT             CONSTRAINT [DF_Trans_ACTIVITY_SEQN] DEFAULT ((0)) NOT NULL,
    [POSTED]                TINYINT         CONSTRAINT [DF_Trans_POSTED] DEFAULT ((0)) NOT NULL,
    [PROD_TYPE]             VARCHAR (5)     CONSTRAINT [DF_Trans_PROD_TYPE] DEFAULT ('') NOT NULL,
    [ACTIVITY_TYPE]         VARCHAR (10)    CONSTRAINT [DF_Trans_ACTIVITY_TYPE] DEFAULT ('') NOT NULL,
    [ACTION_CODES]          VARCHAR (255)   CONSTRAINT [DF_Trans_ACTION_CODES] DEFAULT ('') NOT NULL,
    [TICKLER_DATE]          DATETIME        NULL,
    [DATE_ENTERED]          DATETIME        NULL,
    [ENTERED_BY]            VARCHAR (60)    CONSTRAINT [DF_Trans_ENTERED_BY] DEFAULT ('') NOT NULL,
    [SUB_LINE_NUMBER]       INT             CONSTRAINT [DF_Trans_SUB_LINE_NUMBER] DEFAULT ((0)) NOT NULL,
    [INSTALL_BILL_DATE]     DATETIME        NULL,
    [TAXABLE_VALUE]         MONEY           CONSTRAINT [DF_Trans_TAXABLE_VALUE] DEFAULT ((0)) NOT NULL,
    [SOLICITOR_ID]          VARCHAR (10)    CONSTRAINT [DF_Trans_SOLICITOR_ID] DEFAULT ('') NOT NULL,
    [INVOICE_ADJUSTMENTS]   MONEY           CONSTRAINT [DF_Trans_INVOICE_ADJUSTMENTS] DEFAULT ((0)) NOT NULL,
    [INVOICE_LINE_NUM]      INT             CONSTRAINT [DF_Trans_INVOICE_LINE_NUM] DEFAULT ((0)) NOT NULL,
    [MERGE_CODE]            VARCHAR (40)    CONSTRAINT [DF_Trans_MERGE_CODE] DEFAULT ('') NOT NULL,
    [SALUTATION_CODE]       VARCHAR (40)    CONSTRAINT [DF_Trans_SALUTATION_CODE] DEFAULT ('') NOT NULL,
    [SENDER_CODE]           VARCHAR (40)    CONSTRAINT [DF_Trans_SENDER_CODE] DEFAULT ('') NOT NULL,
    [IS_MATCH_GIFT]         TINYINT         CONSTRAINT [DF_Trans_IS_MATCH_GIFT] DEFAULT ((0)) NOT NULL,
    [MATCH_GIFT_TRANS_NUM]  INT             CONSTRAINT [DF_Trans_MATCH_GIFT_TRANS_NUM] DEFAULT ((0)) NOT NULL,
    [MATCH_ACTIVITY_SEQN]   INT             CONSTRAINT [DF_Trans_MATCH_ACTIVITY_SEQN] DEFAULT ((0)) NOT NULL,
    [MEM_TRIB_ID]           VARCHAR (10)    CONSTRAINT [DF_Trans_MEM_TRIB_ID] DEFAULT ('') NOT NULL,
    [RECEIPT_ID]            INT             CONSTRAINT [DF_Trans_RECEIPT_ID] DEFAULT ((0)) NOT NULL,
    [DO_NOT_RECEIPT]        TINYINT         CONSTRAINT [DF_Trans_DO_NOT_RECEIPT] DEFAULT ((0)) NOT NULL,
    [CC_STATUS]             VARCHAR (1)     CONSTRAINT [DF_Trans_CC_STATUS] DEFAULT ('') NOT NULL,
    [ENCRYPT_CC_NUMBER]     VARCHAR (150)   CONSTRAINT [DF_Trans_ENCRYPT_CC_NUMBER] DEFAULT ('') NOT NULL,
    [ENCRYPT_CC_EXPIRE]     VARCHAR (150)   CONSTRAINT [DF_Trans_ENCRYPT_CC_EXPIRE] DEFAULT ('') NOT NULL,
    [FR_ACTIVITY]           VARCHAR (1)     CONSTRAINT [DF_Trans_FR_ACTIVITY] DEFAULT ('') NOT NULL,
    [FR_ACTIVITY_SEQN]      INT             CONSTRAINT [DF_Trans_FR_ACTIVITY_SEQN] DEFAULT ((0)) NOT NULL,
    [MEM_TRIB_NAME_TEXT]    VARCHAR (100)   CONSTRAINT [DF_Trans_MEM_TRIB_NAME_TEXT] DEFAULT ('') NOT NULL,
    [CAMPAIGN_CODE]         VARCHAR (10)    CONSTRAINT [DF_Trans_CAMPAIGN_CODE] DEFAULT ('') NOT NULL,
    [IS_FR_ITEM]            BIT             CONSTRAINT [DF_Trans_IS_FR_ITEM] DEFAULT ((0)) NOT NULL,
    [ENCRYPT_CSC]           VARCHAR (150)   CONSTRAINT [DF_Trans_ENCRYPT_CSC] DEFAULT ('') NOT NULL,
    [ISSUE_DATE]            VARCHAR (10)    CONSTRAINT [DF_Trans_ISSUE_DATE] DEFAULT ('') NOT NULL,
    [ISSUE_NUMBER]          VARCHAR (2)     CONSTRAINT [DF_Trans_ISSUE_NUMBER] DEFAULT ('') NOT NULL,
    [GL_EXPORT_DATE]        DATETIME        NULL,
    [FR_CHECKBOX]           BIT             CONSTRAINT [DF_Trans_FR_CHECKBOX] DEFAULT ((0)) NOT NULL,
    [GATEWAY_REF]           VARCHAR (100)   CONSTRAINT [DF_Trans_GATEWAY_REF] DEFAULT ('') NOT NULL,
    [TAX_AUTHORITY]         VARCHAR (15)    CONSTRAINT [DF_Trans_TAX_AUTHORITY] DEFAULT ('') NOT NULL,
    [TAX_RATE]              NUMERIC (15, 4) CONSTRAINT [DF_Trans_TAX_RATE] DEFAULT ((0)) NOT NULL,
    [TAX_1]                 NUMERIC (15, 4) CONSTRAINT [DF_Trans_TAX_1] DEFAULT ((0)) NOT NULL,
    [PRICE_ADJ]             BIT             CONSTRAINT [DF_Trans_PRICE_ADJ] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]            ROWVERSION      NULL,
    CONSTRAINT [PK_Trans] PRIMARY KEY CLUSTERED ([TRANS_NUMBER] ASC, [LINE_NUMBER] ASC, [SUB_LINE_NUMBER] ASC)
);

