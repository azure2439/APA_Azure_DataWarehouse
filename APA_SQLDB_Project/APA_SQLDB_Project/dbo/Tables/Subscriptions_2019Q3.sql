﻿CREATE TABLE [dbo].[Subscriptions_2019Q3] (
    [ID]                    VARCHAR (10)    NOT NULL,
    [PRODUCT_CODE]          VARCHAR (31)    NOT NULL,
    [BT_ID]                 VARCHAR (10)    NOT NULL,
    [PROD_TYPE]             VARCHAR (10)    NOT NULL,
    [STATUS]                VARCHAR (5)     NOT NULL,
    [BEGIN_DATE]            DATETIME        NULL,
    [PAID_THRU]             DATETIME        NULL,
    [COPIES]                INT             NOT NULL,
    [SOURCE_CODE]           VARCHAR (40)    NOT NULL,
    [FIRST_SUBSCRIBED]      DATETIME        NULL,
    [CONTINUOUS_SINCE]      DATETIME        NULL,
    [PRIOR_YEARS]           INT             NOT NULL,
    [FUTURE_COPIES]         INT             NOT NULL,
    [FUTURE_COPIES_DATE]    DATETIME        NULL,
    [PREF_MAIL]             INT             NOT NULL,
    [PREF_BILL]             INT             NOT NULL,
    [RENEW_MONTHS]          TINYINT         NOT NULL,
    [MAIL_CODE]             VARCHAR (10)    NOT NULL,
    [PREVIOUS_BALANCE]      MONEY           NOT NULL,
    [BILL_DATE]             DATETIME        NULL,
    [REMINDER_DATE]         DATETIME        NULL,
    [REMINDER_COUNT]        TINYINT         NOT NULL,
    [BILL_BEGIN]            DATETIME        NULL,
    [BILL_THRU]             DATETIME        NULL,
    [BILL_AMOUNT]           MONEY           NOT NULL,
    [BILL_COPIES]           INT             NOT NULL,
    [PAYMENT_AMOUNT]        MONEY           NOT NULL,
    [PAYMENT_DATE]          DATETIME        NULL,
    [PAID_BEGIN]            DATETIME        NULL,
    [LAST_PAID_THRU]        DATETIME        NULL,
    [COPIES_PAID]           INT             NOT NULL,
    [ADJUSTMENT_AMOUNT]     MONEY           NOT NULL,
    [LTD_PAYMENTS]          MONEY           NOT NULL,
    [ISSUES_PRINTED]        VARCHAR (255)   NOT NULL,
    [BALANCE]               MONEY           NOT NULL,
    [CANCEL_REASON]         VARCHAR (10)    NOT NULL,
    [YEARS_ACTIVE_STRING]   VARCHAR (100)   NOT NULL,
    [LAST_ISSUE]            VARCHAR (15)    NOT NULL,
    [LAST_ISSUE_DATE]       DATETIME        NULL,
    [DATE_ADDED]            DATETIME        NULL,
    [LAST_UPDATED]          DATETIME        NULL,
    [UPDATED_BY]            VARCHAR (60)    NOT NULL,
    [INTENT_TO_EDIT]        VARCHAR (80)    NOT NULL,
    [FLAG]                  VARCHAR (5)     NOT NULL,
    [BILL_TYPE]             VARCHAR (1)     NOT NULL,
    [COMPLIMENTARY]         BIT             NOT NULL,
    [FUTURE_CREDITS]        MONEY           NOT NULL,
    [INVOICE_REFERENCE_NUM] INT             NOT NULL,
    [INVOICE_LINE_NUM]      INT             NOT NULL,
    [CAMPAIGN_CODE]         VARCHAR (10)    NOT NULL,
    [APPEAL_CODE]           VARCHAR (40)    NOT NULL,
    [ORG_CODE]              VARCHAR (5)     NOT NULL,
    [IS_FR_ITEM]            BIT             NOT NULL,
    [FAIR_MARKET_VALUE]     NUMERIC (15, 2) NOT NULL,
    [IS_GROUP_ADMIN]        BIT             NOT NULL,
    [TIME_STAMP]            ROWVERSION      NULL,
    CONSTRAINT [PK_Subscriptions_2019Q3] PRIMARY KEY CLUSTERED ([ID] ASC, [PRODUCT_CODE] ASC)
);

