﻿CREATE TABLE [stg].[imis_Subscriptions] (
    [ID]                    VARCHAR (10)    CONSTRAINT [DF_Subscriptions_ID] DEFAULT ('') NOT NULL,
    [PRODUCT_CODE]          VARCHAR (31)    CONSTRAINT [DF_Subscriptions_PRODUCT_CODE] DEFAULT ('') NOT NULL,
    [BT_ID]                 VARCHAR (10)    CONSTRAINT [DF_Subscriptions_BT_ID] DEFAULT ('') NOT NULL,
    [PROD_TYPE]             VARCHAR (10)    CONSTRAINT [DF_Subscriptions_PROD_TYPE] DEFAULT ('') NOT NULL,
    [STATUS]                VARCHAR (5)     CONSTRAINT [DF_Subscriptions_STATUS] DEFAULT ('') NOT NULL,
    [BEGIN_DATE]            DATETIME        NULL,
    [PAID_THRU]             DATETIME        NULL,
    [COPIES]                INT             CONSTRAINT [DF_Subscriptions_COPIES] DEFAULT ((0)) NOT NULL,
    [SOURCE_CODE]           VARCHAR (40)    CONSTRAINT [DF_Subscriptions_SOURCE_CODE] DEFAULT ('') NOT NULL,
    [FIRST_SUBSCRIBED]      DATETIME        NULL,
    [CONTINUOUS_SINCE]      DATETIME        NULL,
    [PRIOR_YEARS]           INT             CONSTRAINT [DF_Subscriptions_PRIOR_YEARS] DEFAULT ((0)) NOT NULL,
    [FUTURE_COPIES]         INT             CONSTRAINT [DF_Subscriptions_FUTURE_COPIES] DEFAULT ((0)) NOT NULL,
    [FUTURE_COPIES_DATE]    DATETIME        NULL,
    [PREF_MAIL]             INT             CONSTRAINT [DF_Subscriptions_PREF_MAIL] DEFAULT ((0)) NOT NULL,
    [PREF_BILL]             INT             CONSTRAINT [DF_Subscriptions_PREF_BILL] DEFAULT ((0)) NOT NULL,
    [RENEW_MONTHS]          TINYINT         CONSTRAINT [DF_Subscriptions_RENEW_MONTHS] DEFAULT ((0)) NOT NULL,
    [MAIL_CODE]             VARCHAR (10)    CONSTRAINT [DF_Subscriptions_MAIL_CODE] DEFAULT ('') NOT NULL,
    [PREVIOUS_BALANCE]      MONEY           CONSTRAINT [DF_Subscriptions_PREVIOUS_BALANCE] DEFAULT ((0)) NOT NULL,
    [BILL_DATE]             DATETIME        NULL,
    [REMINDER_DATE]         DATETIME        NULL,
    [REMINDER_COUNT]        TINYINT         CONSTRAINT [DF_Subscriptions_REMINDER_COUNT] DEFAULT ((0)) NOT NULL,
    [BILL_BEGIN]            DATETIME        NULL,
    [BILL_THRU]             DATETIME        NULL,
    [BILL_AMOUNT]           MONEY           CONSTRAINT [DF_Subscriptions_BILL_AMOUNT] DEFAULT ((0)) NOT NULL,
    [BILL_COPIES]           INT             CONSTRAINT [DF_Subscriptions_BILL_COPIES] DEFAULT ((0)) NOT NULL,
    [PAYMENT_AMOUNT]        MONEY           CONSTRAINT [DF_Subscriptions_PAYMENT_AMOUNT] DEFAULT ((0)) NOT NULL,
    [PAYMENT_DATE]          DATETIME        NULL,
    [PAID_BEGIN]            DATETIME        NULL,
    [LAST_PAID_THRU]        DATETIME        NULL,
    [COPIES_PAID]           INT             CONSTRAINT [DF_Subscriptions_COPIES_PAID] DEFAULT ((0)) NOT NULL,
    [ADJUSTMENT_AMOUNT]     MONEY           CONSTRAINT [DF_Subscriptions_ADJUSTMENT_AMOUNT] DEFAULT ((0)) NOT NULL,
    [LTD_PAYMENTS]          MONEY           CONSTRAINT [DF_Subscriptions_LTD_PAYMENTS] DEFAULT ((0)) NOT NULL,
    [ISSUES_PRINTED]        VARCHAR (255)   CONSTRAINT [DF_Subscriptions_ISSUES_PRINTED] DEFAULT ('') NOT NULL,
    [BALANCE]               MONEY           CONSTRAINT [DF_Subscriptions_BALANCE] DEFAULT ((0)) NOT NULL,
    [CANCEL_REASON]         VARCHAR (10)    CONSTRAINT [DF_Subscriptions_CANCEL_REASON] DEFAULT ('') NOT NULL,
    [YEARS_ACTIVE_STRING]   VARCHAR (100)   CONSTRAINT [DF_Subscriptions_YEARS_ACTIVE_STRING] DEFAULT ('') NOT NULL,
    [LAST_ISSUE]            VARCHAR (15)    CONSTRAINT [DF_Subscriptions_LAST_ISSUE] DEFAULT ('') NOT NULL,
    [LAST_ISSUE_DATE]       DATETIME        NULL,
    [DATE_ADDED]            DATETIME        NULL,
    [LAST_UPDATED]          DATETIME        NULL,
    [UPDATED_BY]            VARCHAR (60)    CONSTRAINT [DF_Subscriptions_UPDATED_BY] DEFAULT ('') NOT NULL,
    [INTENT_TO_EDIT]        VARCHAR (80)    CONSTRAINT [DF_Subscriptions_INTENT_TO_EDIT] DEFAULT ('') NOT NULL,
    [FLAG]                  VARCHAR (5)     CONSTRAINT [DF_Subscriptions_FLAG] DEFAULT ('') NOT NULL,
    [BILL_TYPE]             VARCHAR (1)     CONSTRAINT [DF_Subscriptions_BILL_TYPE] DEFAULT ('') NOT NULL,
    [COMPLIMENTARY]         BIT             CONSTRAINT [DF_Subscriptions_COMPLIMENTARY] DEFAULT ((0)) NOT NULL,
    [FUTURE_CREDITS]        MONEY           CONSTRAINT [DF_Subscriptions_FUTURE_CREDITS] DEFAULT ((0)) NOT NULL,
    [INVOICE_REFERENCE_NUM] INT             CONSTRAINT [DF_Subscriptions_INVOICE_REFERENCE_NUM] DEFAULT ((0)) NOT NULL,
    [INVOICE_LINE_NUM]      INT             CONSTRAINT [DF_Subscriptions_INVOICE_LINE_NUM] DEFAULT ((0)) NOT NULL,
    [CAMPAIGN_CODE]         VARCHAR (10)    CONSTRAINT [DF_Subscriptions_CAMPAIGN_CODE] DEFAULT ('') NOT NULL,
    [APPEAL_CODE]           VARCHAR (40)    CONSTRAINT [DF_Subscriptions_APPEAL_CODE] DEFAULT ('') NOT NULL,
    [ORG_CODE]              VARCHAR (5)     CONSTRAINT [DF_Subscriptions_ORG_CODE] DEFAULT ('') NOT NULL,
    [IS_FR_ITEM]            BIT             CONSTRAINT [DF_Subscriptions_IS_FR_ITEM] DEFAULT ((0)) NOT NULL,
    [FAIR_MARKET_VALUE]     NUMERIC (15, 2) CONSTRAINT [DF_Subscriptions_FAIR_MARKET_VALUE] DEFAULT ((0)) NOT NULL,
    [IS_GROUP_ADMIN]        BIT             CONSTRAINT [DF_Subscriptions_IS_GROUP_ADMIN] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]            BIGINT          NULL,
    [Id_Identity]           INT             IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]         VARCHAR (40)    CONSTRAINT [df_SubscriptionsLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]          DATETIME        CONSTRAINT [df_SubscriptionsLastModified] DEFAULT (getdate()) NULL,
    [IsActive]              BIT             NULL,
    [StartDate]             DATETIME        CONSTRAINT [df_SubscriptionsStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]               DATETIME        CONSTRAINT [df_SubscriptionsEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Subscriptions] PRIMARY KEY CLUSTERED ([Id_Identity] ASC)
);

