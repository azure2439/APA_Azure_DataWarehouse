﻿CREATE TABLE [stg].[imis_Orders] (
    [ORDER_NUMBER]                  NUMERIC (15, 2) CONSTRAINT [DF_Orders_ORDER_NUMBER] DEFAULT ((0)) NOT NULL,
    [ORG_CODE]                      VARCHAR (5)     CONSTRAINT [DF_Orders_ORG_CODE] DEFAULT ('') NOT NULL,
    [ORDER_TYPE_CODE]               VARCHAR (10)    CONSTRAINT [DF_Orders_ORDER_TYPE_CODE] DEFAULT ('') NOT NULL,
    [STAGE]                         VARCHAR (10)    CONSTRAINT [DF_Orders_STAGE] DEFAULT ('') NOT NULL,
    [SOURCE_SYSTEM]                 VARCHAR (10)    CONSTRAINT [DF_Orders_SOURCE_SYSTEM] DEFAULT ('') NOT NULL,
    [BATCH_NUM]                     VARCHAR (15)    CONSTRAINT [DF_Orders_BATCH_NUM] DEFAULT ('') NOT NULL,
    [STATUS]                        VARCHAR (10)    CONSTRAINT [DF_Orders_STATUS] DEFAULT ('') NOT NULL,
    [HOLD_CODE]                     VARCHAR (10)    CONSTRAINT [DF_Orders_HOLD_CODE] DEFAULT ('') NOT NULL,
    [ORDER_DATE]                    DATETIME        NULL,
    [BT_ID]                         VARCHAR (10)    CONSTRAINT [DF_Orders_BT_ID] DEFAULT ('') NOT NULL,
    [ST_ID]                         VARCHAR (10)    CONSTRAINT [DF_Orders_ST_ID] DEFAULT ('') NOT NULL,
    [ST_ADDRESS_NUM]                INT             CONSTRAINT [DF_Orders_ST_ADDRESS_NUM] DEFAULT ((0)) NOT NULL,
    [ENTERED_DATE_TIME]             DATETIME        NULL,
    [ENTERED_BY]                    VARCHAR (60)    CONSTRAINT [DF_Orders_ENTERED_BY] DEFAULT ('') NOT NULL,
    [UPDATED_DATE_TIME]             DATETIME        NULL,
    [UPDATED_BY]                    VARCHAR (60)    CONSTRAINT [DF_Orders_UPDATED_BY] DEFAULT ('') NOT NULL,
    [INVOICE_REFERENCE_NUM]         INT             CONSTRAINT [DF_Orders_INVOICE_REFERENCE_NUM] DEFAULT ((0)) NOT NULL,
    [INVOICE_NUMBER]                INT             CONSTRAINT [DF_Orders_INVOICE_NUMBER] DEFAULT ((0)) NOT NULL,
    [INVOICE_DATE]                  DATETIME        NULL,
    [NUMBER_LINES]                  INT             CONSTRAINT [DF_Orders_NUMBER_LINES] DEFAULT ((0)) NOT NULL,
    [FULL_NAME]                     VARCHAR (70)    CONSTRAINT [DF_Orders_FULL_NAME] DEFAULT ('') NOT NULL,
    [TITLE]                         VARCHAR (80)    CONSTRAINT [DF_Orders_TITLE] DEFAULT ('') NOT NULL,
    [COMPANY]                       VARCHAR (80)    CONSTRAINT [DF_Orders_COMPANY] DEFAULT ('') NOT NULL,
    [FULL_ADDRESS]                  VARCHAR (255)   CONSTRAINT [DF_Orders_FULL_ADDRESS] DEFAULT ('') NOT NULL,
    [PREFIX]                        VARCHAR (25)    CONSTRAINT [DF_Orders_PREFIX] DEFAULT ('') NOT NULL,
    [FIRST_NAME]                    VARCHAR (20)    CONSTRAINT [DF_Orders_FIRST_NAME] DEFAULT ('') NOT NULL,
    [MIDDLE_NAME]                   VARCHAR (20)    CONSTRAINT [DF_Orders_MIDDLE_NAME] DEFAULT ('') NOT NULL,
    [LAST_NAME]                     VARCHAR (30)    CONSTRAINT [DF_Orders_LAST_NAME] DEFAULT ('') NOT NULL,
    [SUFFIX]                        VARCHAR (10)    CONSTRAINT [DF_Orders_SUFFIX] DEFAULT ('') NOT NULL,
    [DESIGNATION]                   VARCHAR (30)    CONSTRAINT [DF_Orders_DESIGNATION] DEFAULT ('') NOT NULL,
    [INFORMAL]                      VARCHAR (20)    CONSTRAINT [DF_Orders_INFORMAL] DEFAULT ('') NOT NULL,
    [LAST_FIRST]                    VARCHAR (30)    CONSTRAINT [DF_Orders_LAST_FIRST] DEFAULT ('') NOT NULL,
    [COMPANY_SORT]                  VARCHAR (30)    CONSTRAINT [DF_Orders_COMPANY_SORT] DEFAULT ('') NOT NULL,
    [ADDRESS_1]                     VARCHAR (40)    CONSTRAINT [DF_Orders_ADDRESS_1] DEFAULT ('') NOT NULL,
    [ADDRESS_2]                     VARCHAR (40)    CONSTRAINT [DF_Orders_ADDRESS_2] DEFAULT ('') NOT NULL,
    [CITY]                          VARCHAR (40)    CONSTRAINT [DF_Orders_CITY] DEFAULT ('') NOT NULL,
    [STATE_PROVINCE]                VARCHAR (15)    CONSTRAINT [DF_Orders_STATE_PROVINCE] DEFAULT ('') NOT NULL,
    [ZIP]                           VARCHAR (10)    CONSTRAINT [DF_Orders_ZIP] DEFAULT ('') NOT NULL,
    [COUNTRY]                       VARCHAR (25)    CONSTRAINT [DF_Orders_COUNTRY] DEFAULT ('') NOT NULL,
    [DPB]                           VARCHAR (8)     CONSTRAINT [DF_Orders_DPB] DEFAULT ('') NOT NULL,
    [BAR_CODE]                      VARCHAR (14)    CONSTRAINT [DF_Orders_BAR_CODE] DEFAULT ('') NOT NULL,
    [ADDRESS_FORMAT]                TINYINT         CONSTRAINT [DF_Orders_ADDRESS_FORMAT] DEFAULT ((0)) NOT NULL,
    [PHONE]                         VARCHAR (25)    CONSTRAINT [DF_Orders_PHONE] DEFAULT ('') NOT NULL,
    [FAX]                           VARCHAR (25)    CONSTRAINT [DF_Orders_FAX] DEFAULT ('') NOT NULL,
    [NOTES]                         NVARCHAR (MAX)  NULL,
    [TOTAL_CHARGES]                 MONEY           CONSTRAINT [DF_Orders_TOTAL_CHARGES] DEFAULT ((0)) NOT NULL,
    [TOTAL_PAYMENTS]                MONEY           CONSTRAINT [DF_Orders_TOTAL_PAYMENTS] DEFAULT ((0)) NOT NULL,
    [BALANCE]                       MONEY           CONSTRAINT [DF_Orders_BALANCE] DEFAULT ((0)) NOT NULL,
    [LINE_TOTAL]                    MONEY           CONSTRAINT [DF_Orders_LINE_TOTAL] DEFAULT ((0)) NOT NULL,
    [LINE_TAXABLE]                  MONEY           CONSTRAINT [DF_Orders_LINE_TAXABLE] DEFAULT ((0)) NOT NULL,
    [FREIGHT_1]                     MONEY           CONSTRAINT [DF_Orders_FREIGHT_1] DEFAULT ((0)) NOT NULL,
    [FREIGHT_2]                     MONEY           CONSTRAINT [DF_Orders_FREIGHT_2] DEFAULT ((0)) NOT NULL,
    [HANDLING_1]                    MONEY           CONSTRAINT [DF_Orders_HANDLING_1] DEFAULT ((0)) NOT NULL,
    [HANDLING_2]                    MONEY           CONSTRAINT [DF_Orders_HANDLING_2] DEFAULT ((0)) NOT NULL,
    [CANCELLATION_FEE]              MONEY           CONSTRAINT [DF_Orders_CANCELLATION_FEE] DEFAULT ((0)) NOT NULL,
    [TAX_1]                         MONEY           CONSTRAINT [DF_Orders_TAX_1] DEFAULT ((0)) NOT NULL,
    [TAX_2]                         MONEY           CONSTRAINT [DF_Orders_TAX_2] DEFAULT ((0)) NOT NULL,
    [TAX_3]                         MONEY           CONSTRAINT [DF_Orders_TAX_3] DEFAULT ((0)) NOT NULL,
    [LINE_PAY]                      MONEY           CONSTRAINT [DF_Orders_LINE_PAY] DEFAULT ((0)) NOT NULL,
    [OTHER_PAY]                     MONEY           CONSTRAINT [DF_Orders_OTHER_PAY] DEFAULT ((0)) NOT NULL,
    [AR_PAY]                        MONEY           CONSTRAINT [DF_Orders_AR_PAY] DEFAULT ((0)) NOT NULL,
    [TAX_AUTHOR_1]                  VARCHAR (15)    CONSTRAINT [DF_Orders_TAX_AUTHOR_1] DEFAULT ('') NOT NULL,
    [TAX_AUTHOR_2]                  VARCHAR (15)    CONSTRAINT [DF_Orders_TAX_AUTHOR_2] DEFAULT ('') NOT NULL,
    [TAX_AUTHOR_3]                  VARCHAR (15)    CONSTRAINT [DF_Orders_TAX_AUTHOR_3] DEFAULT ('') NOT NULL,
    [TAX_RATE_1]                    NUMERIC (15, 4) CONSTRAINT [DF_Orders_TAX_RATE_1] DEFAULT ((0)) NOT NULL,
    [TAX_RATE_2]                    NUMERIC (15, 4) CONSTRAINT [DF_Orders_TAX_RATE_2] DEFAULT ((0)) NOT NULL,
    [TAX_RATE_3]                    NUMERIC (15, 4) CONSTRAINT [DF_Orders_TAX_RATE_3] DEFAULT ((0)) NOT NULL,
    [TAX_EXEMPT]                    VARCHAR (15)    CONSTRAINT [DF_Orders_TAX_EXEMPT] DEFAULT ('') NOT NULL,
    [TERMS_CODE]                    VARCHAR (5)     CONSTRAINT [DF_Orders_TERMS_CODE] DEFAULT ('') NOT NULL,
    [SCHEDULED_DATE]                DATETIME        NULL,
    [CONFIRMATION_DATE_TIME]        DATETIME        NULL,
    [SHIP_PAPERS_DATE_TIME]         DATETIME        NULL,
    [SHIPPED_DATE_TIME]             DATETIME        NULL,
    [BO_RELEASED_DATE_TIME]         DATETIME        NULL,
    [SOURCE_CODE]                   VARCHAR (40)    CONSTRAINT [DF_Orders_SOURCE_CODE] DEFAULT ('') NOT NULL,
    [SALESMAN]                      VARCHAR (15)    CONSTRAINT [DF_Orders_SALESMAN] DEFAULT ('') NOT NULL,
    [COMMISSION_RATE]               NUMERIC (15, 2) CONSTRAINT [DF_Orders_COMMISSION_RATE] DEFAULT ((0)) NOT NULL,
    [DISCOUNT_RATE]                 NUMERIC (15, 2) CONSTRAINT [DF_Orders_DISCOUNT_RATE] DEFAULT ((0)) NOT NULL,
    [PRIORITY]                      INT             CONSTRAINT [DF_Orders_PRIORITY] DEFAULT ((0)) NOT NULL,
    [HOLD_COMMENT]                  VARCHAR (255)   CONSTRAINT [DF_Orders_HOLD_COMMENT] DEFAULT ('') NOT NULL,
    [AFFECT_INVENTORY]              BIT             CONSTRAINT [DF_Orders_AFFECT_INVENTORY] DEFAULT ((0)) NOT NULL,
    [HOLD_FLAG]                     BIT             CONSTRAINT [DF_Orders_HOLD_FLAG] DEFAULT ((0)) NOT NULL,
    [CUSTOMER_REFERENCE]            VARCHAR (40)    CONSTRAINT [DF_Orders_CUSTOMER_REFERENCE] DEFAULT ('') NOT NULL,
    [VALUATION_BASIS]               TINYINT         CONSTRAINT [DF_Orders_VALUATION_BASIS] DEFAULT ((0)) NOT NULL,
    [UNDISCOUNTED_TOTAL]            MONEY           CONSTRAINT [DF_Orders_UNDISCOUNTED_TOTAL] DEFAULT ((0)) NOT NULL,
    [AUTO_CALC_HANDLING]            BIT             CONSTRAINT [DF_Orders_AUTO_CALC_HANDLING] DEFAULT ((0)) NOT NULL,
    [AUTO_CALC_RESTOCKING]          BIT             CONSTRAINT [DF_Orders_AUTO_CALC_RESTOCKING] DEFAULT ((0)) NOT NULL,
    [BACKORDERS]                    TINYINT         CONSTRAINT [DF_Orders_BACKORDERS] DEFAULT ((0)) NOT NULL,
    [MEMBER_TYPE]                   VARCHAR (5)     CONSTRAINT [DF_Orders_MEMBER_TYPE] DEFAULT ('') NOT NULL,
    [PAY_TYPE]                      VARCHAR (10)    CONSTRAINT [DF_Orders_PAY_TYPE] DEFAULT ('') NOT NULL,
    [PAY_NUMBER]                    VARCHAR (25)    CONSTRAINT [DF_Orders_PAY_NUMBER] DEFAULT ('') NOT NULL,
    [CREDIT_CARD_EXPIRES]           VARCHAR (10)    CONSTRAINT [DF_Orders_CREDIT_CARD_EXPIRES] DEFAULT ('') NOT NULL,
    [AUTHORIZE]                     VARCHAR (40)    CONSTRAINT [DF_Orders_AUTHORIZE] DEFAULT ('') NOT NULL,
    [CREDIT_CARD_NAME]              VARCHAR (30)    CONSTRAINT [DF_Orders_CREDIT_CARD_NAME] DEFAULT ('') NOT NULL,
    [BO_STATUS]                     TINYINT         CONSTRAINT [DF_Orders_BO_STATUS] DEFAULT ((0)) NOT NULL,
    [BO_RELEASE_DATE]               DATETIME        NULL,
    [TOTAL_QUANTITY_ORDERED]        NUMERIC (15, 4) CONSTRAINT [DF_Orders_TOTAL_QUANTITY_ORDERED] DEFAULT ((0)) NOT NULL,
    [TOTAL_QUANTITY_BACKORDERED]    NUMERIC (15, 4) CONSTRAINT [DF_Orders_TOTAL_QUANTITY_BACKORDERED] DEFAULT ((0)) NOT NULL,
    [SHIP_METHOD]                   VARCHAR (10)    CONSTRAINT [DF_Orders_SHIP_METHOD] DEFAULT ('') NOT NULL,
    [TOTAL_WEIGHT]                  NUMERIC (15, 2) CONSTRAINT [DF_Orders_TOTAL_WEIGHT] DEFAULT ((0)) NOT NULL,
    [CASH_GL_ACCT]                  VARCHAR (30)    CONSTRAINT [DF_Orders_CASH_GL_ACCT] DEFAULT ('') NOT NULL,
    [LINE_PST_TAXABLE]              MONEY           CONSTRAINT [DF_Orders_LINE_PST_TAXABLE] DEFAULT ((0)) NOT NULL,
    [INTENT_TO_EDIT]                VARCHAR (80)    CONSTRAINT [DF_Orders_INTENT_TO_EDIT] DEFAULT ('') NOT NULL,
    [PREPAID_INVOICE_REFERENCE_NUM] INT             CONSTRAINT [DF_Orders_PREPAID_INVOICE_REFERENCE_NUM] DEFAULT ((0)) NOT NULL,
    [AUTO_CALC_FREIGHT]             BIT             CONSTRAINT [DF_Orders_AUTO_CALC_FREIGHT] DEFAULT ((0)) NOT NULL,
    [CO_ID]                         VARCHAR (10)    CONSTRAINT [DF_Orders_CO_ID] DEFAULT ('') NOT NULL,
    [CO_MEMBER_TYPE]                VARCHAR (5)     CONSTRAINT [DF_Orders_CO_MEMBER_TYPE] DEFAULT ('') NOT NULL,
    [EMAIL]                         VARCHAR (100)   CONSTRAINT [DF_Orders_EMAIL] DEFAULT ('') NOT NULL,
    [CRRT]                          VARCHAR (40)    CONSTRAINT [DF_Orders_CRRT] DEFAULT ('') NOT NULL,
    [ADDRESS_STATUS]                VARCHAR (5)     CONSTRAINT [DF_Orders_ADDRESS_STATUS] DEFAULT ('') NOT NULL,
    [RECOGNIZED_CASH_AMOUNT]        MONEY           CONSTRAINT [DF_Orders_RECOGNIZED_CASH_AMOUNT] DEFAULT ((0)) NOT NULL,
    [IS_FR_ORDER]                   TINYINT         CONSTRAINT [DF_Orders_IS_FR_ORDER] DEFAULT ((0)) NOT NULL,
    [VAT_TAX_CODE_FH]               VARCHAR (31)    NULL,
    [ENCRYPT_PAY_NUMBER]            VARCHAR (150)   CONSTRAINT [DF_Orders_ENCRYPT_PAY_NUMBER] DEFAULT ('') NOT NULL,
    [ENCRYPT_CREDIT_CARD_EXPIRES]   VARCHAR (150)   CONSTRAINT [DF_Orders_ENCRYPT_CREDIT_CARD_EXPIRES] DEFAULT ('') NOT NULL,
    [AUTO_FREIGHT_TYPE]             TINYINT         CONSTRAINT [DF_Orders_AUTO_FREIGHT_TYPE] DEFAULT ((0)) NOT NULL,
    [USE_MEMBER_PRICE]              BIT             CONSTRAINT [DF_Orders_USE_MEMBER_PRICE] DEFAULT ((0)) NOT NULL,
    [ST_PRINT_COMPANY]              BIT             CONSTRAINT [DF_Orders_ST_PRINT_COMPANY] DEFAULT ((0)) NOT NULL,
    [ST_PRINT_TITLE]                BIT             CONSTRAINT [DF_Orders_ST_PRINT_TITLE] DEFAULT ((0)) NOT NULL,
    [TOLL_FREE]                     VARCHAR (25)    CONSTRAINT [DF_Orders_TOLL_FREE] DEFAULT ('') NOT NULL,
    [MAIL_CODE]                     VARCHAR (10)    CONSTRAINT [DF_Orders_MAIL_CODE] DEFAULT ('') NOT NULL,
    [ADDRESS_3]                     VARCHAR (40)    CONSTRAINT [DF_Orders_ADDRESS_3] DEFAULT ('') NOT NULL,
    [ENCRYPT_CSC]                   VARCHAR (150)   CONSTRAINT [DF_Orders_ENCRYPT_CSC] DEFAULT ('') NOT NULL,
    [ISSUE_DATE]                    VARCHAR (10)    CONSTRAINT [DF_Orders_ISSUE_DATE] DEFAULT ('') NOT NULL,
    [ISSUE_NUMBER]                  VARCHAR (2)     CONSTRAINT [DF_Orders_ISSUE_NUMBER] DEFAULT ('') NOT NULL,
    [MORE_PAYMENTS]                 NUMERIC (15, 2) CONSTRAINT [DF_Orders_MORE_PAYMENTS] DEFAULT ((0)) NOT NULL,
    [GATEWAY_REF]                   VARCHAR (100)   CONSTRAINT [DF_Orders_GATEWAY_REF] DEFAULT ('') NOT NULL,
    [ORIGINATING_TRANS_NUM]         INT             CONSTRAINT [DF_Orders_ORIGINATING_TRANS_NUM] DEFAULT ((0)) NOT NULL,
    [FREIGHT_TAX]                   MONEY           CONSTRAINT [DF_Orders_FREIGHT_TAX] DEFAULT ((0)) NOT NULL,
    [HANDLING_TAX]                  MONEY           CONSTRAINT [DF_Orders_HANDLING_TAX] DEFAULT ((0)) NOT NULL,
    [TAX_RATE_FH]                   NUMERIC (15, 4) CONSTRAINT [DF_Orders_TAX_RATE_FH] DEFAULT ((0)) NOT NULL,
    [DISCOUNT_CODE]                 VARCHAR (50)    CONSTRAINT [DF_Orders_DISCOUNT_CODE] DEFAULT ('') NOT NULL,
    [TIME_STAMP]                    BIGINT          NULL,
    [Id_Identitycolumn]             INT             IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]                 VARCHAR (40)    CONSTRAINT [df_Orders_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]                  DATETIME        CONSTRAINT [df_Orders_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]                      BIT             NULL,
    [StartDate]                     DATETIME        CONSTRAINT [df_Orders_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                       DATETIME        CONSTRAINT [df_Orders_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([ORDER_NUMBER] ASC)
);
