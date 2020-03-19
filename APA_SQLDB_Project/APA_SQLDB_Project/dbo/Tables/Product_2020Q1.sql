﻿CREATE TABLE [dbo].[Product_2020Q1] (
    [PRODUCT_CODE]            VARCHAR (31)     CONSTRAINT [DF_Product_PRODUCT_CODE] DEFAULT ('') NOT NULL,
    [PRODUCT_MAJOR]           VARCHAR (15)     CONSTRAINT [DF_Product_PRODUCT_MAJOR] DEFAULT ('') NOT NULL,
    [PRODUCT_MINOR]           VARCHAR (15)     CONSTRAINT [DF_Product_PRODUCT_MINOR] DEFAULT ('') NOT NULL,
    [PROD_TYPE]               VARCHAR (10)     CONSTRAINT [DF_Product_PROD_TYPE] DEFAULT ('') NOT NULL,
    [CATEGORY]                VARCHAR (10)     CONSTRAINT [DF_Product_CATEGORY] DEFAULT ('') NOT NULL,
    [TITLE_KEY]               VARCHAR (60)     CONSTRAINT [DF_Product_TITLE_KEY] DEFAULT ('') NOT NULL,
    [TITLE]                   VARCHAR (60)     CONSTRAINT [DF_Product_TITLE] DEFAULT ('') NOT NULL,
    [DESCRIPTION]             TEXT             CONSTRAINT [DF_Product_DESCRIPTION] DEFAULT ('') NOT NULL,
    [STATUS]                  VARCHAR (1)      CONSTRAINT [DF_Product_STATUS] DEFAULT ('') NOT NULL,
    [NOTE]                    TEXT             NULL,
    [GROUP_1]                 VARCHAR (31)     CONSTRAINT [DF_Product_GROUP_1] DEFAULT ('') NOT NULL,
    [GROUP_2]                 VARCHAR (31)     CONSTRAINT [DF_Product_GROUP_2] DEFAULT ('') NOT NULL,
    [GROUP_3]                 VARCHAR (31)     CONSTRAINT [DF_Product_GROUP_3] DEFAULT ('') NOT NULL,
    [PRICE_RULES_EXIST]       BIT              CONSTRAINT [DF_Product_PRICE_RULES_EXIST] DEFAULT ((0)) NOT NULL,
    [LOT_SERIAL_EXIST]        BIT              CONSTRAINT [DF_Product_LOT_SERIAL_EXIST] DEFAULT ((0)) NOT NULL,
    [PAYMENT_PRIORITY]        INT              CONSTRAINT [DF_Product_PAYMENT_PRIORITY] DEFAULT ((0)) NOT NULL,
    [RENEW_MONTHS]            INT              CONSTRAINT [DF_Product_RENEW_MONTHS] DEFAULT ((0)) NOT NULL,
    [PRORATE]                 VARCHAR (50)     CONSTRAINT [DF_Product_PRORATE] DEFAULT ('') NOT NULL,
    [STOCK_ITEM]              BIT              CONSTRAINT [DF_Product_STOCK_ITEM] DEFAULT ((0)) NOT NULL,
    [UNIT_OF_MEASURE]         VARCHAR (10)     CONSTRAINT [DF_Product_UNIT_OF_MEASURE] DEFAULT ('') NOT NULL,
    [WEIGHT]                  NUMERIC (15, 2)  CONSTRAINT [DF_Product_WEIGHT] DEFAULT ((0)) NOT NULL,
    [TAXABLE]                 BIT              CONSTRAINT [DF_Product_TAXABLE] DEFAULT ((0)) NOT NULL,
    [COMMISIONABLE]           BIT              CONSTRAINT [DF_Product_COMMISIONABLE] DEFAULT ((0)) NOT NULL,
    [COMMISION_PERCENT]       NUMERIC (15, 2)  CONSTRAINT [DF_Product_COMMISION_PERCENT] DEFAULT ((0)) NOT NULL,
    [DECIMAL_POINTS]          INT              CONSTRAINT [DF_Product_DECIMAL_POINTS] DEFAULT ((0)) NOT NULL,
    [INCOME_ACCOUNT]          VARCHAR (50)     CONSTRAINT [DF_Product_INCOME_ACCOUNT] DEFAULT ('') NOT NULL,
    [DEFERRED_INCOME_ACCOUNT] VARCHAR (50)     CONSTRAINT [DF_Product_DEFERRED_INCOME_ACCOUNT] DEFAULT ('') NOT NULL,
    [INVENTORY_ACCOUNT]       VARCHAR (50)     CONSTRAINT [DF_Product_INVENTORY_ACCOUNT] DEFAULT ('') NOT NULL,
    [ADJUSTMENT_ACCOUNT]      VARCHAR (50)     CONSTRAINT [DF_Product_ADJUSTMENT_ACCOUNT] DEFAULT ('') NOT NULL,
    [COG_ACCOUNT]             VARCHAR (50)     CONSTRAINT [DF_Product_COG_ACCOUNT] DEFAULT ('') NOT NULL,
    [INTENT_TO_EDIT]          VARCHAR (80)     CONSTRAINT [DF_Product_INTENT_TO_EDIT] DEFAULT ('') NOT NULL,
    [PRICE_1]                 MONEY            CONSTRAINT [DF_Product_PRICE_1] DEFAULT ((0)) NOT NULL,
    [PRICE_2]                 MONEY            CONSTRAINT [DF_Product_PRICE_2] DEFAULT ((0)) NOT NULL,
    [PRICE_3]                 MONEY            CONSTRAINT [DF_Product_PRICE_3] DEFAULT ((0)) NOT NULL,
    [COMPLIMENTARY]           BIT              CONSTRAINT [DF_Product_COMPLIMENTARY] DEFAULT ((0)) NOT NULL,
    [ATTRIBUTES]              VARCHAR (255)    CONSTRAINT [DF_Product_ATTRIBUTES] DEFAULT ('') NOT NULL,
    [PST_TAXABLE]             BIT              CONSTRAINT [DF_Product_PST_TAXABLE] DEFAULT ((0)) NOT NULL,
    [TAXABLE_VALUE]           MONEY            CONSTRAINT [DF_Product_TAXABLE_VALUE] DEFAULT ((0)) NOT NULL,
    [ORG_CODE]                VARCHAR (5)      CONSTRAINT [DF_Product_ORG_CODE] DEFAULT ('') NOT NULL,
    [TAX_AUTHORITY]           VARCHAR (15)     CONSTRAINT [DF_Product_TAX_AUTHORITY] DEFAULT ('') NOT NULL,
    [WEB_OPTION]              TINYINT          CONSTRAINT [DF_Product_WEB_OPTION] DEFAULT ((0)) NOT NULL,
    [IMAGE_URL]               VARCHAR (100)    CONSTRAINT [DF_Product_IMAGE_URL] DEFAULT ('') NOT NULL,
    [APPLY_IMAGE]             BIT              CONSTRAINT [DF_Product_APPLY_IMAGE] DEFAULT ((0)) NOT NULL,
    [IS_KIT]                  BIT              CONSTRAINT [DF_Product_IS_KIT] DEFAULT ((0)) NOT NULL,
    [INFO_URL]                VARCHAR (100)    CONSTRAINT [DF_Product_INFO_URL] DEFAULT ('') NOT NULL,
    [APPLY_INFO]              BIT              CONSTRAINT [DF_Product_APPLY_INFO] DEFAULT ((0)) NOT NULL,
    [PLP_CODE]                VARCHAR (6)      CONSTRAINT [DF_Product_PLP_CODE] DEFAULT ('') NOT NULL,
    [PROMOTE]                 BIT              CONSTRAINT [DF_Product_PROMOTE] DEFAULT ((0)) NOT NULL,
    [THUMBNAIL_URL]           VARCHAR (100)    CONSTRAINT [DF_Product_THUMBNAIL_URL] DEFAULT ('') NOT NULL,
    [APPLY_THUMBNAIL]         BIT              CONSTRAINT [DF_Product_APPLY_THUMBNAIL] DEFAULT ((0)) NOT NULL,
    [CATALOG_DESC]            TEXT             NULL,
    [WEB_DESC]                TEXT             NULL,
    [OTHER_DESC]              TEXT             NULL,
    [LOCATION]                VARCHAR (10)     CONSTRAINT [DF_Product_LOCATION] DEFAULT ('') NOT NULL,
    [PREMIUM]                 BIT              CONSTRAINT [DF_Product_PREMIUM] DEFAULT ((0)) NOT NULL,
    [FAIR_MARKET_VALUE]       NUMERIC (15, 2)  CONSTRAINT [DF_Product_FAIR_MARKET_VALUE] DEFAULT ((0)) NOT NULL,
    [IS_FR_ITEM]              BIT              CONSTRAINT [DF_Product_IS_FR_ITEM] DEFAULT ((0)) NOT NULL,
    [APPEAL_CODE]             VARCHAR (40)     CONSTRAINT [DF_Product_APPEAL_CODE] DEFAULT ('') NOT NULL,
    [CAMPAIGN_CODE]           VARCHAR (10)     CONSTRAINT [DF_Product_CAMPAIGN_CODE] DEFAULT ('') NOT NULL,
    [PRICE_FROM_COMPONENTS]   BIT              CONSTRAINT [DF_Product_PRICE_FROM_COMPONENTS] DEFAULT ((0)) NOT NULL,
    [PUBLISH_START_DATE]      DATETIME         NULL,
    [PUBLISH_END_DATE]        DATETIME         NULL,
    [TAX_BY_LOCATION]         BIT              CONSTRAINT [DF_Product_TAX_BY_LOCATION] DEFAULT ((0)) NOT NULL,
    [TAXCATEGORY_CODE]        VARCHAR (10)     CONSTRAINT [DF_Product_TAXCATEGORY_CODE] DEFAULT ('') NOT NULL,
    [RELATED_CONTENT_MESSAGE] VARCHAR (MAX)    NULL,
    [MINIMUM_GIFT_AMOUNT]     MONEY            CONSTRAINT [DF_Product_MINIMUM_GIFT_AMOUNT] DEFAULT ((0)) NOT NULL,
    [ProductKey]              UNIQUEIDENTIFIER CONSTRAINT [DF_Product_ProductKey] DEFAULT (newid()) NOT NULL,
    [AllowOrderLineNote]      BIT              CONSTRAINT [DF_Product_AllowOrderLineNote] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]              ROWVERSION       NULL,
    CONSTRAINT [PK_Product] PRIMARY KEY CLUSTERED ([PRODUCT_CODE] ASC),
    CONSTRAINT [AK_Product_ProductKey] UNIQUE NONCLUSTERED ([ProductKey] ASC)
);

