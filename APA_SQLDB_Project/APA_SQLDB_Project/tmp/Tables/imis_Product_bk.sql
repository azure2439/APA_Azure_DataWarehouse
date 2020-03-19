﻿CREATE TABLE [tmp].[imis_Product_bk] (
    [PRODUCT_CODE]            VARCHAR (31)     NOT NULL,
    [PRODUCT_MAJOR]           VARCHAR (15)     NOT NULL,
    [PRODUCT_MINOR]           VARCHAR (15)     NOT NULL,
    [PROD_TYPE]               VARCHAR (10)     NOT NULL,
    [CATEGORY]                VARCHAR (10)     NOT NULL,
    [TITLE_KEY]               VARCHAR (60)     NOT NULL,
    [TITLE]                   VARCHAR (60)     NOT NULL,
    [DESCRIPTION]             TEXT             NOT NULL,
    [STATUS]                  VARCHAR (1)      NOT NULL,
    [NOTE]                    TEXT             NULL,
    [GROUP_1]                 VARCHAR (31)     NOT NULL,
    [GROUP_2]                 VARCHAR (31)     NOT NULL,
    [GROUP_3]                 VARCHAR (31)     NOT NULL,
    [PRICE_RULES_EXIST]       BIT              NOT NULL,
    [LOT_SERIAL_EXIST]        BIT              NOT NULL,
    [PAYMENT_PRIORITY]        INT              NOT NULL,
    [RENEW_MONTHS]            INT              NOT NULL,
    [PRORATE]                 VARCHAR (50)     NOT NULL,
    [STOCK_ITEM]              BIT              NOT NULL,
    [UNIT_OF_MEASURE]         VARCHAR (10)     NOT NULL,
    [WEIGHT]                  NUMERIC (15, 2)  NOT NULL,
    [TAXABLE]                 BIT              NOT NULL,
    [COMMISIONABLE]           BIT              NOT NULL,
    [COMMISION_PERCENT]       NUMERIC (15, 2)  NOT NULL,
    [DECIMAL_POINTS]          INT              NOT NULL,
    [INCOME_ACCOUNT]          VARCHAR (50)     NOT NULL,
    [DEFERRED_INCOME_ACCOUNT] VARCHAR (50)     NOT NULL,
    [INVENTORY_ACCOUNT]       VARCHAR (50)     NOT NULL,
    [ADJUSTMENT_ACCOUNT]      VARCHAR (50)     NOT NULL,
    [COG_ACCOUNT]             VARCHAR (50)     NOT NULL,
    [INTENT_TO_EDIT]          VARCHAR (80)     NOT NULL,
    [PRICE_1]                 MONEY            NOT NULL,
    [PRICE_2]                 MONEY            NOT NULL,
    [PRICE_3]                 MONEY            NOT NULL,
    [COMPLIMENTARY]           BIT              NOT NULL,
    [ATTRIBUTES]              VARCHAR (255)    NOT NULL,
    [PST_TAXABLE]             BIT              NOT NULL,
    [TAXABLE_VALUE]           MONEY            NOT NULL,
    [ORG_CODE]                VARCHAR (5)      NOT NULL,
    [TAX_AUTHORITY]           VARCHAR (15)     NOT NULL,
    [WEB_OPTION]              TINYINT          NOT NULL,
    [IMAGE_URL]               VARCHAR (100)    NOT NULL,
    [APPLY_IMAGE]             BIT              NOT NULL,
    [IS_KIT]                  BIT              NOT NULL,
    [INFO_URL]                VARCHAR (100)    NOT NULL,
    [APPLY_INFO]              BIT              NOT NULL,
    [PLP_CODE]                VARCHAR (6)      NOT NULL,
    [PROMOTE]                 BIT              NOT NULL,
    [THUMBNAIL_URL]           VARCHAR (100)    NOT NULL,
    [APPLY_THUMBNAIL]         BIT              NOT NULL,
    [CATALOG_DESC]            TEXT             NULL,
    [WEB_DESC]                TEXT             NULL,
    [OTHER_DESC]              TEXT             NULL,
    [LOCATION]                VARCHAR (10)     NOT NULL,
    [PREMIUM]                 BIT              NOT NULL,
    [FAIR_MARKET_VALUE]       NUMERIC (15, 2)  NOT NULL,
    [IS_FR_ITEM]              BIT              NOT NULL,
    [APPEAL_CODE]             VARCHAR (40)     NOT NULL,
    [CAMPAIGN_CODE]           VARCHAR (10)     NOT NULL,
    [PRICE_FROM_COMPONENTS]   BIT              NOT NULL,
    [PUBLISH_START_DATE]      DATETIME         NULL,
    [PUBLISH_END_DATE]        DATETIME         NULL,
    [TAX_BY_LOCATION]         BIT              NOT NULL,
    [TAXCATEGORY_CODE]        VARCHAR (10)     NOT NULL,
    [RELATED_CONTENT_MESSAGE] VARCHAR (MAX)    NULL,
    [MINIMUM_GIFT_AMOUNT]     MONEY            NOT NULL,
    [ProductKey]              UNIQUEIDENTIFIER NOT NULL,
    [AllowOrderLineNote]      BIT              NOT NULL,
    [TIME_STAMP]              ROWVERSION       NULL
);
