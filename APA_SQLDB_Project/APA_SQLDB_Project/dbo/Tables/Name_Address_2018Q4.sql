﻿CREATE TABLE [dbo].[Name_Address_2018Q4] (
    [ID]             VARCHAR (10)  CONSTRAINT [DF_Name_Address_2018Q4_ID] DEFAULT ('') NOT NULL,
    [ADDRESS_NUM]    INT           CONSTRAINT [DF_Name_Address_2018Q4_ADDRESS_NUM] DEFAULT ((0)) NOT NULL,
    [PURPOSE]        VARCHAR (20)  CONSTRAINT [DF_Name_Address_2018Q4_PURPOSE] DEFAULT ('') NOT NULL,
    [COMPANY]        VARCHAR (80)  CONSTRAINT [DF_Name_Address_2018Q4_COMPANY] DEFAULT ('') NOT NULL,
    [ADDRESS_1]      VARCHAR (40)  CONSTRAINT [DF_Name_Address_2018Q4_ADDRESS_1] DEFAULT ('') NOT NULL,
    [ADDRESS_2]      VARCHAR (40)  CONSTRAINT [DF_Name_Address_2018Q4_ADDRESS_2] DEFAULT ('') NOT NULL,
    [CITY]           VARCHAR (40)  CONSTRAINT [DF_Name_Address_2018Q4_CITY] DEFAULT ('') NOT NULL,
    [STATE_PROVINCE] VARCHAR (15)  CONSTRAINT [DF_Name_Address_2018Q4_STATE_PROVINCE] DEFAULT ('') NOT NULL,
    [ZIP]            VARCHAR (10)  CONSTRAINT [DF_Name_Address_2018Q4_ZIP] DEFAULT ('') NOT NULL,
    [COUNTRY]        VARCHAR (25)  CONSTRAINT [DF_Name_Address_2018Q4_COUNTRY] DEFAULT ('') NOT NULL,
    [CRRT]           VARCHAR (40)  CONSTRAINT [DF_Name_Address_2018Q4_CRRT] DEFAULT ('') NOT NULL,
    [DPB]            VARCHAR (8)   CONSTRAINT [DF_Name_Address_2018Q4_DPB] DEFAULT ('') NOT NULL,
    [BAR_CODE]       VARCHAR (14)  CONSTRAINT [DF_Name_Address_2018Q4_BAR_CODE] DEFAULT ('') NOT NULL,
    [COUNTRY_CODE]   VARCHAR (10)  CONSTRAINT [DF_Name_Address_2018Q4_COUNTRY_CODE] DEFAULT ('') NOT NULL,
    [ADDRESS_FORMAT] TINYINT       CONSTRAINT [DF_Name_Address_2018Q4_ADDRESS_FORMAT] DEFAULT ((0)) NOT NULL,
    [FULL_ADDRESS]   VARCHAR (255) CONSTRAINT [DF_Name_Address_2018Q4_FULL_ADDRESS] DEFAULT ('') NOT NULL,
    [COUNTY]         VARCHAR (30)  CONSTRAINT [DF_Name_Address_2018Q4_COUNTY] DEFAULT ('') NOT NULL,
    [US_CONGRESS]    VARCHAR (5)   CONSTRAINT [DF_Name_Address_2018Q4_US_CONGRESS] DEFAULT ('') NOT NULL,
    [STATE_SENATE]   VARCHAR (5)   CONSTRAINT [DF_Name_Address_2018Q4_STATE_SENATE] DEFAULT ('') NOT NULL,
    [STATE_HOUSE]    VARCHAR (5)   CONSTRAINT [DF_Name_Address_2018Q4_STATE_HOUSE] DEFAULT ('') NOT NULL,
    [MAIL_CODE]      VARCHAR (10)  CONSTRAINT [DF_Name_Address_2018Q4_MAIL_CODE] DEFAULT ('') NOT NULL,
    [PHONE]          VARCHAR (25)  CONSTRAINT [DF_Name_Address_2018Q4_PHONE] DEFAULT ('') NOT NULL,
    [FAX]            VARCHAR (25)  CONSTRAINT [DF_Name_Address_2018Q4_FAX] DEFAULT ('') NOT NULL,
    [TOLL_FREE]      VARCHAR (25)  CONSTRAINT [DF_Name_Address_2018Q4_TOLL_FREE] DEFAULT ('') NOT NULL,
    [COMPANY_SORT]   VARCHAR (30)  CONSTRAINT [DF_Name_Address_2018Q4_COMPANY_SORT] DEFAULT ('') NOT NULL,
    [NOTE]           TEXT          NULL,
    [STATUS]         VARCHAR (5)   CONSTRAINT [DF_Name_Address_2018Q4_STATUS] DEFAULT ('') NOT NULL,
    [LAST_UPDATED]   DATETIME      NULL,
    [LIST_STRING]    VARCHAR (255) CONSTRAINT [DF_Name_Address_2018Q4_LIST_STRING] DEFAULT ('') NOT NULL,
    [PREFERRED_MAIL] BIT           CONSTRAINT [DF_Name_Address_2018Q4_PREFERRED_MAIL] DEFAULT ((0)) NOT NULL,
    [PREFERRED_BILL] BIT           CONSTRAINT [DF_Name_Address_2018Q4_PREFERRED_BILL] DEFAULT ((0)) NOT NULL,
    [LAST_VERIFIED]  DATETIME      NULL,
    [EMAIL]          VARCHAR (100) CONSTRAINT [DF_Name_Address_2018Q4_EMAIL] DEFAULT ('') NOT NULL,
    [BAD_ADDRESS]    VARCHAR (10)  CONSTRAINT [DF_Name_Address_2018Q4_BAD_ADDRESS] DEFAULT ('') NOT NULL,
    [NO_AUTOVERIFY]  BIT           CONSTRAINT [DF_Name_Address_2018Q4_NO_AUTOVERIFY] DEFAULT ((0)) NOT NULL,
    [LAST_QAS_BATCH] DATETIME      NULL,
    [ADDRESS_3]      VARCHAR (40)  CONSTRAINT [DF_Name_Address_2018Q4_ADDRESS_3] DEFAULT ('') NOT NULL,
    [PREFERRED_SHIP] BIT           CONSTRAINT [DF_Name_Address_2018Q4_PREFERRED_SHIP] DEFAULT ((0)) NOT NULL,
    [INFORMAL]       VARCHAR (20)  CONSTRAINT [DF_Name_Address_2018Q4_INFORMAL] DEFAULT ('') NOT NULL,
    [TITLE]          VARCHAR (80)  CONSTRAINT [DF_Name_Address_2018Q4_TITLE] DEFAULT ('') NOT NULL,
    [TIME_STAMP]     BIGINT        NULL,
    CONSTRAINT [PK_Name_Address_2018Q4] PRIMARY KEY NONCLUSTERED ([ADDRESS_NUM] ASC)
);
