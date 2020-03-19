﻿CREATE TABLE [dbo].[Name_2017Q4] (
    [ID]                 VARCHAR (10)  NOT NULL,
    [ORG_CODE]           VARCHAR (5)   NOT NULL,
    [MEMBER_TYPE]        VARCHAR (5)   NOT NULL,
    [CATEGORY]           VARCHAR (5)   NOT NULL,
    [STATUS]             VARCHAR (5)   NOT NULL,
    [MAJOR_KEY]          VARCHAR (15)  NOT NULL,
    [CO_ID]              VARCHAR (10)  NOT NULL,
    [LAST_FIRST]         VARCHAR (70)  NOT NULL,
    [COMPANY_SORT]       VARCHAR (30)  NOT NULL,
    [BT_ID]              VARCHAR (10)  NOT NULL,
    [DUP_MATCH_KEY]      VARCHAR (20)  NOT NULL,
    [FULL_NAME]          VARCHAR (70)  NOT NULL,
    [TITLE]              VARCHAR (80)  NOT NULL,
    [COMPANY]            VARCHAR (80)  NOT NULL,
    [FULL_ADDRESS]       VARCHAR (255) NOT NULL,
    [PREFIX]             VARCHAR (25)  NOT NULL,
    [FIRST_NAME]         VARCHAR (20)  NOT NULL,
    [MIDDLE_NAME]        VARCHAR (20)  NOT NULL,
    [LAST_NAME]          VARCHAR (30)  NOT NULL,
    [SUFFIX]             VARCHAR (10)  NOT NULL,
    [DESIGNATION]        VARCHAR (20)  NOT NULL,
    [INFORMAL]           VARCHAR (20)  NOT NULL,
    [WORK_PHONE]         VARCHAR (25)  NOT NULL,
    [HOME_PHONE]         VARCHAR (25)  NOT NULL,
    [FAX]                VARCHAR (25)  NOT NULL,
    [TOLL_FREE]          VARCHAR (25)  NOT NULL,
    [CITY]               VARCHAR (40)  NOT NULL,
    [STATE_PROVINCE]     VARCHAR (15)  NOT NULL,
    [ZIP]                VARCHAR (10)  NOT NULL,
    [COUNTRY]            VARCHAR (25)  NOT NULL,
    [MAIL_CODE]          VARCHAR (10)  NOT NULL,
    [CRRT]               VARCHAR (40)  NOT NULL,
    [BAR_CODE]           VARCHAR (14)  NOT NULL,
    [COUNTY]             VARCHAR (30)  NOT NULL,
    [MAIL_ADDRESS_NUM]   INT           NOT NULL,
    [BILL_ADDRESS_NUM]   INT           NOT NULL,
    [GENDER]             VARCHAR (1)   NOT NULL,
    [BIRTH_DATE]         DATETIME      NULL,
    [US_CONGRESS]        VARCHAR (20)  NOT NULL,
    [STATE_SENATE]       VARCHAR (20)  NOT NULL,
    [STATE_HOUSE]        VARCHAR (20)  NOT NULL,
    [SIC_CODE]           VARCHAR (10)  NOT NULL,
    [CHAPTER]            VARCHAR (15)  NOT NULL,
    [FUNCTIONAL_TITLE]   VARCHAR (50)  NOT NULL,
    [CONTACT_RANK]       INT           NOT NULL,
    [MEMBER_RECORD]      BIT           NOT NULL,
    [COMPANY_RECORD]     BIT           NOT NULL,
    [JOIN_DATE]          DATETIME      NULL,
    [SOURCE_CODE]        VARCHAR (40)  NOT NULL,
    [PAID_THRU]          DATETIME      NULL,
    [MEMBER_STATUS]      VARCHAR (5)   NOT NULL,
    [MEMBER_STATUS_DATE] DATETIME      NULL,
    [PREVIOUS_MT]        VARCHAR (5)   NOT NULL,
    [MT_CHANGE_DATE]     DATETIME      NULL,
    [CO_MEMBER_TYPE]     VARCHAR (5)   NOT NULL,
    [EXCLUDE_MAIL]       BIT           NOT NULL,
    [EXCLUDE_DIRECTORY]  BIT           NOT NULL,
    [DATE_ADDED]         DATETIME      NULL,
    [LAST_UPDATED]       DATETIME      NULL,
    [UPDATED_BY]         VARCHAR (60)  NOT NULL,
    [INTENT_TO_EDIT]     VARCHAR (80)  NOT NULL,
    [ADDRESS_NUM_1]      INT           NOT NULL,
    [ADDRESS_NUM_2]      INT           NOT NULL,
    [ADDRESS_NUM_3]      INT           NOT NULL,
    [EMAIL]              VARCHAR (100) NOT NULL,
    [WEBSITE]            VARCHAR (255) NOT NULL,
    [SHIP_ADDRESS_NUM]   INT           NOT NULL,
    [DISPLAY_CURRENCY]   VARCHAR (3)   NOT NULL,
    [MOBILE_PHONE]       VARCHAR (25)  NOT NULL,
    [TIME_STAMP]         BIGINT        NULL
);

