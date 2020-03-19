﻿CREATE TABLE [tmp].[imis_Meet_Master] (
    [MEETING]                         VARCHAR (10)    CONSTRAINT [DF_Meet_Master_MEETING] DEFAULT ('') NOT NULL,
    [TITLE]                           VARCHAR (60)    CONSTRAINT [DF_Meet_Master_TITLE] DEFAULT ('') NOT NULL,
    [MEETING_TYPE]                    VARCHAR (5)     CONSTRAINT [DF_Meet_Master_MEETING_TYPE] DEFAULT ('') NOT NULL,
    [DESCRIPTION]                     NVARCHAR (MAX)  NULL,
    [BEGIN_DATE]                      DATETIME        NULL,
    [END_DATE]                        DATETIME        NULL,
    [STATUS]                          VARCHAR (1)     CONSTRAINT [DF_Meet_Master_STATUS] DEFAULT ('') NOT NULL,
    [ADDRESS_1]                       VARCHAR (50)    CONSTRAINT [DF_Meet_Master_ADDRESS_1] DEFAULT ('') NOT NULL,
    [ADDRESS_2]                       VARCHAR (50)    CONSTRAINT [DF_Meet_Master_ADDRESS_2] DEFAULT ('') NOT NULL,
    [ADDRESS_3]                       VARCHAR (50)    CONSTRAINT [DF_Meet_Master_ADDRESS_3] DEFAULT ('') NOT NULL,
    [CITY]                            VARCHAR (40)    CONSTRAINT [DF_Meet_Master_CITY] DEFAULT ('') NOT NULL,
    [STATE_PROVINCE]                  VARCHAR (15)    CONSTRAINT [DF_Meet_Master_STATE_PROVINCE] DEFAULT ('') NOT NULL,
    [ZIP]                             VARCHAR (10)    CONSTRAINT [DF_Meet_Master_ZIP] DEFAULT ('') NOT NULL,
    [COUNTRY]                         VARCHAR (25)    CONSTRAINT [DF_Meet_Master_COUNTRY] DEFAULT ('') NOT NULL,
    [DIRECTIONS]                      NVARCHAR (MAX)  NULL,
    [COORDINATORS]                    VARCHAR (200)   CONSTRAINT [DF_Meet_Master_COORDINATORS] DEFAULT ('') NOT NULL,
    [NOTES]                           NVARCHAR (MAX)  NULL,
    [ALLOW_REG_STRING]                NVARCHAR (MAX)  NULL,
    [EARLY_CUTOFF]                    DATETIME        NULL,
    [REG_CUTOFF]                      DATETIME        NULL,
    [LATE_CUTOFF]                     DATETIME        NULL,
    [ORG_CODE]                        VARCHAR (5)     CONSTRAINT [DF_Meet_Master_ORG_CODE] DEFAULT ('') NOT NULL,
    [LOGO]                            IMAGE           NULL,
    [MAX_REGISTRANTS]                 INT             CONSTRAINT [DF_Meet_Master_MAX_REGISTRANTS] DEFAULT ((0)) NOT NULL,
    [TOTAL_REGISTRANTS]               INT             CONSTRAINT [DF_Meet_Master_TOTAL_REGISTRANTS] DEFAULT ((0)) NOT NULL,
    [TOTAL_CANCELATIONS]              INT             CONSTRAINT [DF_Meet_Master_TOTAL_CANCELATIONS] DEFAULT ((0)) NOT NULL,
    [TOTAL_REVENUE]                   NUMERIC (15, 2) CONSTRAINT [DF_Meet_Master_TOTAL_REVENUE] DEFAULT ((0)) NOT NULL,
    [HEAD_COUNT]                      INT             CONSTRAINT [DF_Meet_Master_HEAD_COUNT] DEFAULT ((0)) NOT NULL,
    [TAX_AUTHORITY_1]                 VARCHAR (20)    CONSTRAINT [DF_Meet_Master_TAX_AUTHORITY_1] DEFAULT ('') NOT NULL,
    [SUPPRESS_COOR]                   BIT             CONSTRAINT [DF_Meet_Master_SUPPRESS_COOR] DEFAULT ((0)) NOT NULL,
    [SUPPRESS_DIR]                    BIT             CONSTRAINT [DF_Meet_Master_SUPPRESS_DIR] DEFAULT ((0)) NOT NULL,
    [SUPPRESS_NOTES]                  BIT             CONSTRAINT [DF_Meet_Master_SUPPRESS_NOTES] DEFAULT ((0)) NOT NULL,
    [MUF_1]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_1] DEFAULT ('') NOT NULL,
    [MUF_2]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_2] DEFAULT ('') NOT NULL,
    [MUF_3]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_3] DEFAULT ('') NOT NULL,
    [MUF_4]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_4] DEFAULT ('') NOT NULL,
    [MUF_5]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_5] DEFAULT ('') NOT NULL,
    [MUF_6]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_6] DEFAULT ('') NOT NULL,
    [MUF_7]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_7] DEFAULT ('') NOT NULL,
    [MUF_8]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_8] DEFAULT ('') NOT NULL,
    [MUF_9]                           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_9] DEFAULT ('') NOT NULL,
    [MUF_10]                          VARCHAR (100)   CONSTRAINT [DF_Meet_Master_MUF_10] DEFAULT ('') NOT NULL,
    [INTENT_TO_EDIT]                  VARCHAR (80)    CONSTRAINT [DF_Meet_Master_INTENT_TO_EDIT] DEFAULT ('') NOT NULL,
    [SUPPRESS_CONFIRM]                BIT             CONSTRAINT [DF_Meet_Master_SUPPRESS_CONFIRM] DEFAULT ((0)) NOT NULL,
    [WEB_VIEW_ONLY]                   BIT             CONSTRAINT [DF_Meet_Master_WEB_VIEW_ONLY] DEFAULT ((0)) NOT NULL,
    [WEB_ENABLED]                     BIT             CONSTRAINT [DF_Meet_Master_WEB_ENABLED] DEFAULT ((0)) NOT NULL,
    [POST_REGISTRATION]               BIT             CONSTRAINT [DF_Meet_Master_POST_REGISTRATION] DEFAULT ((0)) NOT NULL,
    [EMAIL_REGISTRATION]              BIT             CONSTRAINT [DF_Meet_Master_EMAIL_REGISTRATION] DEFAULT ((0)) NOT NULL,
    [MEETING_URL]                     VARCHAR (255)   CONSTRAINT [DF_Meet_Master_MEETING_URL] DEFAULT ('') NOT NULL,
    [MEETING_IMAGE_NAME]              VARCHAR (255)   CONSTRAINT [DF_Meet_Master_MEETING_IMAGE_NAME] DEFAULT ('') NOT NULL,
    [CONTACT_ID]                      VARCHAR (10)    CONSTRAINT [DF_Meet_Master_CONTACT_ID] DEFAULT ('') NOT NULL,
    [IS_FR_MEET]                      TINYINT         CONSTRAINT [DF_Meet_Master_IS_FR_MEET] DEFAULT ((0)) NOT NULL,
    [MEET_APPEAL]                     VARCHAR (40)    CONSTRAINT [DF_Meet_Master_MEET_APPEAL] DEFAULT ('') NOT NULL,
    [MEET_CAMPAIGN]                   VARCHAR (10)    CONSTRAINT [DF_Meet_Master_MEET_CAMPAIGN] DEFAULT ('') NOT NULL,
    [MEET_CATEGORY]                   TINYINT         CONSTRAINT [DF_Meet_Master_MEET_CATEGORY] DEFAULT ((0)) NOT NULL,
    [COMP_REG_REG_CLASS]              VARCHAR (100)   NULL,
    [COMP_REG_CALCULATION]            NVARCHAR (MAX)  NULL,
    [SQUARE_FOOT_RULES]               NVARCHAR (MAX)  NULL,
    [TAX_BY_ADDRESS]                  BIT             CONSTRAINT [DF_Meet_Master_TAX_BY_ADDRESS] DEFAULT ((0)) NOT NULL,
    [VAT_RULESET]                     VARCHAR (10)    CONSTRAINT [DF_Meet_Master_VAT_RULESET] DEFAULT ('') NOT NULL,
    [REG_CLASS_STORED_PROC]           VARCHAR (100)   CONSTRAINT [DF_Meet_Master_REG_CLASS_STORED_PROC] DEFAULT ('') NOT NULL,
    [WEB_REG_CLASS_METHOD]            INT             CONSTRAINT [DF_Meet_Master_WEB_REG_CLASS_METHOD] DEFAULT ((0)) NOT NULL,
    [REG_OTHERS]                      BIT             CONSTRAINT [DF_Meet_Master_REG_OTHERS] DEFAULT ((0)) NOT NULL,
    [ADD_GUESTS]                      BIT             CONSTRAINT [DF_Meet_Master_ADD_GUESTS] DEFAULT ((0)) NOT NULL,
    [WEB_DESC]                        NVARCHAR (MAX)  NULL,
    [ALLOW_REG_EDIT]                  BIT             CONSTRAINT [DF_Meet_Master_ALLOW_REG_EDIT] DEFAULT ((0)) NOT NULL,
    [REG_EDIT_CUTOFF]                 DATETIME        NULL,
    [FORM_DEFINITION_ID]              VARCHAR (36)    CONSTRAINT [DF_Meet_Master_Form_Definition_ID] DEFAULT ('') NOT NULL,
    [FORM_DEFINITION_SECTION_ID]      VARCHAR (36)    CONSTRAINT [DF_Meet_Master_Form_Definition_Section_ID] DEFAULT ('') NOT NULL,
    [PUBLISH_START_DATE]              DATETIME        NULL,
    [PUBLISH_END_DATE]                DATETIME        NULL,
    [REGISTRATION_START_DATE]         DATETIME        NULL,
    [REGISTRATION_END_DATE]           DATETIME        NULL,
    [REGISTRATION_CLOSED_MESSAGE]     VARCHAR (400)   NULL,
    [DEFAULT_PROGRAMITEM_DISPLAYMODE] INT             CONSTRAINT [DF_Meet_Master_DEFAULT_PROGRAMITEM_DISPLAYMODE] DEFAULT ((0)) NOT NULL,
    [TEMPLATE_STATE_CODE]             INT             CONSTRAINT [DF_Meet_Master_Template_State_Code] DEFAULT ((0)) NOT NULL,
    [ENABLE_TIME_CONFLICTS]           BIT             CONSTRAINT [DF_Meet_Master_Enable_Time_Conflicts] DEFAULT ((0)) NOT NULL,
    [ALLOW_REGISTRANT_CONFLICTS]      BIT             CONSTRAINT [DF_Meet_Master_Allow_Registrant_Conflicts] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]                      BIGINT          NULL,
    CONSTRAINT [PK_Meet_Master] PRIMARY KEY CLUSTERED ([MEETING] ASC)
);

