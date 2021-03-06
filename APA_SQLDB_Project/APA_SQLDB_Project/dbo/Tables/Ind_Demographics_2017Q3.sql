﻿CREATE TABLE [dbo].[Ind_Demographics_2017Q3] (
    [ID]                          VARCHAR (10)  DEFAULT ('') NOT NULL,
    [APA_LIFE_DATE]               DATETIME      NULL,
    [AICP_LIFE_MEMBER]            BIT           DEFAULT ((0)) NOT NULL,
    [AICP_LIFE_DATE]              DATETIME      NULL,
    [FACULTY_POSITION]            VARCHAR (5)   DEFAULT ('') NOT NULL,
    [ADMIN_POSITION]              VARCHAR (5)   DEFAULT ('') NOT NULL,
    [SALARY_RANGE]                VARCHAR (5)   DEFAULT ('') NOT NULL,
    [PROMOTION_CODES]             VARCHAR (60)  DEFAULT ('') NOT NULL,
    [DATE_OF_BIRTH]               DATETIME      NULL,
    [SUB_SPECIALTY]               VARCHAR (6)   DEFAULT ('') NOT NULL,
    [USA_CITIZEN]                 BIT           DEFAULT ((0)) NOT NULL,
    [AICP_START]                  DATETIME      NULL,
    [AICP_CERT_NO]                VARCHAR (10)  DEFAULT ('') NOT NULL,
    [PERPETUITY]                  BIT           DEFAULT ((0)) NOT NULL,
    [AICP_PROMO_1]                VARCHAR (20)  DEFAULT ('') NOT NULL,
    [HINT_PASSWORD]               VARCHAR (4)   DEFAULT ('') NOT NULL,
    [HINT_ANSWER]                 VARCHAR (60)  DEFAULT ('') NOT NULL,
    [COUNTRY_CODES]               VARCHAR (5)   DEFAULT ('') NOT NULL,
    [SPECIALTY]                   VARCHAR (50)  DEFAULT ('') NOT NULL,
    [APA_LIFE_MEMBER]             BIT           DEFAULT ((0)) NOT NULL,
    [CONF_CODE]                   VARCHAR (4)   DEFAULT ('') NOT NULL,
    [MENTOR_SIGNUP]               BIT           DEFAULT ((0)) NOT NULL,
    [MALE]                        BIT           DEFAULT ((0)) NOT NULL,
    [FEMALE]                      BIT           DEFAULT ((0)) NOT NULL,
    [DEPARTMENT]                  VARCHAR (50)  DEFAULT ('') NOT NULL,
    [CONV_NP]                     BIT           DEFAULT ((0)) NOT NULL,
    [INVOICE_NUM]                 VARCHAR (30)  DEFAULT ('') NOT NULL,
    [PREV_MT]                     VARCHAR (10)  DEFAULT ('') NOT NULL,
    [CONV_FREESTU]                BIT           DEFAULT ((0)) NOT NULL,
    [CONV_STU]                    BIT           DEFAULT ((0)) NOT NULL,
    [CHAPT_ONLY]                  BIT           DEFAULT ((0)) NOT NULL,
    [ASLA]                        BIT           DEFAULT ((0)) NOT NULL,
    [SALARY_VERIFYDATE]           DATETIME      NULL,
    [FUNCTIONAL_TITLE_VERIFYDATE] DATETIME      NULL,
    [PREVIOUS_AICP_CERT_NO]       VARCHAR (10)  DEFAULT ('') NOT NULL,
    [PREVIOUS_AICP_START]         DATETIME      NULL,
    [EMAIL_SECONDARY]             VARCHAR (100) DEFAULT ('') NOT NULL,
    [CONV_ECP5]                   BIT           DEFAULT ((0)) NOT NULL,
    [EXCLUDE_FROM_DROP]           BIT           DEFAULT ((0)) NOT NULL,
    [STUDENT_START_DATE]          DATETIME      NULL,
    [IS_CURRENT_STUDENT]          BIT           DEFAULT ((0)) NOT NULL,
    [STUDENT_PROGRAM_YEAR]        FLOAT (53)    NOT NULL,
    [JOIN_TYPE]                   VARCHAR (25)  DEFAULT ('') NOT NULL,
    [UNDERPAID_MEMBER]            BIT           DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]                  ROWVERSION    NULL,
    [ECP_START_DATE]              DATETIME      NULL,
    CONSTRAINT [PK_Ind_Demographics_2017Q3] PRIMARY KEY CLUSTERED ([ID] ASC)
);

