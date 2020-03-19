﻿CREATE TABLE [stg].[imis_Custom_AICP_Exam_Score] (
    [ID]                 VARCHAR (10)  DEFAULT ('') NOT NULL,
    [SEQN]               INT           DEFAULT ((0)) NOT NULL,
    [EXAM_CODE]          VARCHAR (15)  DEFAULT ('') NOT NULL,
    [EXAM_DATE]          DATETIME      NULL,
    [PASS]               BIT           DEFAULT ((0)) NOT NULL,
    [SCALED_SCORE]       INT           DEFAULT ((0)) NOT NULL,
    [RAW_SCORE]          INT           DEFAULT ((0)) NOT NULL,
    [SCORE_1]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_2]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_3]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_4]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_5]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_6]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_7]            INT           DEFAULT ((0)) NOT NULL,
    [SCORE_8]            INT           DEFAULT ((0)) NOT NULL,
    [TESTFORM_CODE]      VARCHAR (50)  DEFAULT ('') NOT NULL,
    [UPDATED_ON]         DATETIME      NULL,
    [TEST_CENTER]        VARCHAR (20)  DEFAULT ('') NOT NULL,
    [REGISTRANT_TYPE]    VARCHAR (50)  DEFAULT ('') NOT NULL,
    [FILE_NAME]          VARCHAR (255) DEFAULT ('') NOT NULL,
    [GEE_ELIGIBILITY_ID] VARCHAR (50)  DEFAULT ('') NOT NULL,
    [TIME_STAMP]         BIGINT        NULL,
    [Id_Identitycolumn]  INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]      VARCHAR (40)  CONSTRAINT [df_aicpLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME      CONSTRAINT [df_aicpLastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT           NULL,
    [StartDate]          DATETIME      CONSTRAINT [df_aicpStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME      CONSTRAINT [df_aicpEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Custom_AICP_Exam_Score] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);
