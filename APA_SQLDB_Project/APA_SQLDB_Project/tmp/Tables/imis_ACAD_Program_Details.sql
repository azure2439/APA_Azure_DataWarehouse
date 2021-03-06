﻿CREATE TABLE [tmp].[imis_ACAD_Program_Details] (
    [ID]                     VARCHAR (10)  NOT NULL,
    [PROGRAM_NAME]           VARCHAR (150) CONSTRAINT [DF__ACAD_Prog__PROGR__0E33BEFE] DEFAULT ('') NOT NULL,
    [CHAIR_IMIS_ID]          VARCHAR (10)  CONSTRAINT [DF__ACAD_Prog__CHAIR__0F27E337] DEFAULT ('') NOT NULL,
    [CHAIR_TITLE]            VARCHAR (150) CONSTRAINT [DF__ACAD_Prog__CHAIR__101C0770] DEFAULT ('') NOT NULL,
    [FSTU_COUNT]             INT           CONSTRAINT [DF__ACAD_Prog__FSTU___11102BA9] DEFAULT ((0)) NOT NULL,
    [ACAD_PROGRAM_TYPE]      VARCHAR (20)  CONSTRAINT [DF__ACAD_Prog__ACAD___12044FE2] DEFAULT ('') NOT NULL,
    [FSTU_COORDINATOR_ID]    VARCHAR (10)  CONSTRAINT [DF__ACAD_Prog__FSTU___12F8741B] DEFAULT ('') NOT NULL,
    [SCH_PAYING_AICP]        BIT           CONSTRAINT [DF__ACAD_Prog__SCH_P__13EC9854] DEFAULT ((0)) NOT NULL,
    [SIGNUP_DATE]            DATETIME      NULL,
    [ACTIVE_ACAD_MEMBERSHIP] BIT           CONSTRAINT [DF__ACAD_Prog__ACTIV__14E0BC8D] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]             ROWVERSION    NULL,
    CONSTRAINT [pkACAD_Program_DetailsID] PRIMARY KEY CLUSTERED ([ID] ASC)
);

