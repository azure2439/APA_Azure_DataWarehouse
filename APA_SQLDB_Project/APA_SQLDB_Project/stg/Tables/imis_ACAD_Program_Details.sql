CREATE TABLE [stg].[imis_ACAD_Program_Details] (
    [ID]                     VARCHAR (10)  CONSTRAINT [DF__ACAD_Program__ID__0D3F9AC5] DEFAULT ('') NOT NULL,
    [PROGRAM_NAME]           VARCHAR (150) CONSTRAINT [DF__ACAD_Prog__PROGR__0E33BEFE] DEFAULT ('') NOT NULL,
    [CHAIR_IMIS_ID]          VARCHAR (10)  CONSTRAINT [DF__ACAD_Prog__CHAIR__0F27E337] DEFAULT ('') NOT NULL,
    [CHAIR_TITLE]            VARCHAR (150) CONSTRAINT [DF__ACAD_Prog__CHAIR__101C0770] DEFAULT ('') NOT NULL,
    [FSTU_COUNT]             INT           CONSTRAINT [DF__ACAD_Prog__FSTU___11102BA9] DEFAULT ((0)) NOT NULL,
    [ACAD_PROGRAM_TYPE]      VARCHAR (20)  CONSTRAINT [DF__ACAD_Prog__ACAD___12044FE2] DEFAULT ('') NOT NULL,
    [FSTU_COORDINATOR_ID]    VARCHAR (10)  CONSTRAINT [DF__ACAD_Prog__FSTU___12F8741B] DEFAULT ('') NOT NULL,
    [SCH_PAYING_AICP]        BIT           CONSTRAINT [DF__ACAD_Prog__SCH_P__13EC9854] DEFAULT ((0)) NOT NULL,
    [SIGNUP_DATE]            DATETIME      NULL,
    [ACTIVE_ACAD_MEMBERSHIP] BIT           CONSTRAINT [DF__ACAD_Prog__ACTIV__14E0BC8D] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]             BIGINT        NULL,
    [Id_Identitycolumn]      INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]          VARCHAR (40)  CONSTRAINT [df_acadLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]           DATETIME      CONSTRAINT [df_acadLastModified] DEFAULT (getdate()) NULL,
    [IsActive]               BIT           NULL,
    [StartDate]              DATETIME      CONSTRAINT [df_acadStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                DATETIME      CONSTRAINT [df_acadEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [pkACAD_Program_DetailsID] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

