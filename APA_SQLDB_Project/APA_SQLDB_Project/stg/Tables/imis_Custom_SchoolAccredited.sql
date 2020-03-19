CREATE TABLE [stg].[imis_Custom_SchoolAccredited] (
    [ID]                  VARCHAR (10)  DEFAULT ('') NOT NULL,
    [SEQN]                INT           DEFAULT ((0)) NOT NULL,
    [START_DATE]          DATETIME      NULL,
    [END_DATE]            DATETIME      NULL,
    [DEGREE_LEVEL]        VARCHAR (60)  DEFAULT ('') NOT NULL,
    [SCHOOL_PROGRAM_TYPE] VARCHAR (20)  DEFAULT ('') NOT NULL,
    [DEGREE_PROGRAM]      VARCHAR (255) DEFAULT ('') NOT NULL,
    [TIME_STAMP]          BIGINT        NULL,
    [Id_Identitycolumn]   INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]       VARCHAR (40)  CONSTRAINT [df_schoolLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME      CONSTRAINT [df_schoolLastModified] DEFAULT (getdate()) NULL,
    [IsActive]            BIT           NULL,
    [StartDate]           DATETIME      CONSTRAINT [df_schoolStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]             DATETIME      CONSTRAINT [df_schoolEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Custom_SchoolAccredited] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

