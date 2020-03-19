CREATE TABLE [tmp].[imis_Custom_SchoolAccredited] (
    [ID]                  VARCHAR (10)  NOT NULL,
    [SEQN]                INT           DEFAULT ((0)) NOT NULL,
    [START_DATE]          DATETIME      NULL,
    [END_DATE]            DATETIME      NULL,
    [DEGREE_LEVEL]        VARCHAR (60)  DEFAULT ('') NOT NULL,
    [SCHOOL_PROGRAM_TYPE] VARCHAR (20)  DEFAULT ('') NOT NULL,
    [DEGREE_PROGRAM]      VARCHAR (255) DEFAULT ('') NOT NULL,
    [TIME_STAMP]          ROWVERSION    NULL,
    CONSTRAINT [PK_Custom_SchoolAccredited] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

