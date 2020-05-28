CREATE TABLE [rpt].[dimAICPExam] (
    [ExamIDKey]             INT           IDENTITY (1, 1) NOT NULL,
    [ExamCode]              VARCHAR (200) NULL,
    [Title]                 VARCHAR (200) NULL,
    [Status]                VARCHAR (5)   NULL,
    [ExamStartDate]         DATETIME      NULL,
    [ExamEndDate]           DATETIME      NULL,
    [RegistrationStartDate] DATETIME      NULL,
    [RegistrationEndDate]   DATETIME      NULL,
    [ApplicationStartDate]  DATETIME      NULL,
    [ApplicationEndDate]    DATETIME      NULL,
    [IsAdvanced]            BIT           NULL,
    [LastUpdatedBy]         VARCHAR (40)  CONSTRAINT [DF_dimAICPExam_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]          DATETIME      CONSTRAINT [DF_dimAICPExam_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]              BIT           CONSTRAINT [DF_dimAICPExam_IsActive] DEFAULT ((1)) NULL,
    [StartDate]             DATETIME      CONSTRAINT [DF_dimAICPExam_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]               DATETIME      CONSTRAINT [DF_dimAICPExam_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ExamIDKey] ASC)
);

