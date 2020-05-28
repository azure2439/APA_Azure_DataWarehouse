CREATE TABLE [rpt].[dimJob] (
    [JobIDKey]          INT           NOT NULL,
    [Job_Type]          VARCHAR (50)  NULL,
    [City]              VARCHAR (40)  NULL,
    [State]             VARCHAR (100) NULL,
    [Country]           VARCHAR (100) NULL,
    [Company]           VARCHAR (80)  NULL,
    [Salary_Range]      VARCHAR (50)  NULL,
    [Post_Time]         DATETIME      NULL,
    [Title]             VARCHAR (200) NULL,
    [Status]            VARCHAR (5)   NULL,
    [MemberID_Author]   VARCHAR (7)   NULL,
    [MemberID_Provider] VARCHAR (7)   NULL,
    [LastUpdatedBy]     VARCHAR (40)  CONSTRAINT [DF_dimJob_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME      CONSTRAINT [DF_dimJob_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT           CONSTRAINT [DF_dimJob_IsActive] DEFAULT ((1)) NULL,
    [StartDate]         DATETIME      CONSTRAINT [DF_dimJob_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME      CONSTRAINT [DF_dimJob_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([JobIDKey] ASC)
);

