CREATE TABLE [tmp].[Fact_Jobs] (
    [Job_ID]            INT           NOT NULL,
    [City]              VARCHAR (40)  NULL,
    [State]             VARCHAR (100) NULL,
    [Country]           VARCHAR (100) NULL,
    [Job_Type]          VARCHAR (50)  NOT NULL,
    [Company]           VARCHAR (80)  NULL,
    [Salary_Range]      VARCHAR (50)  NULL,
    [Post_Time]         DATETIME      NULL,
    [Title]             VARCHAR (200) NULL,
    [Status]            VARCHAR (5)   NULL,
    [MemberID_Author]   VARCHAR (7)   NULL,
    [MemberID_Provider] VARCHAR (7)   NULL,
    [Lastupdated]       DATETIME      DEFAULT (getdate()) NULL,
    [LastModifiedBy]    VARCHAR (40)  DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([Job_ID] ASC)
);

