CREATE TABLE [rpt].[dimMember] (
    [MemberIDKey]      INT           IDENTITY (1, 1) NOT NULL,
    [Parent]           VARCHAR (10)  NULL,
    [Member_ID]        VARCHAR (10)  NULL,
    [First_Name]       VARCHAR (70)  NULL,
    [Middle_Name]      VARCHAR (70)  NULL,
    [Last_Name]        VARCHAR (70)  NULL,
    [Full_Name]        VARCHAR (70)  NULL,
    [Email]            VARCHAR (100) NULL,
    [Home_Phone]       VARCHAR (25)  NULL,
    [Mobile_Phone]     VARCHAR (25)  NULL,
    [Work_Phone]       VARCHAR (25)  NULL,
    [Designation]      VARCHAR (20)  NULL,
    [Functional_Title] VARCHAR (20)  NULL,
    [Birth_Date]       DATETIME      NULL,
    [JoinDate]         DATETIME      NULL,
    [Cohort]           VARCHAR (20)  NULL,
    [LastUpdatedBy]    VARCHAR (40)  CONSTRAINT [DF_dimMember_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]     DATETIME      CONSTRAINT [DF_dimMember_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]         BIT           CONSTRAINT [DF_dimMember_IsActive] DEFAULT ((1)) NULL,
    [StartDate]        DATETIME      CONSTRAINT [DF_dimMember_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]          DATETIME      CONSTRAINT [DF_dimMember_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [Merged_Date]      DATETIME      NULL,
    [Cohortquarter]    VARCHAR (20)  NULL,
    PRIMARY KEY CLUSTERED ([MemberIDKey] ASC)
);


GO
CREATE NONCLUSTERED INDEX [nci_wi_dimMember_ECE90ED2C450FF43E7DF79B7B7C46556]
    ON [rpt].[dimMember]([Member_ID] ASC, [IsActive] ASC)
    INCLUDE([Birth_Date], [Cohort], [Cohortquarter], [Designation], [Email], [First_Name], [Full_Name], [Functional_Title], [Home_Phone], [JoinDate], [Last_Name], [Merged_Date], [Middle_Name], [Mobile_Phone], [Parent], [Work_Phone]);

