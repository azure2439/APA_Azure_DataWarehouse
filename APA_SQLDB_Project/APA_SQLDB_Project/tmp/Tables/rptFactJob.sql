CREATE TABLE [tmp].[rptFactJob] (
    [JobIDKey]            INT           NOT NULL,
    [JobTypeKey]          INT           NOT NULL,
    [StatusKey]           INT           NOT NULL,
    [JobAddressKey]       INT           NOT NULL,
    [Salary_Range]        VARCHAR (50)  NULL,
    [Company]             VARCHAR (80)  NULL,
    [Job_Title]           VARCHAR (200) NULL,
    [PostTimeKey]         INT           NULL,
    [MemberIDAuthorKey]   INT           NOT NULL,
    [MemberIDProviderKey] INT           NOT NULL,
    [LastUpdatedBy]       VARCHAR (40)  DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME      DEFAULT (getdate()) NULL
);

