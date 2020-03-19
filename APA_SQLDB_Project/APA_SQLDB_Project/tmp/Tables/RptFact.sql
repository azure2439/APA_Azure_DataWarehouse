CREATE TABLE [tmp].[RptFact] (
    [MemberIDKey]      INT      NOT NULL,
    [ProductTypeKey]   INT      NOT NULL,
    [ProductCodeKey]   INT      NOT NULL,
    [MemberTypeIDKey]  INT      NOT NULL,
    [SalaryIDKey]      INT      NOT NULL,
    [RaceIDKey]        INT      NOT NULL,
    [OrgKey]           INT      NOT NULL,
    [ChapterKey]       INT      NOT NULL,
    [Trans_Date_Key]   INT      NOT NULL,
    [Paid_Through_Key] INT      NOT NULL,
    [Home_Address_Key] INT      NOT NULL,
    [Work_Address_Key] INT      NOT NULL,
    [StatusKey]        INT      NOT NULL,
    [MemberStatusKey]  INT      NULL,
    [LastUpdated]      DATETIME NULL,
    [LastModifiedBy]   DATETIME NULL,
    [IsCurrent]        BIT      NOT NULL
);

