﻿CREATE TABLE [rpt].[tblFactMember] (
    [MemberIDKey]      INT          NOT NULL,
    [ProductTypeKey]   INT          NOT NULL,
    [ProductCodeKey]   INT          NOT NULL,
    [MemberTypeIDKey]  INT          NOT NULL,
    [SalaryIDKey]      INT          NOT NULL,
    [RaceIDKey]        INT          NOT NULL,
    [OrgKey]           INT          NOT NULL,
    [ChapterKey]       INT          NOT NULL,
    [Trans_Date_Key]   INT          NOT NULL,
    [Paid_Through_Key] INT          NOT NULL,
    [Home_Address_Key] INT          NOT NULL,
    [Work_Address_Key] INT          NOT NULL,
    [StatusKey]        INT          NOT NULL,
    [MemberStatusKey]  INT          NULL,
    [LastUpdated]      DATETIME     DEFAULT (getdate()) NULL,
    [LastModifiedBy]   VARCHAR (40) DEFAULT (suser_sname()) NULL,
    [IsCurrent]        BIT          NOT NULL,
    CONSTRAINT [FK_tblFactMember_dimAddress] FOREIGN KEY ([Home_Address_Key]) REFERENCES [rpt].[dimAddress] ([Address_Key]),
    CONSTRAINT [FK_tblFactMember_dimChapter] FOREIGN KEY ([ChapterKey]) REFERENCES [rpt].[dimChapter] ([ChapterKey]),
    CONSTRAINT [FK_tblFactMember_dimDate] FOREIGN KEY ([Paid_Through_Key]) REFERENCES [rpt].[dimDate] ([Date_Key]),
    CONSTRAINT [FK_tblFactMember_dimMember] FOREIGN KEY ([MemberIDKey]) REFERENCES [rpt].[dimMember] ([MemberIDKey]),
    CONSTRAINT [FK_tblFactMember_dimMemberStatus] FOREIGN KEY ([MemberStatusKey]) REFERENCES [rpt].[dimMemberStatus] ([MemberStatusKey]),
    CONSTRAINT [FK_tblFactMember_dimMemberType] FOREIGN KEY ([MemberTypeIDKey]) REFERENCES [rpt].[dimMemberType] ([MemberTypeIDKey]),
    CONSTRAINT [FK_tblFactMember_dimOrg] FOREIGN KEY ([OrgKey]) REFERENCES [rpt].[dimOrg] ([OrgKey]),
    CONSTRAINT [FK_tblFactMember_dimProductCode] FOREIGN KEY ([ProductCodeKey]) REFERENCES [rpt].[dimProductCode] ([ProductCodeKey]),
    CONSTRAINT [FK_tblFactMember_dimProductType] FOREIGN KEY ([ProductTypeKey]) REFERENCES [rpt].[dimProductType] ([ProductTypeKey]),
    CONSTRAINT [FK_tblFactMember_dimRace] FOREIGN KEY ([RaceIDKey]) REFERENCES [rpt].[dimRace] ([RaceIDKey]),
    CONSTRAINT [FK_tblFactMember_dimSalary] FOREIGN KEY ([SalaryIDKey]) REFERENCES [rpt].[dimSalary] ([SalaryIDKey])
);

