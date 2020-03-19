﻿CREATE TABLE [rpt].[tblFactActivity] (
    [MemberIDKey]         INT          NOT NULL,
    [MemberTypeIDKey]     INT          NOT NULL,
    [ProductTypeKey]      INT          NOT NULL,
    [ProductCodeKey]      INT          NOT NULL,
    [ActivityCategoryKey] INT          NOT NULL,
    [CampaignCodeKey]     INT          NOT NULL,
    [Trans_Date_Key]      INT          NOT NULL,
    [Paid_Through_Key]    INT          NOT NULL,
    [SourceKey]           INT          NOT NULL,
    [Quantity]            INT          NULL,
    [Amount]              MONEY        NULL,
    [Units]               INT          NULL,
    [LastUpdated]         DATETIME     DEFAULT (getdate()) NULL,
    [LastModifiedBy]      VARCHAR (40) DEFAULT (suser_sname()) NULL,
    [SourceCodeKey]       INT          NULL,
    FOREIGN KEY ([ActivityCategoryKey]) REFERENCES [rpt].[dimActivityCategory] ([ActivityCategoryKey]),
    FOREIGN KEY ([CampaignCodeKey]) REFERENCES [rpt].[dimCampaignCode] ([CampaignCodeKey]),
    FOREIGN KEY ([MemberIDKey]) REFERENCES [rpt].[dimMember] ([MemberIDKey]),
    FOREIGN KEY ([MemberTypeIDKey]) REFERENCES [rpt].[dimMemberType] ([MemberTypeIDKey]),
    FOREIGN KEY ([Paid_Through_Key]) REFERENCES [rpt].[dimDate] ([Date_Key]),
    FOREIGN KEY ([ProductCodeKey]) REFERENCES [rpt].[dimProductCode] ([ProductCodeKey]),
    FOREIGN KEY ([ProductTypeKey]) REFERENCES [rpt].[dimProductType] ([ProductTypeKey]),
    FOREIGN KEY ([SourceCodeKey]) REFERENCES [rpt].[dimSourceCode] ([SourceCodeKey]),
    FOREIGN KEY ([SourceKey]) REFERENCES [rpt].[dimSource] ([SourceKey]),
    FOREIGN KEY ([Trans_Date_Key]) REFERENCES [rpt].[dimDate] ([Date_Key])
);
