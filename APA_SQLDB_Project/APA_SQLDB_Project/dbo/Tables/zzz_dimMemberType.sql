CREATE TABLE [dbo].[zzz_dimMemberType] (
    [MemberTypeIDKey] INT           IDENTITY (1, 1) NOT NULL,
    [Member_Type]     VARCHAR (5)   NULL,
    [Description]     VARCHAR (255) NULL,
    [LastUpdatedBy]   VARCHAR (40)  NULL,
    [LastModified]    DATETIME      NULL,
    [IsActive]        BIT           NULL,
    [StartDate]       DATETIME      NULL,
    [EndDate]         DATETIME      NULL,
    [Member_Record]   BIT           NULL,
    [Company_Record]  BIT           NULL,
    [Dues_Code_1]     VARCHAR (40)  NULL
);

