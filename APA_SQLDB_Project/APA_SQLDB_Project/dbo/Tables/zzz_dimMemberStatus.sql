CREATE TABLE [dbo].[zzz_dimMemberStatus] (
    [MemberStatusKey] INT          IDENTITY (1, 1) NOT NULL,
    [Member_Status]   VARCHAR (10) NULL,
    [Description]     VARCHAR (30) NULL,
    [LastUpdatedBy]   VARCHAR (40) NULL,
    [LastModified]    DATETIME     NULL,
    [IsActive]        BIT          NULL,
    [StartDate]       DATETIME     NULL,
    [EndDate]         DATETIME     NULL,
    [Complimentary]   BIT          NULL
);

