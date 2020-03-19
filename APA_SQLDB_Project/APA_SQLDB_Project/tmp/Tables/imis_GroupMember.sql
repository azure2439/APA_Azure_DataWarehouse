CREATE TABLE [tmp].[imis_GroupMember] (
    [GroupMemberKey]    UNIQUEIDENTIFIER CONSTRAINT [DF_GroupMember_GroupMemberKey] DEFAULT (newid()) NOT NULL,
    [GroupKey]          UNIQUEIDENTIFIER NOT NULL,
    [MemberContactKey]  UNIQUEIDENTIFIER NOT NULL,
    [IsActive]          BIT              CONSTRAINT [DF_GroupMember_IsActive] DEFAULT ((1)) NOT NULL,
    [CreatedByUserKey]  UNIQUEIDENTIFIER NOT NULL,
    [CreatedOn]         DATETIME         NOT NULL,
    [UpdatedByUserKey]  UNIQUEIDENTIFIER NOT NULL,
    [UpdatedOn]         DATETIME         NOT NULL,
    [DropDate]          DATETIME         NULL,
    [JoinDate]          DATETIME         NULL,
    [MarkedForDeleteOn] DATETIME         NULL,
    CONSTRAINT [PK_GroupMember] PRIMARY KEY CLUSTERED ([GroupMemberKey] ASC)
);

