CREATE TABLE [stg].[imis_GroupMember] (
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
    [Id_Identitycolumn] INT              IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40)     CONSTRAINT [df_GroupMember_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME         CONSTRAINT [df_GroupMember_LastModified] DEFAULT (getdate()) NULL,
    [StartDate]         DATETIME         CONSTRAINT [df_GroupMember_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME         CONSTRAINT [df_GroupMember_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_GroupMember] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

