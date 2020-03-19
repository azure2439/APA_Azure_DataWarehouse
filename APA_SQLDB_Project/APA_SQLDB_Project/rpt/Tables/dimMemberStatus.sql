CREATE TABLE [rpt].[dimMemberStatus] (
    [MemberStatusKey] INT          IDENTITY (1, 1) NOT NULL,
    [Member_Status]   VARCHAR (10) NULL,
    [Description]     VARCHAR (30) NULL,
    [LastUpdatedBy]   VARCHAR (40) CONSTRAINT [DF_dimMemberStatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME     CONSTRAINT [DF_dimMemberStatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]        BIT          CONSTRAINT [DF_dimMemberStatus_IsActive] DEFAULT ((1)) NULL,
    [StartDate]       DATETIME     CONSTRAINT [DF_dimMemberStatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]         DATETIME     CONSTRAINT [DF_dimMemberStatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [Complimentary]   BIT          DEFAULT ((0)) NULL,
    PRIMARY KEY CLUSTERED ([MemberStatusKey] ASC)
);

