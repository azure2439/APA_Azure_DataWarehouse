CREATE TABLE [rpt].[dimMember_Status] (
    [MemberStatus_Key] INT          IDENTITY (1, 1) NOT NULL,
    [Member_Status]    VARCHAR (10) NULL,
    [StartDate]        DATETIME     CONSTRAINT [DF_dimmemstatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]          DATETIME     CONSTRAINT [DF_dimmemstatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [LastUpdatedBy]    VARCHAR (40) CONSTRAINT [DF_dimmemstatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]     DATETIME     CONSTRAINT [DF_dimmemstatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]         BIT          CONSTRAINT [DF_dimmemstatus_IsActive] DEFAULT ((1)) NULL,
    PRIMARY KEY CLUSTERED ([MemberStatus_Key] ASC)
);

