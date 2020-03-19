CREATE TABLE [rpt].[dimMemberType] (
    [MemberTypeIDKey] INT           IDENTITY (1, 1) NOT NULL,
    [Member_Type]     VARCHAR (5)   NULL,
    [Description]     VARCHAR (255) NULL,
    [LastUpdatedBy]   VARCHAR (40)  CONSTRAINT [DF_dimMemberType_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME      CONSTRAINT [DF_dimMemberType_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]        BIT           CONSTRAINT [DF_dimMemberType_IsActive] DEFAULT ((1)) NULL,
    [StartDate]       DATETIME      CONSTRAINT [DF_dimMemberType_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]         DATETIME      CONSTRAINT [DF_dimMemberType_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [Member_Record]   BIT           NULL,
    [Company_Record]  BIT           NULL,
    [Dues_Code_1]     VARCHAR (40)  NULL,
    PRIMARY KEY CLUSTERED ([MemberTypeIDKey] ASC)
);

