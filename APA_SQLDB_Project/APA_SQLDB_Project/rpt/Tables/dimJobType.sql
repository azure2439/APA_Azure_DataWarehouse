CREATE TABLE [rpt].[dimJobType] (
    [JobTypeKey]    INT           IDENTITY (1, 1) NOT NULL,
    [Job_Type]      VARCHAR (50)  NULL,
    [Title]         VARCHAR (100) NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [DF_dimJobType_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [DF_dimJobType_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT           CONSTRAINT [DF_dimJobType_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME      CONSTRAINT [DF_dimJobType_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME      CONSTRAINT [DF_dimJobType_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([JobTypeKey] ASC)
);

