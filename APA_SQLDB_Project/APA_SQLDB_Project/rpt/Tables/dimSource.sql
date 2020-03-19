CREATE TABLE [rpt].[dimSource] (
    [SourceKey]          INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]           VARCHAR (10)  NOT NULL,
    [Source_System_Code] VARCHAR (10)  NULL,
    [Description]        VARCHAR (255) NULL,
    [LastUpdatedBy]      VARCHAR (40)  CONSTRAINT [DF_dimSource_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME      CONSTRAINT [DF_dimSource_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT           CONSTRAINT [DF_dimSource_IsActive] DEFAULT ((1)) NULL,
    [StartDate]          DATETIME      CONSTRAINT [DF_dimSource_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME      CONSTRAINT [DF_dimSource_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([SourceKey] ASC)
);

