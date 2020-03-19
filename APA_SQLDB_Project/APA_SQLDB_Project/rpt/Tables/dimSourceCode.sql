CREATE TABLE [rpt].[dimSourceCode] (
    [SourceCodeKey] INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]      VARCHAR (50)  NULL,
    [Source_Code]   VARCHAR (50)  NULL,
    [Description]   VARCHAR (255) NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [DF_dimSourceCode_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [DF_dimSourceCode_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT           CONSTRAINT [DF_dimSourceCode_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME      CONSTRAINT [DF_dimSourceCode_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME      CONSTRAINT [DF_dimSourceCode_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([SourceCodeKey] ASC)
);

