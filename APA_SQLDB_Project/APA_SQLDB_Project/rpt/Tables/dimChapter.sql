CREATE TABLE [rpt].[dimChapter] (
    [ChapterKey]    INT            IDENTITY (1, 1) NOT NULL,
    [Chapter_Code]  VARCHAR (30)   NULL,
    [Chapter_Minor] VARCHAR (30)   NULL,
    [Chapter_Major] VARCHAR (30)   NULL,
    [Description]   NVARCHAR (MAX) NULL,
    [LastUpdatedBy] VARCHAR (40)   CONSTRAINT [DF_dimChapter_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME       CONSTRAINT [DF_dimChapter_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT            CONSTRAINT [DF_dimChapter_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME       CONSTRAINT [DF_dimChapter_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME       CONSTRAINT [DF_dimChapter_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ChapterKey] ASC)
);

