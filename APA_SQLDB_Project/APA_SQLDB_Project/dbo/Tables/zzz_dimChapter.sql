CREATE TABLE [dbo].[zzz_dimChapter] (
    [ChapterKey]    INT            IDENTITY (1, 1) NOT NULL,
    [Chapter_Code]  VARCHAR (30)   NULL,
    [Chapter_Minor] VARCHAR (30)   NULL,
    [Chapter_Major] VARCHAR (30)   NULL,
    [Description]   NVARCHAR (MAX) NULL,
    [LastUpdatedBy] VARCHAR (40)   NULL,
    [LastModified]  DATETIME       NULL,
    [IsActive]      BIT            NULL,
    [StartDate]     DATETIME       NULL,
    [EndDate]       DATETIME       NULL
);

