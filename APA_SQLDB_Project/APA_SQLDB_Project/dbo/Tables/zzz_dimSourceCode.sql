CREATE TABLE [dbo].[zzz_dimSourceCode] (
    [SourceCodeKey] INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]      VARCHAR (50)  NULL,
    [Source_Code]   VARCHAR (50)  NULL,
    [Description]   VARCHAR (255) NULL,
    [LastUpdatedBy] VARCHAR (40)  NULL,
    [LastModified]  DATETIME      NULL,
    [IsActive]      BIT           NULL,
    [StartDate]     DATETIME      NULL,
    [EndDate]       DATETIME      NULL
);

