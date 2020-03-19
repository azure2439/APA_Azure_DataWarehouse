CREATE TABLE [dbo].[zzz_dimSource] (
    [SourceKey]          INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]           VARCHAR (10)  NOT NULL,
    [Source_System_Code] VARCHAR (10)  NULL,
    [Description]        VARCHAR (255) NULL,
    [LastUpdatedBy]      VARCHAR (40)  NULL,
    [LastModified]       DATETIME      NULL,
    [IsActive]           BIT           NULL,
    [StartDate]          DATETIME      NULL,
    [EndDate]            DATETIME      NULL
);

