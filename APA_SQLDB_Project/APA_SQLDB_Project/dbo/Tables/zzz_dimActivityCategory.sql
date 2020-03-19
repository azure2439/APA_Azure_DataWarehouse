CREATE TABLE [dbo].[zzz_dimActivityCategory] (
    [ActivityCategoryKey]    INT           IDENTITY (1, 1) NOT NULL,
    [Activity_Category_Code] VARCHAR (20)  NULL,
    [Description]            VARCHAR (255) NULL,
    [LastUpdatedBy]          VARCHAR (40)  NULL,
    [LastModified]           DATETIME      NULL,
    [IsActive]               BIT           NULL,
    [StartDate]              DATETIME      NULL,
    [EndDate]                DATETIME      NULL
);

