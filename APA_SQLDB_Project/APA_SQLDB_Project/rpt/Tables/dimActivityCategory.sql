CREATE TABLE [rpt].[dimActivityCategory] (
    [ActivityCategoryKey]    INT           IDENTITY (1, 1) NOT NULL,
    [Activity_Category_Code] VARCHAR (20)  NULL,
    [Description]            VARCHAR (255) NULL,
    [LastUpdatedBy]          VARCHAR (40)  CONSTRAINT [DF_dimActivityCategory_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]           DATETIME      CONSTRAINT [DF_dimActivityCategory_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]               BIT           CONSTRAINT [DF_dimActivityCategory_IsActive] DEFAULT ((1)) NULL,
    [StartDate]              DATETIME      CONSTRAINT [DF_dimActivityCategory_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                DATETIME      CONSTRAINT [DF_dimActivityCategory_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ActivityCategoryKey] ASC)
);

