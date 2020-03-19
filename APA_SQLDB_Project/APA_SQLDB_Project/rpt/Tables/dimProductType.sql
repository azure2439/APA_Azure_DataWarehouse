CREATE TABLE [rpt].[dimProductType] (
    [ProductTypeKey] INT          IDENTITY (1, 1) NOT NULL,
    [ParentID]       VARCHAR (30) NOT NULL,
    [Product_Type]   VARCHAR (30) NULL,
    [Description]    VARCHAR (40) NULL,
    [LastUpdatedBy]  VARCHAR (40) CONSTRAINT [DF_dimProductType_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME     CONSTRAINT [DF_dimProductType_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]       BIT          CONSTRAINT [DF_dimProductType_IsActive] DEFAULT ((1)) NULL,
    [StartDate]      DATETIME     CONSTRAINT [DF_dimProductType_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]        DATETIME     CONSTRAINT [DF_dimProductType_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ProductTypeKey] ASC)
);

