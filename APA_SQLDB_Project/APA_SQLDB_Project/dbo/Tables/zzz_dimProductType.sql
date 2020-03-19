CREATE TABLE [dbo].[zzz_dimProductType] (
    [ProductTypeKey] INT          IDENTITY (1, 1) NOT NULL,
    [ParentID]       VARCHAR (30) NOT NULL,
    [Product_Type]   VARCHAR (30) NULL,
    [Description]    VARCHAR (40) NULL,
    [LastUpdatedBy]  VARCHAR (40) NULL,
    [LastModified]   DATETIME     NULL,
    [IsActive]       BIT          NULL,
    [StartDate]      DATETIME     NULL,
    [EndDate]        DATETIME     NULL
);

