CREATE TABLE [dbo].[zzz_dimProductCode] (
    [ProductCodeKey] INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]       VARCHAR (31)  NOT NULL,
    [Product_Code]   VARCHAR (31)  NULL,
    [Product_Major]  VARCHAR (15)  NULL,
    [Product_Minor]  VARCHAR (15)  NULL,
    [Title]          VARCHAR (60)  NULL,
    [Description]    VARCHAR (MAX) NULL,
    [LastUpdatedBy]  VARCHAR (40)  NULL,
    [LastModified]   DATETIME      NULL,
    [IsActive]       BIT           NULL,
    [StartDate]      DATETIME      NULL,
    [EndDate]        DATETIME      NULL
);

