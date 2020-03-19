CREATE TABLE [rpt].[dimProductCode] (
    [ProductCodeKey] INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]       VARCHAR (31)  NOT NULL,
    [Product_Code]   VARCHAR (31)  NULL,
    [Product_Major]  VARCHAR (15)  NULL,
    [Product_Minor]  VARCHAR (15)  NULL,
    [Title]          VARCHAR (60)  NULL,
    [Description]    VARCHAR (MAX) NULL,
    [LastUpdatedBy]  VARCHAR (40)  CONSTRAINT [DF_dimProductCode_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME      CONSTRAINT [DF_dimProductCode_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]       BIT           CONSTRAINT [DF_dimProductCode_IsActive] DEFAULT ((1)) NULL,
    [StartDate]      DATETIME      CONSTRAINT [DF_dimProductCode_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]        DATETIME      CONSTRAINT [DF_dimProductCode_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ProductCodeKey] ASC)
);

