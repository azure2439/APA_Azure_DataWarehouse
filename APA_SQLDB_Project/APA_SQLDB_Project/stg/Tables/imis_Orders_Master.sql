CREATE TABLE [stg].[imis_Orders_Master] (
    [Order_Number]  NUMERIC (15, 2) NULL,
    [IsActive]      BIT             NULL,
    [IsDeleted]     BIT             NULL,
    [LastModified]  DATETIME        DEFAULT (getdate()) NULL,
    [LastUpdatedBy] VARCHAR (60)    DEFAULT (suser_sname()) NULL
);

