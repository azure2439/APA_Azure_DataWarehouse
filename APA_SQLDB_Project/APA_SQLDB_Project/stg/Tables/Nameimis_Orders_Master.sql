﻿CREATE TABLE [stg].[Nameimis_Orders_Master] (
    [Order_Number]  DECIMAL (15, 2) NULL,
    [IsActive]      INT             NULL,
    [IsDeleted]     INT             NULL,
    [LastModified]  DATETIME2 (7)   NULL,
    [LastUpdatedBy] NVARCHAR (MAX)  NULL
);

