CREATE VIEW vw_dimProductType AS
SELECT 
       [ProductTypeKey]
      ,[ParentID]
      ,[Product_Type]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
FROM [rpt].[dimProductType]