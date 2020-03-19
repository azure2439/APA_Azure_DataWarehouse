CREATE VIEW vw_dimActivityCategory AS
SELECT 
[ActivityCategoryKey]
      ,[Activity_Category_Code]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
FROM [rpt].[dimActivityCategory]
