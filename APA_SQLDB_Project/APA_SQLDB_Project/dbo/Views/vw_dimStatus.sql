CREATE VIEW vw_dimStatus AS
SELECT 
[StatusKey]
      ,[Status]
      ,[Gender]
      ,[Category]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
  FROM [rpt].[dimStatus]