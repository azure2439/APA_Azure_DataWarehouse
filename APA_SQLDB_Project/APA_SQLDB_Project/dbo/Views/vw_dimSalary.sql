CREATE VIEW vw_dimSalary AS
SELECT 
[SalaryIDKey]
      ,[SalaryID]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
  FROM [rpt].[dimSalary]