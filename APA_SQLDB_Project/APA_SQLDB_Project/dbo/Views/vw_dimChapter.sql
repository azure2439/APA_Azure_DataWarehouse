CREATE VIEW vw_dimChapter AS
SELECT 
[ChapterKey]
      ,[Chapter_Code]
      ,[Chapter_Minor]
      ,[Chapter_Major]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
FROM [rpt].[dimChapter]

