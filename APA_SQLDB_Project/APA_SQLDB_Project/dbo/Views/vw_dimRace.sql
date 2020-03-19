CREATE VIEW vw_dimRace AS
SELECT 
[RaceIDKey]
      ,[RaceID]
      ,[Ethnicity]
      ,[Custom_Rollup]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
      ,[Origin]
      ,[OriginDescription]
  FROM [rpt].[dimRace]