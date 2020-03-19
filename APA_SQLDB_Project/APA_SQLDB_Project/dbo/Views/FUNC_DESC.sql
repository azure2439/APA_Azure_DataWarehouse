/****** Script for SelectTopNRows command from SSMS  ******/
CREATE VIEW FUNC_DESC AS
SELECT [TABLE_NAME]
      ,[CODE]
      ,[SUBSTITUTE]
      ,[UPPER_CODE]
      ,[DESCRIPTION]
      ,[OBSOLETE_DESCRIPTION]
      ,[NCODE]
      ,[TIME_STAMP]
      ,[Id_Identity]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
  FROM [stg].[imis_Gen_Tables]
  WHERE TABLE_NAME='FUNC_TITLE'