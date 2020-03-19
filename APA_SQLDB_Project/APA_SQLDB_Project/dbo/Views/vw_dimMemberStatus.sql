/****** Script for SelectTopNRows command from SSMS  ******/
CREATE VIEW vw_dimMemberStatus AS
SELECT [MemberStatusKey]
      ,[Member_Status]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
      ,[Complimentary]
  FROM [rpt].[dimMemberStatus]