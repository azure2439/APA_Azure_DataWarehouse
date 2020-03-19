CREATE VIEW vw_dimMemberType AS
SELECT 
[MemberTypeIDKey]
      ,[Member_Type]
      ,[Description]
      ,[LastUpdatedBy]
      ,[LastModified]
      ,[IsActive]
      ,[StartDate]
      ,[EndDate]
      ,[Member_Record]
      ,[Company_Record]
      ,[Dues_Code_1]
  FROM [rpt].[dimMemberType]
