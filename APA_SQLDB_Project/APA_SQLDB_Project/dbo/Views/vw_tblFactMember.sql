﻿CREATE VIEW vw_tblFactMember AS
SELECT 
[MemberIDKey]
      ,[MemberTypeIDKey]
      ,[SalaryIDKey]
      ,[RaceIDKey]
      ,[OrgKey]
      ,[ChapterKey]
      ,[Trans_Date_Key]
      ,[Paid_Through_Key]
      ,[Home_Address_Key]
      ,[Work_Address_Key]
      ,[StatusKey]
      ,[LastUpdated]
      ,[LastModifiedBy]
      ,[MemberStatusKey]
  FROM [rpt].[tblFactMember]