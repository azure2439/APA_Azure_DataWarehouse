
CREATE VIEW [dbo].[vw_dimMember] AS
Select [MemberIDKey]
      ,[Parent]
      ,[Member_ID]
      ,[First_Name]
      ,[Middle_Name]
      ,[Last_Name]
      ,[Full_Name]
      ,[Email]
      ,[Home_Phone]
      ,[Mobile_Phone]
      ,[Work_Phone]
      ,[Designation]
      ,[Functional_Title]
      ,[Birth_Date]
      ,[JoinDate]
      ,[Cohort]
      ,[Merged_Date]
      ,[Cohortquarter], B.AICP_START from rpt.dimMember A left join stg.imis_Ind_Demographics B on
A.Member_ID = B.ID
where  B.IsActive = 1
