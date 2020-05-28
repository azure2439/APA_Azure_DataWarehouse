
 CREATE VIEW [dbo].[vw_dimMember] AS
 SELECT a.[MemberIDKey]
      ,a.[Parent]
      ,a.[Member_ID]
      ,a.[First_Name]
      ,a.[Middle_Name]
      ,a.[Last_Name]
      ,a.[Full_Name]
      ,a.[Email]
      ,a.[Home_Phone]
      ,a.[Mobile_Phone]
      ,a.[Work_Phone]
      ,a.[Designation]
      ,a.[Functional_Title]
      ,a.[Birth_Date]
      ,a.[JoinDate]
      ,a.[Cohort]
      ,A.[LastUpdatedBy]
      ,A.[LastModified]
      ,A.[StartDate]
      ,A.[EndDate]
      ,[Merged_Date]
      ,[Cohortquarter]
      ,[Cohort_MemberType]
	  ,A.[Isactive],
	   B.AICP_START
	   ,C.CO_ID
	   ,C.Date_Added
	   from rpt.dimMember A left join stg.imis_Ind_Demographics B on
A.Member_ID = B.ID
left join stg.imis_name C on A.member_id = c.id
where  b.IsActive = 1 and c.IsActive = 1
