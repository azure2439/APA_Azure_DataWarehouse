


/*******************************************************************************************************************************
 Description:   This Stored Procedure incrementally merges new records from the temp tables into the dimension 
				tables. It sets no longer active records to "0" and merges the new ones with an IsCurrent value of "1"
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/



CREATE procedure [etl].[usp_IMIS_Dimension_Table_Merge] 
@PipelineName varchar(30) 

as

Begin

/**********************************************rpt.DimAddress*********************************************/
---add new addresses to the Address Dimension
DECLARE @Yesterday DATETIME = DATEADD(dd, - 1, GETDATE())
DECLARE @Today DATETIME = GETDATE()

IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
       Drop TABLE #audittemp

CREATE TABLE #audittemp (
       action NVARCHAR(20)
       ,inserted_id varchar(30)
       ,deleted_id varchar(30)
       );

DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @ErrorSeverity NVARCHAR(MAX)
DECLARE @ErrorState tinyint

BEGIN TRY

BEGIN TRAN



MERGE rpt.dimAddress AS DST
USING tmp.imis_Name_Address AS SRC
       ON DST.Address_Num = SRC.Address_Num
WHEN MATCHED
           AND DST.ISACTIVE = 1 
              AND (
                   
                     ISNULL(DST.PURPOSE, '') <> ISNULL(SRC.PURPOSE, '')
                     OR ISNULL(DST.COMPANY, '') <> ISNULL(SRC.COMPANY, '')
                     OR ISNULL(DST.FULLADDRESS, '') <> ISNULL(SRC.FULL_ADDRESS, '')
                     OR ISNULL(DST.STREET, '') <> ISNULL(SRC.ADDRESS_1, '')
                     OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
					 OR ISNULL(DST.[STATE], '') <> ISNULL(SRC.STATE_PROVINCE, '')
					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
					 OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
					 OR ISNULL(DST.COUNTY, '') <> ISNULL(SRC.COUNTY, '')
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [Address_Num]
      ,[Purpose]
      ,[Company]
      ,[FullAddress]
      ,[Street]
      ,[City]
      ,[State]
      ,[Country]
      ,[Zip]
      ,[County]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                    src.[Address_Num]
      ,src.[Purpose]
      ,src.[Company]
      ,src.[Full_Address]
      ,src.[Address_1]
      ,src.[City]
      ,src.[STATE_PROVINCE]
      ,src.[Country]
      ,src.[Zip]
      ,src.[County]
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.Address_Num
       ,deleted.Address_Num
INTO #audittemp;


INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Name_Address'       
       ,'rpt.dimAddress'
	          ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
		)
		,
		(select count(*) as RowsRead
		from tmp.imis_Name_Address
		)
		,getdate()
       )

INSERT INTO rpt.dimAddress (
[Address_Num]
      ,[Purpose]
      ,[Company]
      ,[FullAddress]
      ,[Street]
      ,[City]
      ,[State]
      ,[Country]
      ,[Zip]
      ,[County]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct  
                     [Address_Num]
      ,[Purpose]
      ,[Company]
      ,[Full_Address]
      ,[Address_1]
      ,[City]
      ,[STATE_PROVINCE]
      ,[Country]
      ,[Zip]
      ,[County]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Name_Address
WHERE Address_Num IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/***************************************rpt.DimChapter*******************************/
---add new Chapters to the Chapter Dimension
MERGE rpt.dimChapter AS DST
USING (select * from tmp.imis_Product where product_major = 'CHAPT') AS SRC
       ON DST.Chapter_Code = SRC.Product_Code 
WHEN MATCHED
           AND DST.ISACTIVE = 1 
              AND (
                   
                     ISNULL(DST.CHAPTER_MINOR, '') <> ISNULL(SRC.PRODUCT_MINOR, '')
                     OR ISNULL(DST.CHAPTER_MAJOR, '') <> ISNULL(SRC.PRODUCT_MAJOR, '')
                     --OR ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                   [Chapter_Code]
      ,[Chapter_Minor]
      ,[Chapter_Major]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
    
 
          
                     )
              VALUES (
                    SRC.[Product_Code]
      ,SRC.[Product_Minor]
      ,SRC.[Product_Major]
      ,SRC.[Description]
	  ,1
      ,@Today
                     )
			--WHERE SRC.Product_Major = 'CHAPT' 

OUTPUT $ACTION AS action
       ,inserted.Chapter_Code
       ,deleted.Chapter_code
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Product'
       ,'rpt.dimChapter'
              ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*) as Rowsread
		from tmp.imis_Product where product_major = 'CHAPT'
		)
       ,getdate()
       )

INSERT INTO rpt.dimChapter (
 [Chapter_Code]
      ,[Chapter_Minor]
      ,[Chapter_Major]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct 
                      [Product_Code]
      ,[Product_Minor]
      ,[Product_Major]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Product where product_major = 'CHAPT' and Product_Code IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/************************************************rpt.dimMember**********************************/
---add new members to the Member Dimension
MERGE rpt.dimMember AS DST
USING tmp.imis_Name AS SRC
       ON DST.Member_ID = SRC.ID
WHEN MATCHED
             AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.FIRST_NAME, '') <> ISNULL(SRC.FIRST_NAME, '')
                     OR ISNULL(DST.MIDDLE_NAME, '') <> ISNULL(SRC.MIDDLE_NAME, '')
                     OR ISNULL(DST.LAST_NAME, '') <> ISNULL(SRC.LAST_NAME, '')
                     OR ISNULL(DST.FULL_NAME, '') <> ISNULL(SRC.FULL_NAME, '')
					 OR ISNULL(DST.EMAIL, '') <> ISNULL(SRC.EMAIL, '')
					 OR ISNULL(DST.HOME_PHONE, '') <> ISNULL(SRC.HOME_PHONE, '')
					 OR ISNULL(DST.MOBILE_PHONE, '') <> ISNULL(SRC.MOBILE_PHONE, '')
					 OR ISNULL(DST.WORK_PHONE, '') <> ISNULL(SRC.WORK_PHONE, '')
					 OR ISNULL(DST.DESIGNATION, '') <> ISNULL(SRC.DESIGNATION, '')
					 OR ISNULL(DST.FUNCTIONAL_TITLE, '') <> ISNULL(SRC.FUNCTIONAL_TITLE, '')
					 OR ISNULL(DST.BIRTH_DATE, '') <> ISNULL(SRC.BIRTH_DATE, '')
					 OR ISNULL(DST.JOINDATE, '') <> ISNULL(SRC.JOIN_DATE, '')
					-- OR ISNULL(DST.COHORT, '') <> ISNULL(SRC.COHORT, '')
					-- OR ISNULL(DST.COHORTQUARTER, '') <> ISNULL(SRC.COHORTQUARTER, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
              
      [Parent]
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
      ,[IsActive]
      ,[StartDate]
	  ,[CohortQuarter]
	  ,[Cohort_MemberType]

 
          
                     )
              VALUES (
                
      SRC.[ID]
      ,SRC.[ID]
      ,SRC.[First_Name]
      ,SRC.[Middle_Name]
      ,SRC.[Last_Name]
      ,SRC.[Full_Name]
      ,SRC.[Email]
      ,SRC.[Home_Phone]
      ,SRC.[Mobile_Phone]
      ,SRC.[Work_Phone]
      ,SRC.[Designation]
      ,SRC.[Functional_Title]
      ,SRC.[Birth_Date]
      ,SRC.[Join_Date]
      ,'newcohort'
	  ,1
      ,@Today
	  ,'newcohortquarter'
	  ,'newcohortmemtype'
	
                     ) 



OUTPUT $ACTION AS action
       ,inserted.Member_ID
       ,deleted.Member_ID
INTO #audittemp;


--  Update Cohort values for the new members only
update A set A.cohort =
case when 
B.member_type = 'MEM' and MEMBER_STATUS = 'N' and CATEGORY = 'NM1' then (case when month(B.Join_Date) < 10 then year(join_date) when month(B.Join_Date) >= 10 then  year(join_date)+1 else '0' end)
when B.member_type = 'STU' and MEMBER_STATUS = 'N'  then (case when month(B.Join_Date) < 10 then year(join_date) when month(B.Join_Date) >= 10 then  year(join_date)+1 else '0' end)
else '0'
end
from rpt.dimmember A inner join tmp.imis_name B on
A.member_id = B.ID
where cohort = 'newcohort'


--  Update Cohort Quarter values for the new members only
update A set A.cohortquarter =
case when 
B.member_type = 'MEM' and MEMBER_STATUS = 'N' and CATEGORY = 'NM1' then (case when month(B.Join_Date) between 10 and 12 then 'Q1' when month(B.Join_Date) between 1 and 3 then 'Q2' when month(B.Join_Date) between 4 and 6 then 'Q3' when month(B.Join_Date) between 7 and 9 then 'Q4' else '0' end)
when B.member_type = 'STU' and MEMBER_STATUS = 'N'  then (case when month(B.Join_Date) between 10 and 12 then 'Q1' when month(B.Join_Date) between 1 and 3 then 'Q2' when month(B.Join_Date) between 4 and 6 then 'Q3' when month(B.Join_Date) between 7 and 9 then 'Q4' else '0' end)
else '0'
end
from rpt.dimmember A join tmp.imis_name B on
A.member_id = B.ID
where cohortquarter = 'newcohortquarter'

--  Update Cohort Member Type values for the new members only
update A set A.Cohort_MemberType =
case when 
B.member_type = 'MEM' and MEMBER_STATUS = 'N' and CATEGORY = 'NM1' then 'New Member'
when B.member_type = 'STU' and MEMBER_STATUS = 'N'  then 'Student'
else ''
end
from rpt.dimmember A join tmp.imis_name B on
A.member_id = B.ID
where Cohort_MemberType = 'newcohortmemtype'



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		 ,'tmp.imis_Name'
       ,'rpt.dimMember'
             ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*) as Rowsread
		from tmp.imis_Name
		) 
       ,getdate()
       )

;with cte_mem as
(select distinct Member_ID, Cohort, Cohortquarter, Cohort_MemberType from rpt.dimMember
where Member_ID in
(Select Member_ID from tmp.imis_Name)
)

INSERT INTO rpt.dimMember (
[Parent]
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
      ,[IsActive]
      ,[StartDate]
	  ,[Cohortquarter]
	  ,[Cohort_MemberType]

 
                 )
select distinct
      [ID]
      ,[ID]
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
      ,[Join_Date]
      ,B.[Cohort]
      ,1
      ,@Today
	  ,B.[Cohortquarter]
	  ,B.[Cohort_MemberType]
           
         
FROM 
tmp.imis_Name A inner join cte_mem B
on A.ID = B.Member_ID
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/**************************************************rpt.dimMemberType**********************************/
---add new member types to the Member Type Dimension
MERGE rpt.dimMemberType AS DST
USING tmp.imis_Member_Types AS SRC
       ON DST.Member_Type = SRC.Member_Type
WHEN MATCHED
             AND DST.ISACTIVE = 1 
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.MEMBER_RECORD, '') <> ISNULL(SRC.MEMBER_RECORD, '')
                     OR ISNULL(DST.COMPANY_RECORD, '') <> ISNULL(SRC.COMPANY_RECORD, '')
                     OR ISNULL(DST.DUES_CODE_1, '') <> ISNULL(SRC.DUES_CODE_1, '')
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [Member_Type]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
      ,[Member_Record]
      ,[Company_Record]
      ,[Dues_Code_1]
 
          
                     )
              VALUES (
                    src.[Member_Type]
      ,src.[Description]
	  ,1
      ,@Today
      ,src.[Member_Record]
      ,src.[Company_Record]
      ,src.[Dues_Code_1]
	
                     ) 

OUTPUT $ACTION AS action
       ,inserted.Member_Type
       ,deleted.Member_Type
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		 ,'tmp.imis_Member_Types'
       ,'rpt.dimMemberType'
             ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*) as Rowsread
		from tmp.imis_Member_Types
		) 
       ,getdate()
       )

INSERT INTO rpt.dimMemberType (
 [Member_Type]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
      ,[Member_Record]
      ,[Company_Record]
      ,[Dues_Code_1]
 
                 )
select distinct 
                      [Member_Type]
      ,[Description]
      ,1
      ,@Today
      ,[Member_Record]
      ,[Company_Record]
      ,[Dues_Code_1]
           
         
FROM 
tmp.imis_Member_Types
WHERE Member_Type IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/**************************************************rpt.DimOrg*************************************/
---add new organization types to the Org Dimension
MERGE rpt.dimOrg AS DST
USING (select * from tmp.imis_Gen_Tables where table_name = 'Org_Type') AS SRC
       ON DST.OrgCode = SRC.Code                                                         
WHEN MATCHED
              AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                 
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [OrgCode]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                   src.[Code]
      ,src.[Description]
	  ,1
      ,@Today
                     ) 
			                                                                                

OUTPUT $ACTION AS action
       ,inserted.OrgCode
       ,deleted.OrgCode
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		 ,'tmp.imis_Gen_Tables'
       ,'rpt.dimOrg'
             ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*) as Rowsread
		from tmp.imis_Gen_Tables where table_name = 'Org_Type'
		) 
       ,getdate()
       )

INSERT INTO rpt.dimOrg (
 [OrgCode]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct  
       [Code]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Gen_Tables where table_name = 'Org_Type' and
 Code IN (												
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp




/**************************************************rpt.DimRace********************************/
--MERGE rpt.dimRace AS DST
--USING (select A.code,A.description, B.code, B.description
--from 
--(select ROW_NUMBER() over (order by code, description) as rown, code, description from #Eth ) A
--cross join 
--(select ROW_NUMBER() over (order by code, description) as rown, code, DESCRIPTION  from #Ori) Bs) AS SRC
--       ON DST.RaceID = SRC.Code                                                          
--WHEN MATCHED
--		AND DST.ISACTIVE = 1
--              AND (
                   
--                     ISNULL(DST.PURPOSE, '') <> ISNULL(SRC.PURPOSE, '')
--                     OR ISNULL(DST.COMPANY, '') <> ISNULL(SRC.COMPANY, '')
--                     OR ISNULL(DST.FULLADDRESS, '') <> ISNULL(SRC.FULL_ADDRESS, '')
--                     OR ISNULL(DST.STREET, '') <> ISNULL(SRC.ADDRESS_1, '')
--                     OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
--					 OR ISNULL(DST.[STATE], '') <> ISNULL(SRC.STATE_PROVINCE, '')
--					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTY, '')
--					 OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
--					 OR ISNULL(DST.COUNTY, '') <> ISNULL(SRC.COUNTY, '')
						
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                    [RaceID]
--      ,[Ethnicity]
--      ,[Custom_Rollup]
--      ,[IsActive]
--      ,[StartDate]
--      ,[Origin]
--      ,[OriginDescription]
 
          
--                     )
--              VALUES (
--                    src.[Address_Num]
--      ,src.[Purpose]
--      ,src.[Company]
--      ,src.[Full_Address]
--      ,src.[Address_1]
--      ,src.[City]
--      ,src.[STATE_PROVINCE]
--      ,src.[Country]
--      ,src.[Zip]
--      ,src.[County]
--	  ,1
--      ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.Address_Num
--       ,deleted.Address_Num
--INTO #audittemp;



--INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
--VALUES (
--		@PipelineName
--       ,'rpt.dimRace'
--       ,'tmp.imis_Gen_Tables'
--       ,(
--              SELECT action_Count
--              FROM (
--                     SELECT action
--                           ,count(*) AS action_count
--                     FROM #audittemp
--                     WHERE action = 'INSERT'
--                     GROUP BY action
--                     ) X
--              )
--       ,(
--              SELECT action_Count
--              FROM (
--                     SELECT action
--                           ,count(*) AS action_count
--                     FROM #audittemp
--                     WHERE action = 'UPDATE'
--                     GROUP BY action
--                     ) X
--		,(select count(*)
--		from tmp.imis_Gen_Tables																
--		) X
--		)
--       ,getdate()
--       )

--INSERT INTO rpt.dimRace (
--[RaceID]
--      ,[Ethnicity]
--      ,[Custom_Rollup]
--      ,[IsActive]
--      ,[StartDate]
--      ,[Origin]
--      ,[OriginDescription]
 
--                 )
--select distinct  
--                     [Address_Num]
--      ,[Purpose]
--      ,[Company]
--      ,[Full_Address]
--      ,[Address_1]
--      ,[City]
--      ,[STATE_PROVINCE]
--      ,[Country]
--      ,[Zip]
--      ,[County]
--      ,1
--      ,@Today
           
         
--FROM 
--tmp.imis_Gen_Tables
--WHERE Address_Num IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )

--			  Truncate TABLE #audittemp


/**************************************************rpt.DimSalary***********************************/
---add new salaries to the Salary Dimension
MERGE rpt.dimSalary AS DST
USING (select * from tmp.imis_Gen_Tables where table_name = 'Salary_Range') AS SRC
       ON DST.SalaryID = SRC.Code													
WHEN MATCHED
              AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [SalaryID]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                    src.[Code]
      ,src.[Description]
	  ,1
      ,@Today
                     )																					

OUTPUT $ACTION AS action
       ,inserted.SalaryID
       ,deleted.SalaryID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Gen_Tables'
       ,'rpt.dimSalary'
              ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*)
		from tmp.imis_Gen_Tables where table_name = 'Salary_Range'												
		) 
       ,getdate()
       )

INSERT INTO rpt.dimSalary (
 [SalaryID]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct  
                  [Code]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Gen_Tables where table_name = 'Salary_Range' and																				
Code IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp




/*************************************************rpt.DimProductCode*************************************/
---add new product codes to the Product Code Dimension
MERGE rpt.dimProductCode AS DST
USING tmp.imis_Product AS SRC
       ON DST.Product_Code = SRC.Product_Code													
WHEN MATCHED
             AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.PRODUCT_MAJOR, '') <> ISNULL(SRC.PRODUCT_MAJOR, '')
				  OR ISNULL(DST.PRODUCT_MINOR, '') <> ISNULL(SRC.PRODUCT_MINOR, '')
				  OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')	
				  OR ISNULL(DST.DESCRIPTION, '') <> ISNULL(SRC.DESCRIPTION, '')	
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                   [ParentID]
      ,[Product_Code]
      ,[Product_Major]
      ,[Product_Minor]
      ,[Title]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                   SRC.[Product_Code]
      ,SRC.[Product_Code]
      ,SRC.[Product_Major]
      ,SRC.[Product_Minor]
      ,SRC.[Title]
      ,SRC.[Description]
	  ,1
      ,@Today
                     )																					

OUTPUT $ACTION AS action
       ,inserted.Product_Code
       ,deleted.Product_Code
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Product'
       ,'rpt.dimProductCode'
              ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*)
		from tmp.imis_Product											
		) 
       ,getdate()
       )

INSERT INTO rpt.dimProductCode (
 [ParentID]
      ,[Product_Code]
      ,[Product_Major]
      ,[Product_Minor]
      ,[Title]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct 
                [Product_Code]
      ,[Product_Code]
      ,[Product_Major]
      ,[Product_Minor]
      ,[Title]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Product 
where Product_Code IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*************************************************rpt.DimProductType*************************************/
---add new product types to the Product Type Dimension
MERGE rpt.dimProductType AS DST
USING tmp.imis_Product_Type AS SRC
       ON DST.Product_Type = SRC.Prod_Type												
WHEN MATCHED
			AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.Description, '') <> ISNULL(SRC.description, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                  [ParentID]
      ,[Product_Type]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                  src.[Prod_Type]
      ,src.[Prod_Type]
      ,src.[Description]
	  ,1
      ,@Today
                     )																					

OUTPUT $ACTION AS action
       ,inserted.Product_Type
       ,deleted.Product_Type
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Product_Type'
       ,'rpt.dimProductType'
              ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*)
		from tmp.imis_Product_Type											
		) 
       ,getdate()
       )

INSERT INTO rpt.dimProductType (
[ParentID]
      ,[Product_Type]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct 
               [Prod_Type]
      ,[Prod_Type]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Product_Type 
where Prod_Type IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/*************************************************rpt.DimSourceCode*************************************/
---add new source codes to the Source Code Dimension
MERGE rpt.dimSourceCode AS DST
USING (select * from tmp.imis_Gen_Tables where table_name = 'Source_Code') AS SRC
       ON DST.Source_Code = SRC.Code													
WHEN MATCHED
			AND DST.ISACTIVE = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
						
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ParentID]
	  ,[Source_Code]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                   [Code]
	  ,[Code]
      ,[Description]
	  ,1
      ,@Today
                     )																					

OUTPUT $ACTION AS action
       ,inserted.Source_Code
       ,deleted.Source_Code
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		@PipelineName
		,'tmp.imis_Gen_Tables'
       ,'rpt.dimSourceCode'
              ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
		,(select count(*)
		from tmp.imis_Gen_Tables where table_name = 'Source_Code'												
		) 
       ,getdate()
       )

INSERT INTO rpt.dimSourceCode (
 [ParentID]
	  ,[Source_Code]
      ,[Description]
      ,[IsActive]
      ,[StartDate]
 
                 )
select distinct 
                 [Code]
	  ,[Code]
      ,[Description]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Gen_Tables where table_name = 'Source_Code' and																				
Code IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/**************************************************table**********************/
COMMIT 
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
	ROLLBACK
	END
SET @ErrorMessage  = ERROR_MESSAGE();
    SET @ErrorSeverity = ERROR_SEVERITY();
    SET @ErrorState    = ERROR_STATE();
    THROW;
END CATCH;
END

