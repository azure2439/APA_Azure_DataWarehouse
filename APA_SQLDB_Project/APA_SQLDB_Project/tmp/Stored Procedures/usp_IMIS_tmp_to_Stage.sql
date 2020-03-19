

CREATE procedure [tmp].[usp_IMIS_tmp_to_Stage]
@PipelineName varchar(60) = 'ssms'
as

/*************************************************CUSTOM DEGREE********************************************************/

DECLARE @Yesterday DATETIME = DATEADD(dd, - 1, GETDATE())
DECLARE @Today DATETIME = GETDATE()

IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
       Truncate TABLE #audittemp

CREATE TABLE #audittemp (
       action NVARCHAR(20)
       ,inserted_id varchar(80)
       ,deleted_id varchar(80)
       );

DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @ErrorSeverity NVARCHAR(MAX)

BEGIN TRY
DECLARE @ErrorState tinyint

MERGE stg.imis_Custom_Degree AS DST
USING tmp.imis_Custom_Degree AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.SCHOOL_ID, '') <> ISNULL(SRC.SCHOOL_ID, '')
                     OR ISNULL(DST.SCHOOL_OTHER, '') <> ISNULL(SRC.SCHOOL_OTHER, '')
                     OR ISNULL(DST.DEGREE_LEVEL, '') <> ISNULL(SRC.DEGREE_LEVEL, '')
                     OR ISNULL(DST.DEGREE_DATE, '') <> ISNULL(SRC.DEGREE_DATE, '')
                     OR ISNULL(DST.DEGREE_PLANNING, '') <> ISNULL(SRC.DEGREE_PLANNING, '')
                     OR ISNULL(DST.DEGREE_PROGRAM, '') <> ISNULL(SRC.DEGREE_PROGRAM, '')
                     OR ISNULL(DST.IS_CURRENT, '') <> ISNULL(SRC.IS_CURRENT, '')
                     OR ISNULL(DST.DEGREE_LEVEL_OTHER, '') <> ISNULL(SRC.DEGREE_LEVEL_OTHER, '')
                     
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     ID
           ,SEQN
           ,SCHOOL_ID
           ,SCHOOL_OTHER
           ,DEGREE_LEVEL
           ,DEGREE_DATE
           ,DEGREE_PLANNING
           ,MAJOR
           ,ACCREDITED_PROGRAM
           ,DEGREE_COMPLETE
           ,SCHOOL_STUDENT_ID
           ,ALL_SCHOOLS
           ,ACCRED_SCHOOLS
           ,DEGREE_PROGRAM
           ,IS_CURRENT
           ,SCHOOL_SEQN
           ,DEGREE_LEVEL_OTHER
           ,TIME_STAMP
           ,IsActive 
           ,StartDate 
          
                     )
              VALUES (
                     SRC.ID
           ,SRC.SEQN
           ,SRC.SCHOOL_ID
           ,SRC.SCHOOL_OTHER
           ,SRC.DEGREE_LEVEL
           ,SRC.DEGREE_DATE
           ,SRC.DEGREE_PLANNING
           ,SRC.MAJOR
           ,SRC.ACCREDITED_PROGRAM
           ,SRC.DEGREE_COMPLETE
           ,SRC.SCHOOL_STUDENT_ID
           ,SRC.ALL_SCHOOLS
           ,SRC.ACCRED_SCHOOLS
           ,SRC.DEGREE_PROGRAM
           ,SRC.IS_CURRENT
           ,SRC.SCHOOL_SEQN
           ,SRC.DEGREE_LEVEL_OTHER
           ,SRC.TIME_STAMP
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
        '@PipelineName'
	   ,'tmp.imis_Custom_Degree'
       ,'stg.imis_Custom_Degree'
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
			 ,(select count(*) as RowsRead
				from tmp.imis_Custom_Degree
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_Degree (
ID
           ,SEQN
           ,SCHOOL_ID
           ,SCHOOL_OTHER
           ,DEGREE_LEVEL
           ,DEGREE_DATE
           ,DEGREE_PLANNING
           ,MAJOR
           ,ACCREDITED_PROGRAM
           ,DEGREE_COMPLETE
           ,SCHOOL_STUDENT_ID
           ,ALL_SCHOOLS
           ,ACCRED_SCHOOLS
           ,DEGREE_PROGRAM
           ,IS_CURRENT
           ,SCHOOL_SEQN
           ,DEGREE_LEVEL_OTHER
           ,TIME_STAMP
           ,IsActive 
           ,StartDate 
                 )
select  
                     ID
                 ,SEQN
           ,SCHOOL_ID
           ,SCHOOL_OTHER
           ,DEGREE_LEVEL
           ,DEGREE_DATE
           ,DEGREE_PLANNING
           ,MAJOR
           ,ACCREDITED_PROGRAM
           ,DEGREE_COMPLETE
           ,SCHOOL_STUDENT_ID
           ,ALL_SCHOOLS
           ,ACCRED_SCHOOLS
           ,DEGREE_PROGRAM
           ,IS_CURRENT
           ,SCHOOL_SEQN
           ,DEGREE_LEVEL_OTHER
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Custom_Degree
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp


/********************************************************ACAD_Program_Details********************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_ACAD_Program_Details AS DST
USING tmp.imis_ACAD_Program_Details AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.[PROGRAM_NAME], '') <> ISNULL(SRC.[PROGRAM_NAME], '')
                     OR ISNULL(DST.CHAIR_IMIS_ID, '') <> ISNULL(SRC.CHAIR_IMIS_ID, '')
                     OR ISNULL(DST.CHAIR_TITLE, '') <> ISNULL(SRC.CHAIR_TITLE, '')
                     OR ISNULL(DST.FSTU_COUNT, '') <> ISNULL(SRC.FSTU_COUNT, '')
                     OR ISNULL(DST.ACAD_PROGRAM_TYPE, '') <> ISNULL(SRC.ACAD_PROGRAM_TYPE, '')
                     OR ISNULL(DST.FSTU_COORDINATOR_ID, '') <> ISNULL(SRC.FSTU_COORDINATOR_ID, '')
                     OR ISNULL(DST.SCH_PAYING_AICP, '') <> ISNULL(SRC.SCH_PAYING_AICP, '')
                     OR ISNULL(DST.SIGNUP_DATE, '') <> ISNULL(SRC.SIGNUP_DATE, '')
					 OR ISNULL(DST.ACTIVE_ACAD_MEMBERSHIP, '') <> ISNULL(SRC.ACTIVE_ACAD_MEMBERSHIP, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     ID
      ,[PROGRAM_NAME]
      ,CHAIR_IMIS_ID
      ,CHAIR_TITLE
      ,FSTU_COUNT
      ,ACAD_PROGRAM_TYPE
      ,FSTU_COORDINATOR_ID
      ,SCH_PAYING_AICP
      ,SIGNUP_DATE
      ,ACTIVE_ACAD_MEMBERSHIP
      ,TIME_STAMP
      ,IsActive
      ,StartDate

                     )

              VALUES (
                     SRC.ID
      ,SRC.[PROGRAM_NAME]
      ,SRC.CHAIR_IMIS_ID
      ,SRC.CHAIR_TITLE
      ,SRC.FSTU_COUNT
      ,SRC.ACAD_PROGRAM_TYPE
      ,SRC.FSTU_COORDINATOR_ID
      ,SRC.SCH_PAYING_AICP
      ,SRC.SIGNUP_DATE
      ,SRC.ACTIVE_ACAD_MEMBERSHIP
      ,SRC.TIME_STAMP
      ,1
	  ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_ACAD_Program_Details'
       ,'stg.imis_ACAD_Program_Details'
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
					 ,(select count(*) as RowsRead
				from tmp.imis_ACAD_Program_Details
				) 
       ,getdate()
       )

INSERT INTO stg.imis_ACAD_Program_Details (
ID
      ,[PROGRAM_NAME]
      ,CHAIR_IMIS_ID
      ,CHAIR_TITLE
      ,FSTU_COUNT
      ,ACAD_PROGRAM_TYPE
      ,FSTU_COORDINATOR_ID
      ,SCH_PAYING_AICP
      ,SIGNUP_DATE
      ,ACTIVE_ACAD_MEMBERSHIP
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                    ID
      ,[PROGRAM_NAME]
      ,CHAIR_IMIS_ID
      ,CHAIR_TITLE
      ,FSTU_COUNT
      ,ACAD_PROGRAM_TYPE
      ,FSTU_COORDINATOR_ID
      ,SCH_PAYING_AICP
      ,SIGNUP_DATE
      ,ACTIVE_ACAD_MEMBERSHIP
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_ACAD_Program_Details
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*********************************************************ADVOCACY*********************************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Advocacy AS DST
USING tmp.imis_Advocacy AS SRC
       ON DST.ID = SRC.ID 

WHEN MATCHED
            AND IsActive = 1
              AND (
						ISNULL(DST.Congressional_District, '') <> ISNULL(SRC.Congressional_District, '')
					 OR ISNULL(DST.St_House, '') <> ISNULL(SRC.St_House, '')
					 OR ISNULL(DST.St_Senate, '') <> ISNULL(SRC.St_Senate, '')
					 OR ISNULL(DST.State_Chair, '') <> ISNULL(SRC.State_Chair, '')
					 OR ISNULL(DST.District_Captain, '') <> ISNULL(SRC.District_Captain, '')
					 OR ISNULL(DST.Transportation, '') <> ISNULL(SRC.Transportation, '')
					 OR ISNULL(DST.Community_Development, '') <> ISNULL(SRC.Community_Development, '')
					 OR ISNULL(DST.Federal_Data, '') <> ISNULL(SRC.Federal_Data, '')
					 OR ISNULL(DST.Water, '') <> ISNULL(SRC.Water, '')
					 OR ISNULL(DST.Other, '') <> ISNULL(SRC.Other, '')
                     OR ISNULL(DST.GrassRootsMember, '') <> ISNULL(SRC.GrassRootsMember, '')
                     OR ISNULL(DST.Join_Date, '') <> ISNULL(SRC.Join_Date, '')
                    
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
              ID
             ,Congressional_District
             ,St_House
             ,St_Senate
             ,State_Chair
             ,District_Captain
			 ,Transportation
			 ,Community_Development
			 ,Federal_Data
			 ,Water
			 ,Other
			 ,GrassRootsMember
			 ,Join_Date
			 ,TIME_STAMP
			 ,IsActive
			 ,StartDate
		 
                     )

           VALUES (
            SRC.ID
           ,SRC.Congressional_District
           ,SRC.St_House
           ,SRC.St_Senate
           ,SRC.State_Chair
           ,SRC.District_Captain
	       ,SRC.Transportation
		   ,SRC.Community_Development
		   ,SRC.Federal_Data
		   ,SRC.Water
		   ,SRC.Other
		   ,SRC.GrassRootsMember
		   ,SRC.Join_Date
		   ,SRC.TIME_STAMP
           ,1
           ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Advocacy'
       ,'stg.imis_Advocacy'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Advocacy
				)
       ,getdate()
       )

INSERT INTO stg.imis_Advocacy (
		ID
             ,Congressional_District
             ,St_House
             ,St_Senate
             ,State_Chair
             ,District_Captain
			 ,Transportation
			 ,Community_Development
			 ,Federal_Data
			 ,Water
			 ,Other
			 ,GrassRootsMember
			 ,Join_Date
			 ,TIME_STAMP
			 ,IsActive
			 ,StartDate 
                 )
select  
            ID
             ,Congressional_District
             ,St_House
             ,St_Senate
             ,State_Chair
             ,District_Captain
			 ,Transportation
			 ,Community_Development
			 ,Federal_Data
			 ,Water
			 ,Other
			 ,GrassRootsMember
			 ,Join_Date
             ,cast(TIME_STAMP as bigint)
             ,1
             ,@Today
           
         
FROM 
tmp.imis_Advocacy
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp
/********************************************************BDR_AuthNet_Temp***********************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_BDR_AuthNet_Temp AS DST
USING tmp.imis_BDR_AuthNet_Temp AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.PROFILE_KEY, '') <> ISNULL(SRC.PROFILE_KEY, '')
                     OR ISNULL(DST.PAYMENT_KEY, '') <> ISNULL(SRC.PAYMENT_KEY, '')
   
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,Profile_Key
      ,Payment_Key
      ,TIME_STAMP
      ,IsActive
      ,StartDate

                     )
              VALUES (
                     SRC.ID
      ,SRC.Profile_Key
      ,SRC.Payment_Key
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imiS_BDR_AuthNet_Temp'
       ,'stg.imis_BDR_AuthNet_Temp'
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
		,(select count(*) as RowsRead
				from tmp.imis_BDR_AuthNet_Temp
				) 
       ,getdate()
       )

INSERT INTO stg.imis_BDR_AuthNet_Temp (
ID
      ,Profile_Key
      ,Payment_Key
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                    ID
      ,Profile_Key
      ,Payment_Key
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_BDR_AuthNet_Temp
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/********************************************************Custom_AICP_Exam_Score*******************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Custom_AICP_Exam_Score AS DST
USING tmp.imis_Custom_AICP_Exam_Score AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.EXAM_CODE, '') <> ISNULL(SRC.EXAM_CODE, '')
                     OR ISNULL(DST.EXAM_DATE, '') <> ISNULL(SRC.EXAM_DATE, '')
                     OR ISNULL(DST.PASS, '') <> ISNULL(SRC.PASS, '')
                     OR ISNULL(DST.SCALED_SCORE, '') <> ISNULL(SRC.SCALED_SCORE, '')
                     OR ISNULL(DST.RAW_SCORE, '') <> ISNULL(SRC.RAW_SCORE, '')
                     OR ISNULL(DST.SCORE_1, '') <> ISNULL(SRC.SCORE_1, '')
					 OR ISNULL(DST.SCORE_2, '') <> ISNULL(SRC.SCORE_2, '')
					 OR ISNULL(DST.SCORE_3, '') <> ISNULL(SRC.SCORE_3, '')
					 OR ISNULL(DST.SCORE_4, '') <> ISNULL(SRC.SCORE_4, '')
					 OR ISNULL(DST.SCORE_5, '') <> ISNULL(SRC.SCORE_5, '')
					 OR ISNULL(DST.SCORE_6, '') <> ISNULL(SRC.SCORE_6, '')
					 OR ISNULL(DST.SCORE_7, '') <> ISNULL(SRC.SCORE_7, '')
					 OR ISNULL(DST.SCORE_8, '') <> ISNULL(SRC.SCORE_8, '')
                     OR ISNULL(DST.TESTFORM_CODE, '') <> ISNULL(SRC.TESTFORM_CODE, '')
                     OR ISNULL(DST.UPDATED_ON, '') <> ISNULL(SRC.UPDATED_ON, '')
					 OR ISNULL(DST.TEST_CENTER, '') <> ISNULL(SRC.TEST_CENTER, '')
					 OR ISNULL(DST.REGISTRANT_TYPE, '') <> ISNULL(SRC.REGISTRANT_TYPE, '')
                     OR ISNULL(DST.[FILE_NAME], '') <> ISNULL(SRC.[FILE_NAME], '')
					 OR ISNULL(DST.GEE_ELIGIBILITY_ID, '') <> ISNULL(SRC.GEE_ELIGIBILITY_ID, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    ID
      ,SEQN
      ,EXAM_CODE
      ,EXAM_DATE
      ,PASS
      ,SCALED_SCORE
      ,RAW_SCORE
      ,SCORE_1
      ,SCORE_2
      ,SCORE_3
      ,SCORE_4
      ,SCORE_5
      ,SCORE_6
      ,SCORE_7
      ,SCORE_8
      ,TESTFORM_CODE
      ,UPDATED_ON
      ,TEST_CENTER
      ,REGISTRANT_TYPE
      ,[FILE_NAME]
      ,GEE_ELIGIBILITY_ID
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                     )
              VALUES (
       SRC.ID
      ,SRC.SEQN
      ,SRC.EXAM_CODE
      ,SRC.EXAM_DATE
      ,SRC.PASS
      ,SRC.SCALED_SCORE
      ,SRC.RAW_SCORE
      ,SRC.SCORE_1
      ,SRC.SCORE_2
      ,SRC.SCORE_3
      ,SRC.SCORE_4
      ,SRC.SCORE_5
      ,SRC.SCORE_6
      ,SRC.SCORE_7
      ,SRC.SCORE_8
      ,SRC.TESTFORM_CODE
      ,SRC.UPDATED_ON
      ,SRC.TEST_CENTER
      ,SRC.REGISTRANT_TYPE
      ,SRC.[FILE_NAME]
      ,SRC.GEE_ELIGIBILITY_ID
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                 ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Custom_AICP_Exam_Score'
       ,'stg.imis_Custom_AICP_Exam_Score'
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
		,(select count(*) as RowsRead
				from tmp.imis_Custom_AICP_Exam_Score
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_AICP_Exam_Score (
 ID
      ,SEQN
      ,EXAM_CODE
      ,EXAM_DATE
      ,PASS
      ,SCALED_SCORE
      ,RAW_SCORE
      ,SCORE_1
      ,SCORE_2
      ,SCORE_3
      ,SCORE_4
      ,SCORE_5
      ,SCORE_6
      ,SCORE_7
      ,SCORE_8
      ,TESTFORM_CODE
      ,UPDATED_ON
      ,TEST_CENTER
      ,REGISTRANT_TYPE
      ,[FILE_NAME]
      ,GEE_ELIGIBILITY_ID
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,SEQN
      ,EXAM_CODE
      ,EXAM_DATE
      ,PASS
      ,SCALED_SCORE
      ,RAW_SCORE
      ,SCORE_1
      ,SCORE_2
      ,SCORE_3
      ,SCORE_4
      ,SCORE_5
      ,SCORE_6
      ,SCORE_7
      ,SCORE_8
      ,TESTFORM_CODE
      ,UPDATED_ON
      ,TEST_CENTER
      ,REGISTRANT_TYPE
      ,[FILE_NAME]
      ,GEE_ELIGIBILITY_ID
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Custom_AICP_Exam_Score
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/********************************************************CUSTOM_CREDIT****************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Custom_Credit AS DST
USING tmp.imis_Custom_Credit AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.CREDIT_STATUS, '') <> ISNULL(SRC.CREDIT_STATUS, '')
                     OR ISNULL(DST.CREDIT_PERIOD, '') <> ISNULL(SRC.CREDIT_PERIOD, '')
                     OR ISNULL(DST.CREDIT_NUMBER, '') <> ISNULL(SRC.CREDIT_NUMBER, '')
                     OR ISNULL(DST.CREDIT_NOTES, '') <> ISNULL(SRC.CREDIT_NOTES, '')
                     OR ISNULL(DST.CREDIT_REQUIRED_ADJUST, '') <> ISNULL(SRC.CREDIT_REQUIRED_ADJUST, '')
                     OR ISNULL(DST.CREDIT_LAW_NUMBER, '') <> ISNULL(SRC.CREDIT_LAW_NUMBER, '')
                     OR ISNULL(DST.CREDIT_ETHICS_NUMBER, '') <> ISNULL(SRC.CREDIT_ETHICS_NUMBER, '')
                     OR ISNULL(DST.PERIOD_ISCURRENT, '') <> ISNULL(SRC.PERIOD_ISCURRENT, '')
                     OR ISNULL(DST.CREDIT_SELF_NUMBER, '') <> ISNULL(SRC.CREDIT_SELF_NUMBER, '')
					 OR ISNULL(DST.CREDIT_AUTHORED_NUMBER, '') <> ISNULL(SRC.CREDIT_AUTHORED_NUMBER, '')
					 OR ISNULL(DST.PERIOD_EFFECTIVE_START, '') <> ISNULL(SRC.PERIOD_EFFECTIVE_START, '')
					 OR ISNULL(DST.PERIOD_EFFECTIVE_END, '') <> ISNULL(SRC.PERIOD_EFFECTIVE_END, '')
					 OR ISNULL(DST.PERIOD_REINSTATE_END, '') <> ISNULL(SRC.PERIOD_REINSTATE_END, '')
					 OR ISNULL(DST.CM_INTENTIONS, '') <> ISNULL(SRC.CM_INTENTIONS, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,SEQN
      ,CREDIT_STATUS
      ,CREDIT_PERIOD
      ,CREDIT_NUMBER
      ,CREDIT_NOTES
      ,CREDIT_REQUIRED_ADJUST
      ,CREDIT_LAW_NUMBER
      ,CREDIT_ETHICS_NUMBER
      ,PERIOD_ISCURRENT
      ,CREDIT_SELF_NUMBER
      ,CREDIT_AUTHORED_NUMBER
      ,PERIOD_EFFECTIVE_START
      ,PERIOD_EFFECTIVE_END
      ,PERIOD_REINSTATE_END
      ,CM_INTENTIONS
      ,TIME_STAMP
      ,PERIOD_COMPLETE
      ,IsActive
      ,StartDate
                     )

              VALUES (
        SRC.ID
      ,SRC.SEQN
      ,SRC.CREDIT_STATUS
      ,SRC.CREDIT_PERIOD
      ,SRC.CREDIT_NUMBER
      ,SRC.CREDIT_NOTES
      ,SRC.CREDIT_REQUIRED_ADJUST
      ,SRC.CREDIT_LAW_NUMBER
      ,SRC.CREDIT_ETHICS_NUMBER
      ,SRC.PERIOD_ISCURRENT
      ,SRC.CREDIT_SELF_NUMBER
      ,SRC.CREDIT_AUTHORED_NUMBER
      ,SRC.PERIOD_EFFECTIVE_START
      ,SRC.PERIOD_EFFECTIVE_END
      ,SRC.PERIOD_REINSTATE_END
      ,SRC.CM_INTENTIONS
      ,SRC.TIME_STAMP
      ,SRC.PERIOD_COMPLETE
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Custom_Credit'
       ,'stg.imis_Custom_Credit'
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
			,(select count(*) as RowsRead
				from tmp.imis_Custom_Credit
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_Credit (
ID
      ,SEQN
      ,CREDIT_STATUS
      ,CREDIT_PERIOD
      ,CREDIT_NUMBER
      ,CREDIT_NOTES
      ,CREDIT_REQUIRED_ADJUST
      ,CREDIT_LAW_NUMBER
      ,CREDIT_ETHICS_NUMBER
      ,PERIOD_ISCURRENT
      ,CREDIT_SELF_NUMBER
      ,CREDIT_AUTHORED_NUMBER
      ,PERIOD_EFFECTIVE_START
      ,PERIOD_EFFECTIVE_END
      ,PERIOD_REINSTATE_END
      ,CM_INTENTIONS
      ,TIME_STAMP
      ,PERIOD_COMPLETE
      ,IsActive
      ,StartDate
                 )
select  
                    ID
      ,SEQN
      ,CREDIT_STATUS
      ,CREDIT_PERIOD
      ,CREDIT_NUMBER
      ,CREDIT_NOTES
      ,CREDIT_REQUIRED_ADJUST
      ,CREDIT_LAW_NUMBER
      ,CREDIT_ETHICS_NUMBER
      ,PERIOD_ISCURRENT
      ,CREDIT_SELF_NUMBER
      ,CREDIT_AUTHORED_NUMBER
      ,PERIOD_EFFECTIVE_START
      ,PERIOD_EFFECTIVE_END
      ,PERIOD_REINSTATE_END
      ,CM_INTENTIONS
      ,cast(TIME_STAMP as bigint)
      ,PERIOD_COMPLETE
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Custom_Credit
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/**********************************************CUSTOM_EVENT_REGISTRATION*********************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Custom_Event_Registration AS DST
USING tmp.imis_Custom_Event_Registration AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.MEETING, '') <> ISNULL(SRC.MEETING, '')
                     OR ISNULL(DST.ADDRESS_1, '') <> ISNULL(SRC.ADDRESS_1, '')
                     OR ISNULL(DST.ADDRESS_2, '') <> ISNULL(SRC.ADDRESS_2, '')
                     OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
                     OR ISNULL(DST.STATE_PROVINCE, '') <> ISNULL(SRC.STATE_PROVINCE, '')
                     OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
                     OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
                     OR ISNULL(DST.BADGE_NAME, '') <> ISNULL(SRC.BADGE_NAME, '')
                     OR ISNULL(DST.BADGE_COMPANY, '') <> ISNULL(SRC.BADGE_COMPANY, '')
					 OR ISNULL(DST.BADGE_LOCATION, '') <> ISNULL(SRC.BADGE_LOCATION, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,SEQN
      ,MEETING
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,COUNTRY
      ,ZIP
      ,BADGE_NAME
      ,BADGE_COMPANY
      ,BADGE_LOCATION
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     SRC. ID
      ,SRC.SEQN
      ,SRC.MEETING
      ,SRC.ADDRESS_1
      ,SRC.ADDRESS_2
      ,SRC.CITY
      ,SRC.STATE_PROVINCE
      ,SRC.COUNTRY
      ,SRC.ZIP
      ,SRC.BADGE_NAME
      ,SRC.BADGE_COMPANY
      ,SRC.BADGE_LOCATION
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Custom_Event_Registration'
       ,'stg.imis_Custom_Event_Registration'
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
		,(select count(*) as RowsRead
				from tmp.imis_Custom_Event_Registration
				)     
       ,getdate()
       )

INSERT INTO stg.imis_Custom_Event_Registration (
 ID
      ,SEQN
      ,MEETING
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,COUNTRY
      ,ZIP
      ,BADGE_NAME
      ,BADGE_COMPANY
      ,BADGE_LOCATION
      ,TIME_STAMP
      ,IsActive
      ,StartDate 
                 )
select  
                     ID
      ,SEQN
      ,MEETING
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,COUNTRY
      ,ZIP
      ,BADGE_NAME
      ,BADGE_COMPANY
      ,BADGE_LOCATION
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Custom_Event_Registration
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/**********************************************CUSTOM_EVENT_SCHEDULE********************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Custom_Event_Schedule AS DST
USING tmp.imis_Custom_Event_Schedule AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.MEETING, '') <> ISNULL(SRC.MEETING, '')
                     OR ISNULL(DST.REGISTRANT_CLASS, '') <> ISNULL(SRC.REGISTRANT_CLASS, '')
                     OR ISNULL(DST.PRODUCT_CODE, '') <> ISNULL(SRC.PRODUCT_CODE, '')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.UNIT_PRICE, '') <> ISNULL(SRC.UNIT_PRICE, '')
					 OR ISNULL(DST.IS_WAITLIST, '') <> ISNULL(SRC.IS_WAITLIST, '')
            
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
           ID
      ,SEQN
      ,MEETING
      ,REGISTRANT_CLASS
      ,PRODUCT_CODE
      ,[STATUS]
      ,UNIT_PRICE
	  ,IS_WAITLIST
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     SRC.ID
      ,SRC.SEQN
      ,SRC.MEETING
      ,SRC.REGISTRANT_CLASS
      ,SRC.PRODUCT_CODE
      ,SRC.[STATUS]
      ,SRC.UNIT_PRICE
	  ,SRC.IS_WAITLIST
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Custom_Event_Schedule'
       ,'stg.imis_Custom_Event_Schedule'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Custom_Event_Schedule
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_Event_Schedule (
ID
      ,SEQN
      ,MEETING
      ,REGISTRANT_CLASS
      ,PRODUCT_CODE
      ,[STATUS]
      ,UNIT_PRICE
	  ,IS_WAITLIST
      ,TIME_STAMP
      ,IsActive
      ,StartDate 
                 )
select  
                    ID
      ,SEQN
      ,MEETING
      ,REGISTRANT_CLASS
      ,PRODUCT_CODE
      ,[STATUS]
      ,UNIT_PRICE
	  ,IS_WAITLIST
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Custom_Event_Schedule
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/**********************************************CUSTOM_SCHOOLACCREDITED***********************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Custom_SchoolAccredited AS DST
USING tmp.imis_Custom_SchoolAccredited AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.[START_DATE], '') <> ISNULL(SRC.[START_DATE], '')
                     OR ISNULL(DST.END_DATE, '') <> ISNULL(SRC.END_DATE, '')
                     OR ISNULL(DST.DEGREE_LEVEL, '') <> ISNULL(SRC.DEGREE_LEVEL, '')
                     OR ISNULL(DST.SCHOOL_PROGRAM_TYPE, '') <> ISNULL(SRC.SCHOOL_PROGRAM_TYPE, '')
                     OR ISNULL(DST.DEGREE_PROGRAM, '') <> ISNULL(SRC.DEGREE_PROGRAM, '')
          
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
          ID
      ,SEQN
      ,[START_DATE]
      ,END_DATE
      ,DEGREE_LEVEL
      ,SCHOOL_PROGRAM_TYPE
      ,DEGREE_PROGRAM
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                     )

              VALUES (
        SRC. ID
      ,SRC.SEQN
      ,SRC.[START_DATE]
      ,SRC.END_DATE
      ,SRC.DEGREE_LEVEL
      ,SRC.SCHOOL_PROGRAM_TYPE
      ,SRC.DEGREE_PROGRAM
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Custom_SchoolAccredited'
       ,'stg.imis_Custom_SchoolAccredited'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Custom_SchoolAccredited
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_SchoolAccredited (
 ID
      ,SEQN
      ,[START_DATE]
      ,END_DATE
      ,DEGREE_LEVEL
      ,SCHOOL_PROGRAM_TYPE
      ,DEGREE_PROGRAM
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select   ID
      ,SEQN
      ,[START_DATE]
      ,END_DATE
      ,DEGREE_LEVEL
      ,SCHOOL_PROGRAM_TYPE
      ,DEGREE_PROGRAM
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Custom_SchoolAccredited
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/********************************************************DEMO_INDONATE****************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Demo_Idonate AS DST
USING tmp.imis_Demo_Idonate AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.QUESTIONCODE, '') <> ISNULL(SRC.QUESTIONCODE, '')
                     OR ISNULL(DST.SELECTEDANSWER, '') <> ISNULL(SRC.SELECTEDANSWER, '')
                     OR ISNULL(DST.INVOICENUMBER, '') <> ISNULL(SRC.INVOICENUMBER, '')
                     OR ISNULL(DST.CREATE_DATE, '') <> ISNULL(SRC.CREATE_DATE, '')
                
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
        ID
      ,SEQN
      ,QuestionCode
      ,SelectedAnswer
      ,InvoiceNumber
      ,Create_Date
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
         SRC.ID
      ,SRC.SEQN
      ,SRC.QuestionCode
      ,SRC.SelectedAnswer
      ,SRC.InvoiceNumber
      ,SRC.Create_Date
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Demo_Idonate'
       ,'stg.imis_Demo_Idonate'
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
		,(select count(*) as RowsRead
				from tmp.imis_Demo_Idonate
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Demo_Idonate (
ID
      ,SEQN
      ,QuestionCode
      ,SelectedAnswer
      ,InvoiceNumber
      ,Create_Date
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
      ID
      ,SEQN
      ,QuestionCode
      ,SelectedAnswer
      ,InvoiceNumber
      ,Create_Date
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Demo_Idonate
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/******************************************************DEMO_RECURRING_GIFT********************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Demo_Recurring_Gift AS DST
USING tmp.imis_Demo_Recurring_Gift AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.RG_INITIAL_GIFT_DATE, '') <> ISNULL(SRC.RG_INITIAL_GIFT_DATE, '')
                     OR ISNULL(DST.RG_UNIQUE_ID, '') <> ISNULL(SRC.RG_UNIQUE_ID, '')
                     OR ISNULL(DST.RG_FREQUENCY, '') <> ISNULL(SRC.RG_FREQUENCY, '')
                     OR ISNULL(DST.RG_TOTAL_OCCURENCES, '') <> ISNULL(SRC.RG_TOTAL_OCCURENCES, '')
                     OR ISNULL(DST.RG_RECURRING_AMOUNT, '') <> ISNULL(SRC.RG_RECURRING_AMOUNT, '')
                     OR ISNULL(DST.RG_TOTAL_AMOUNT_EXPECTED, '') <> ISNULL(SRC.RG_TOTAL_AMOUNT_EXPECTED, '')
                     OR ISNULL(DST.RG_NUM_GIFTS_GIVEN, '') <> ISNULL(SRC.RG_NUM_GIFTS_GIVEN, '')
                     OR ISNULL(DST.RG_TOTAL_AMOUNT_GIVEN, '') <> ISNULL(SRC.RG_TOTAL_AMOUNT_GIVEN, '')
                     OR ISNULL(DST.RG_DISTRIBUTION, '') <> ISNULL(SRC.RG_DISTRIBUTION, '')
					 OR ISNULL(DST.RG_FUND, '') <> ISNULL(SRC.RG_FUND, '')
					 OR ISNULL(DST.RG_CAMPAIGN_CODE, '') <> ISNULL(SRC.RG_CAMPAIGN_CODE, '')
					 OR ISNULL(DST.RG_APPEAL_CODE, '') <> ISNULL(SRC.RG_APPEAL_CODE, '')
					 OR ISNULL(DST.RG_CC_EXP_MONTH, '') <> ISNULL(SRC.RG_CC_EXP_MONTH, '')
					 OR ISNULL(DST.RG_CC_EXP_YEAR, '') <> ISNULL(SRC.RG_CC_EXP_YEAR, '')
					 OR ISNULL(DST.RG_LAST_RESPONSE_DATE, '') <> ISNULL(SRC.RG_LAST_RESPONSE_DATE, '')
					 OR ISNULL(DST.RG_CONFIRMATION_NUM, '') <> ISNULL(SRC.RG_CONFIRMATION_NUM, '')
					 OR ISNULL(DST.RG_ACTIVE, '') <> ISNULL(SRC.RG_ACTIVE, '')
					 OR ISNULL(DST.RG_FIN_ACCT_ID, '') <> ISNULL(SRC.RG_FIN_ACCT_ID, '')
					 OR ISNULL(DST.RG_LAST_STATUS, '') <> ISNULL(SRC.RG_LAST_STATUS, '')
					 OR ISNULL(DST.RG_WARNING_FLAG, '') <> ISNULL(SRC.RG_WARNING_FLAG, '')
					 OR ISNULL(DST.RG_PLEDGE_TRANS_NUM, '') <> ISNULL(SRC.RG_PLEDGE_TRANS_NUM, '')
					 OR ISNULL(DST.RG_PLEDGE_LAST_STATUS, '') <> ISNULL(SRC.RG_PLEDGE_LAST_STATUS, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,SEQN
      ,RG_INITIAL_GIFT_DATE
      ,RG_UNIQUE_ID
      ,RG_FREQUENCY
      ,RG_TOTAL_OCCURENCES
      ,RG_RECURRING_AMOUNT
      ,RG_TOTAL_AMOUNT_EXPECTED
      ,RG_NUM_GIFTS_GIVEN
      ,RG_TOTAL_AMOUNT_GIVEN
      ,RG_DISTRIBUTION
      ,RG_FUND
      ,RG_CAMPAIGN_CODE
      ,RG_APPEAL_CODE
      ,RG_CC_EXP_MONTH
      ,RG_CC_EXP_YEAR
      ,RG_LAST_RESPONSE_DATE
      ,RG_CONFIRMATION_NUM
      ,RG_ACTIVE
      ,RG_FIN_ACCT_ID
      ,RG_LAST_STATUS
      ,RG_WARNING_FLAG
      ,TIME_STAMP
      ,RG_PLEDGE_TRANS_NUM
      ,RG_PLEDGE_LAST_STATUS
      ,IsActive
      ,StartDate
                     )

              VALUES (
      SRC.ID
      ,SRC.SEQN
      ,SRC.RG_INITIAL_GIFT_DATE
      ,SRC.RG_UNIQUE_ID
      ,SRC.RG_FREQUENCY
      ,SRC.RG_TOTAL_OCCURENCES
      ,SRC.RG_RECURRING_AMOUNT
      ,SRC.RG_TOTAL_AMOUNT_EXPECTED
      ,SRC.RG_NUM_GIFTS_GIVEN
      ,SRC.RG_TOTAL_AMOUNT_GIVEN
      ,SRC.RG_DISTRIBUTION
      ,SRC.RG_FUND
      ,SRC.RG_CAMPAIGN_CODE
      ,SRC.RG_APPEAL_CODE
      ,SRC.RG_CC_EXP_MONTH
      ,SRC.RG_CC_EXP_YEAR
      ,SRC.RG_LAST_RESPONSE_DATE
      ,SRC.RG_CONFIRMATION_NUM
      ,SRC.RG_ACTIVE
      ,SRC.RG_FIN_ACCT_ID
      ,SRC.RG_LAST_STATUS
      ,SRC.RG_WARNING_FLAG
      ,SRC.TIME_STAMP
      ,SRC.RG_PLEDGE_TRANS_NUM
      ,SRC.RG_PLEDGE_LAST_STATUS
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'       
	   ,'tmp.imis_Demo_Recurring_Gift'
       ,'stg.imis_Demo_Recurring_Gift'
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
			,(select count(*) as RowsRead
				from tmp.imis_Demo_Recurring_Gift
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Demo_Recurring_Gift (
ID
      ,SEQN
      ,RG_INITIAL_GIFT_DATE
      ,RG_UNIQUE_ID
      ,RG_FREQUENCY
      ,RG_TOTAL_OCCURENCES
      ,RG_RECURRING_AMOUNT
      ,RG_TOTAL_AMOUNT_EXPECTED
      ,RG_NUM_GIFTS_GIVEN
      ,RG_TOTAL_AMOUNT_GIVEN
      ,RG_DISTRIBUTION
      ,RG_FUND
      ,RG_CAMPAIGN_CODE
      ,RG_APPEAL_CODE
      ,RG_CC_EXP_MONTH
      ,RG_CC_EXP_YEAR
      ,RG_LAST_RESPONSE_DATE
      ,RG_CONFIRMATION_NUM
      ,RG_ACTIVE
      ,RG_FIN_ACCT_ID
      ,RG_LAST_STATUS
      ,RG_WARNING_FLAG
      ,TIME_STAMP
      ,RG_PLEDGE_TRANS_NUM
      ,RG_PLEDGE_LAST_STATUS
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,SEQN
      ,RG_INITIAL_GIFT_DATE
      ,RG_UNIQUE_ID
      ,RG_FREQUENCY
      ,RG_TOTAL_OCCURENCES
      ,RG_RECURRING_AMOUNT
      ,RG_TOTAL_AMOUNT_EXPECTED
      ,RG_NUM_GIFTS_GIVEN
      ,RG_TOTAL_AMOUNT_GIVEN
      ,RG_DISTRIBUTION
      ,RG_FUND
      ,RG_CAMPAIGN_CODE
      ,RG_APPEAL_CODE
      ,RG_CC_EXP_MONTH
      ,RG_CC_EXP_YEAR
      ,RG_LAST_RESPONSE_DATE
      ,RG_CONFIRMATION_NUM
      ,RG_ACTIVE
      ,RG_FIN_ACCT_ID
      ,RG_LAST_STATUS
      ,RG_WARNING_FLAG
      ,cast(TIME_STAMP as bigint)
      ,RG_PLEDGE_TRANS_NUM
      ,RG_PLEDGE_LAST_STATUS
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Demo_Recurring_Gift
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/***********************************************DIRECT_DEBIT**********************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Direct_Debit AS DST
USING tmp.imis_Direct_Debit AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.AUTO_DEBIT, '') <> ISNULL(SRC.AUTO_DEBIT, '')
                     OR ISNULL(DST.[START_DATE], '') <> ISNULL(SRC.[START_DATE], '')
                     OR ISNULL(DST.ACCOUNT_NUM, '') <> ISNULL(SRC.ACCOUNT_NUM, '')
                     OR ISNULL(DST.ABA_ROUTING, '') <> ISNULL(SRC.ABA_ROUTING, '')
                     OR ISNULL(DST.MONTHLY_PAYMENT, '') <> ISNULL(SRC.MONTHLY_PAYMENT, '')
                     OR ISNULL(DST.BANK_NAME, '') <> ISNULL(SRC.BANK_NAME , '')
                     OR ISNULL(DST.REMAIN_DEBIT, '') <> ISNULL(SRC.REMAIN_DEBIT, '')
                     OR ISNULL(DST.TOTAL_DEBIT, '') <> ISNULL(SRC.TOTAL_DEBIT , '')
					 OR ISNULL(DST.NUM_DEBIT, '') <> ISNULL(SRC.NUM_DEBIT , '')
                     OR ISNULL(DST.REMAIN_TIMES, '') <> ISNULL(SRC.REMAIN_TIMES , '')
					 OR ISNULL(DST.RUN_DATE, '') <> ISNULL(SRC.RUN_DATE , '')
					 OR ISNULL(DST.AT_CHECKING, '') <> ISNULL(SRC.AT_CHECKING , '')
					 OR ISNULL(DST.AT_SAVING, '') <> ISNULL(SRC.AT_SAVING, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                     )

              VALUES (
       SRC.ID
      ,SRC.AUTO_DEBIT
      ,SRC.[START_DATE]
      ,SRC.ACCOUNT_NUM
      ,SRC.ABA_ROUTING
      ,SRC.MONTHLY_PAYMENT
      ,SRC.BANK_NAME
      ,SRC.REMAIN_DEBIT
      ,SRC.TOTAL_DEBIT
      ,SRC.NUM_DEBIT
      ,SRC.REMAIN_TIMES
      ,SRC.RUN_DATE
      ,SRC.AT_CHECKING
      ,SRC.AT_SAVING
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Direct_Debit'
       ,'stg.imis_Direct_Debit'
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
		,(select count(*) as RowsRead
				from tmp.imis_Direct_Debit
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Direct_Debit (
ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Direct_Debit
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp



/********************************************************IND_DEMOGRAPHICS**********************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Ind_Demographics AS DST
USING tmp.imis_Ind_Demographics AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                     ISNULL(DST.APA_LIFE_DATE, '') <> ISNULL(SRC.APA_LIFE_DATE, '')
                     OR ISNULL(DST.AICP_LIFE_MEMBER, '') <> ISNULL(SRC.AICP_LIFE_MEMBER, '')
                     OR ISNULL(DST.AICP_LIFE_DATE, '') <> ISNULL(SRC.AICP_LIFE_DATE, '')
                     OR ISNULL(DST.FACULTY_POSITION, '') <> ISNULL(SRC.FACULTY_POSITION, '')
                     OR ISNULL(DST.ADMIN_POSITION, '') <> ISNULL(SRC.ADMIN_POSITION, '')
                     OR ISNULL(DST.SALARY_RANGE, '') <> ISNULL(SRC.SALARY_RANGE, '')
                     OR ISNULL(DST.PROMOTION_CODES, '') <> ISNULL(SRC.PROMOTION_CODES, '')
                     OR ISNULL(DST.DATE_OF_BIRTH, '') <> ISNULL(SRC.DATE_OF_BIRTH, '')
					 OR ISNULL(DST.SUB_SPECIALTY, '') <> ISNULL(SRC.SUB_SPECIALTY, '')
					 OR ISNULL(DST.USA_CITIZEN, '') <> ISNULL(SRC.USA_CITIZEN, '')
					 OR ISNULL(DST.AICP_START, '') <> ISNULL(SRC.AICP_START, '')
					 OR ISNULL(DST.AICP_CERT_NO, '') <> ISNULL(SRC.AICP_CERT_NO, '')
					 OR ISNULL(DST.PERPETUITY, '') <> ISNULL(SRC.PERPETUITY, '')
					 OR ISNULL(DST.AICP_PROMO_1, '') <> ISNULL(SRC.AICP_PROMO_1, '')
					 OR ISNULL(DST.HINT_PASSWORD, '') <> ISNULL(SRC.HINT_PASSWORD, '')
					 OR ISNULL(DST.HINT_ANSWER, '') <> ISNULL(SRC.HINT_ANSWER, '')
					 OR ISNULL(DST.COUNTRY_CODES, '') <> ISNULL(SRC.COUNTRY_CODES, '')
					 OR ISNULL(DST.SPECIALTY, '') <> ISNULL(SRC.SPECIALTY, '')
					 OR ISNULL(DST.APA_LIFE_MEMBER, '') <> ISNULL(SRC.APA_LIFE_MEMBER, '')
					 OR ISNULL(DST.CONF_CODE, '') <> ISNULL(SRC.CONF_CODE, '')
					 OR ISNULL(DST.MENTOR_SIGNUP, '') <> ISNULL(SRC.MENTOR_SIGNUP, '')
					 OR ISNULL(DST.MALE, '') <> ISNULL(SRC.MALE, '')
					 OR ISNULL(DST.FEMALE, '') <> ISNULL(SRC.FEMALE, '')
					 OR ISNULL(DST.DEPARTMENT, '') <> ISNULL(SRC.DEPARTMENT, '')
					 OR ISNULL(DST.CONV_NP, '') <> ISNULL(SRC.CONV_NP, '')
					 OR ISNULL(DST.INVOICE_NUM, '') <> ISNULL(SRC.INVOICE_NUM, '')
					 OR ISNULL(DST.PREV_MT, '') <> ISNULL(SRC.PREV_MT, '')
					 OR ISNULL(DST.CONV_FREESTU, '') <> ISNULL(SRC.CONV_FREESTU, '')
					 OR ISNULL(DST.CONV_STU, '') <> ISNULL(SRC.CONV_STU, '')
					 OR ISNULL(DST.CHAPT_ONLY, '') <> ISNULL(SRC.CHAPT_ONLY, '')
					 OR ISNULL(DST.ASLA, '') <> ISNULL(SRC.ASLA, '')
					 OR ISNULL(DST.SALARY_VERIFYDATE, '') <> ISNULL(SRC.SALARY_VERIFYDATE, '')
					 OR ISNULL(DST.FUNCTIONAL_TITLE_VERIFYDATE, '') <> ISNULL(SRC.FUNCTIONAL_TITLE_VERIFYDATE, '')
					 OR ISNULL(DST.PREVIOUS_AICP_CERT_NO, '') <> ISNULL(SRC.PREVIOUS_AICP_CERT_NO, '')
					 OR ISNULL(DST.PREVIOUS_AICP_START, '') <> ISNULL(SRC.PREVIOUS_AICP_START, '')
					 OR ISNULL(DST.EMAIL_SECONDARY, '') <> ISNULL(SRC.EMAIL_SECONDARY, '')
					 OR ISNULL(DST.NEW_MEMBER_START_DATE, '') <> ISNULL(SRC.NEW_MEMBER_START_DATE, '')
					 OR ISNULL(DST.CONV_ECP5, '') <> ISNULL(SRC.CONV_ECP5, '')
					 OR ISNULL(DST.EXCLUDE_FROM_DROP, '') <> ISNULL(SRC.EXCLUDE_FROM_DROP, '')
					 OR ISNULL(DST.STUDENT_START_DATE, '') <> ISNULL(SRC.STUDENT_START_DATE, '')
					 OR ISNULL(DST.IS_CURRENT_STUDENT, '') <> ISNULL(SRC.IS_CURRENT_STUDENT, '')
					 OR ISNULL(DST.JOIN_TYPE, '') <> ISNULL(SRC.JOIN_TYPE, '')
					 OR ISNULL(DST.UNDERPAID_MEMBER, '') <> ISNULL(SRC.UNDERPAID_MEMBER, '')
					 OR ISNULL(DST.GENDER_OTHER, '') <> ISNULL(SRC.GENDER_OTHER, '')
					 OR ISNULL(DST.JOIN_SOURCE, '') <> ISNULL(SRC.JOIN_SOURCE, '')

                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
              ID
      ,APA_LIFE_DATE
      ,AICP_LIFE_MEMBER
      ,AICP_LIFE_DATE
      ,FACULTY_POSITION
      ,ADMIN_POSITION
      ,SALARY_RANGE
      ,PROMOTION_CODES
      ,DATE_OF_BIRTH
      ,SUB_SPECIALTY
      ,USA_CITIZEN
      ,AICP_START
      ,AICP_CERT_NO
      ,PERPETUITY
      ,AICP_PROMO_1
      ,HINT_PASSWORD
      ,HINT_ANSWER
      ,COUNTRY_CODES
      ,SPECIALTY
      ,APA_LIFE_MEMBER
      ,CONF_CODE
      ,MENTOR_SIGNUP
      ,MALE
      ,FEMALE
      ,DEPARTMENT
      ,CONV_NP
      ,INVOICE_NUM
      ,PREV_MT
      ,CONV_FREESTU
      ,CONV_STU
      ,CHAPT_ONLY
      ,ASLA
      ,SALARY_VERIFYDATE
      ,FUNCTIONAL_TITLE_VERIFYDATE
      ,PREVIOUS_AICP_CERT_NO
      ,PREVIOUS_AICP_START
      ,EMAIL_SECONDARY
      ,NEW_MEMBER_START_DATE
      ,CONV_ECP5
      ,EXCLUDE_FROM_DROP
      ,STUDENT_START_DATE
      ,IS_CURRENT_STUDENT
      ,JOIN_TYPE
      ,UNDERPAID_MEMBER
      ,GENDER_OTHER
      ,JOIN_SOURCE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     SRC.ID
      ,SRC.APA_LIFE_DATE
      ,SRC.AICP_LIFE_MEMBER
      ,SRC.AICP_LIFE_DATE
      ,SRC.FACULTY_POSITION
      ,SRC.ADMIN_POSITION
      ,SRC.SALARY_RANGE
      ,SRC.PROMOTION_CODES
      ,SRC.DATE_OF_BIRTH
      ,SRC.SUB_SPECIALTY
      ,SRC.USA_CITIZEN
      ,SRC.AICP_START
      ,SRC.AICP_CERT_NO
      ,SRC.PERPETUITY
      ,SRC.AICP_PROMO_1
      ,SRC.HINT_PASSWORD
      ,SRC.HINT_ANSWER
      ,SRC.COUNTRY_CODES
      ,SRC.SPECIALTY
      ,SRC.APA_LIFE_MEMBER
      ,SRC.CONF_CODE
      ,SRC.MENTOR_SIGNUP
      ,SRC.MALE
      ,SRC.FEMALE
      ,SRC.DEPARTMENT
      ,SRC.CONV_NP
      ,SRC.INVOICE_NUM
      ,SRC.PREV_MT
      ,SRC.CONV_FREESTU
      ,SRC.CONV_STU
      ,SRC.CHAPT_ONLY
      ,SRC.ASLA
      ,SRC.SALARY_VERIFYDATE
      ,SRC.FUNCTIONAL_TITLE_VERIFYDATE
      ,SRC.PREVIOUS_AICP_CERT_NO
      ,SRC.PREVIOUS_AICP_START
      ,SRC.EMAIL_SECONDARY
      ,SRC.NEW_MEMBER_START_DATE
      ,SRC.CONV_ECP5
      ,SRC.EXCLUDE_FROM_DROP
      ,SRC.STUDENT_START_DATE
      ,SRC.IS_CURRENT_STUDENT
      ,SRC.JOIN_TYPE
      ,SRC.UNDERPAID_MEMBER
      ,SRC.GENDER_OTHER
      ,SRC.JOIN_SOURCE
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Ind_Demographics'
       ,'stg.imis_Ind_Demographics'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Ind_Demographics
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Ind_Demographics (
 ID
      ,APA_LIFE_DATE
      ,AICP_LIFE_MEMBER
      ,AICP_LIFE_DATE
      ,FACULTY_POSITION
      ,ADMIN_POSITION
      ,SALARY_RANGE
      ,PROMOTION_CODES
      ,DATE_OF_BIRTH
      ,SUB_SPECIALTY
      ,USA_CITIZEN
      ,AICP_START
      ,AICP_CERT_NO
      ,PERPETUITY
      ,AICP_PROMO_1
      ,HINT_PASSWORD
      ,HINT_ANSWER
      ,COUNTRY_CODES
      ,SPECIALTY
      ,APA_LIFE_MEMBER
      ,CONF_CODE
      ,MENTOR_SIGNUP
      ,MALE
      ,FEMALE
      ,DEPARTMENT
      ,CONV_NP
      ,INVOICE_NUM
      ,PREV_MT
      ,CONV_FREESTU
      ,CONV_STU
      ,CHAPT_ONLY
      ,ASLA
      ,SALARY_VERIFYDATE
      ,FUNCTIONAL_TITLE_VERIFYDATE
      ,PREVIOUS_AICP_CERT_NO
      ,PREVIOUS_AICP_START
      ,EMAIL_SECONDARY
      ,NEW_MEMBER_START_DATE
      ,CONV_ECP5
      ,EXCLUDE_FROM_DROP
      ,STUDENT_START_DATE
      ,IS_CURRENT_STUDENT
      ,JOIN_TYPE
      ,UNDERPAID_MEMBER
      ,GENDER_OTHER
      ,JOIN_SOURCE
      ,TIME_STAMP
      ,IsActive
      ,StartDate 
                 )
select  
                    ID
      ,APA_LIFE_DATE
      ,AICP_LIFE_MEMBER
      ,AICP_LIFE_DATE
      ,FACULTY_POSITION
      ,ADMIN_POSITION
      ,SALARY_RANGE
      ,PROMOTION_CODES
      ,DATE_OF_BIRTH
      ,SUB_SPECIALTY
      ,USA_CITIZEN
      ,AICP_START
      ,AICP_CERT_NO
      ,PERPETUITY
      ,AICP_PROMO_1
      ,HINT_PASSWORD
      ,HINT_ANSWER
      ,COUNTRY_CODES
      ,SPECIALTY
      ,APA_LIFE_MEMBER
      ,CONF_CODE
      ,MENTOR_SIGNUP
      ,MALE
      ,FEMALE
      ,DEPARTMENT
      ,CONV_NP
      ,INVOICE_NUM
      ,PREV_MT
      ,CONV_FREESTU
      ,CONV_STU
      ,CHAPT_ONLY
      ,ASLA
      ,SALARY_VERIFYDATE
      ,FUNCTIONAL_TITLE_VERIFYDATE
      ,PREVIOUS_AICP_CERT_NO
      ,PREVIOUS_AICP_START
      ,EMAIL_SECONDARY
      ,NEW_MEMBER_START_DATE
      ,CONV_ECP5
      ,EXCLUDE_FROM_DROP
      ,STUDENT_START_DATE
      ,IS_CURRENT_STUDENT
      ,JOIN_TYPE
      ,UNDERPAID_MEMBER
      ,GENDER_OTHER
      ,JOIN_SOURCE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Ind_Demographics
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/************************************************************MAILING DEMOGRAPHICS************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Mailing_Demographics AS DST
USING tmp.imis_Mailing_Demographics AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
				        ISNULL(DST.JOB_MART_BULK, '') <> ISNULL(SRC.JOB_MART_BULK, '')
					 OR ISNULL(DST.JOB_MART_ADDRESS, '') <> ISNULL(SRC.JOB_MART_ADDRESS, '')
                     OR ISNULL(DST.EXCL_MAIL_LIST, '') <> ISNULL(SRC.EXCL_MAIL_LIST, '')
                     OR ISNULL(DST.EXCL_WEBSITE, '') <> ISNULL(SRC.EXCL_WEBSITE, '')
                     OR ISNULL(DST.EXCL_INTERACT, '') <> ISNULL(SRC.EXCL_INTERACT, '')
                     OR ISNULL(DST.EXCL_SURVEY, '') <> ISNULL(SRC.EXCL_SURVEY, '')
                     OR ISNULL(DST.EXCL_ALL, '') <> ISNULL(SRC.EXCL_ALL, '')
                     OR ISNULL(DST.EXCL_NATLCONF, '') <> ISNULL(SRC.EXCL_NATLCONF, '')
                     OR ISNULL(DST.EXCL_OTHERCONF, '') <> ISNULL(SRC.EXCL_OTHERCONF, '')
                     OR ISNULL(DST.EXCL_PLANNING, '') <> ISNULL(SRC.EXCL_PLANNING, '')
                     OR ISNULL(DST.EXCL_JAPA, '') <> ISNULL(SRC.EXCL_JAPA, '')
					 OR ISNULL(DST.EXCL_ZP, '') <> ISNULL(SRC.EXCL_ZP, '')
					 OR ISNULL(DST.EXCL_PEL, '') <> ISNULL(SRC.EXCL_PEL, '')
					 OR ISNULL(DST.EXCL_PAC, '') <> ISNULL(SRC.EXCL_PAC, '')
					 OR ISNULL(DST.EXCL_PAN, '') <> ISNULL(SRC.EXCL_PAN, '')
					 OR ISNULL(DST.EXCL_PAS, '') <> ISNULL(SRC.EXCL_PAS, '')
					 OR ISNULL(DST.EXCL_COMMISSIONER, '') <> ISNULL(SRC.EXCL_COMMISSIONER, '')
					 OR ISNULL(DST.EXCL_FOUNDATION, '') <> ISNULL(SRC.EXCL_FOUNDATION, '')
					 OR ISNULL(DST.EXCL_LEARN, '') <> ISNULL(SRC.EXCL_LEARN, '')
					 OR ISNULL(DST.EXCL_PLANNING_HOME, '') <> ISNULL(SRC.EXCL_PLANNING_HOME, '')
					 OR ISNULL(DST.EXCL_PLANNING_PRINT, '') <> ISNULL(SRC.EXCL_PLANNING_PRINT, '')
					 OR ISNULL(DST.SPEAKER_ADDRESS, '') <> ISNULL(SRC.SPEAKER_ADDRESS, '')
					 OR ISNULL(DST.LEADERSHIP_ADDRESS, '') <> ISNULL(SRC.LEADERSHIP_ADDRESS, '')
					 OR ISNULL(DST.ROSTER_ADDRESS, '') <> ISNULL(SRC.ROSTER_ADDRESS, '')
					 OR ISNULL(DST.JOB_MART_INVOICE, '') <> ISNULL(SRC.JOB_MART_INVOICE, '')
					 OR ISNULL(DST.EXCL_EMAIL, '') <> ISNULL(SRC.EXCL_EMAIL, '')
					 OR ISNULL(DST.EXCL_ALL_SUB_PROMOS, '') <> ISNULL(SRC.EXCL_ALL_SUB_PROMOS, '')
					 OR ISNULL(DST.EXCL_AFFINITY, '') <> ISNULL(SRC.EXCL_AFFINITY, '')
					 OR ISNULL(DST.EXCL_APAMARKETING, '') <> ISNULL(SRC.EXCL_APAMARKETING, '')
					 OR ISNULL(DST.EXCL_BOOKSTORE, '') <> ISNULL(SRC.EXCL_BOOKSTORE, '')
					 OR ISNULL(DST.EXCL_TUESDAYS, '') <> ISNULL(SRC.EXCL_TUESDAYS, '')
					 OR ISNULL(DST.EXCL_CHINA, '') <> ISNULL(SRC.EXCL_CHINA, '')
					 OR ISNULL(DST.EXCL_CONFERENCE, '') <> ISNULL(SRC.EXCL_CONFERENCE, '')
					 OR ISNULL(DST.EXCL_APAADVOCATE, '') <> ISNULL(SRC.EXCL_APAADVOCATE, '')
					 OR ISNULL(DST.EXCL_INSURANCE, '') <> ISNULL(SRC.EXCL_INSURANCE, '')
					 OR ISNULL(DST.EXCL_JAPANEWS, '') <> ISNULL(SRC.EXCL_JAPANEWS, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
    ID
      ,JOB_MART_BULK
      ,JOB_MART_ADDRESS
      ,EXCL_MAIL_LIST
      ,EXCL_WEBSITE
      ,EXCL_INTERACT
      ,EXCL_SURVEY
      ,EXCL_ALL
      ,EXCL_NATLCONF
      ,EXCL_OTHERCONF
      ,EXCL_PLANNING
      ,EXCL_JAPA
      ,EXCL_ZP
      ,EXCL_PEL
      ,EXCL_PAC
      ,EXCL_PAN
      ,EXCL_PAS
      ,EXCL_COMMISSIONER
      ,EXCL_FOUNDATION
      ,EXCL_LEARN
      ,EXCL_PLANNING_HOME
      ,EXCL_PLANNING_PRINT
      ,SPEAKER_ADDRESS
      ,LEADERSHIP_ADDRESS
      ,ROSTER_ADDRESS
      ,JOB_MART_INVOICE
      ,EXCL_EMAIL
      ,EXCL_ALL_SUB_PROMOS
      ,EXCL_AFFINITY
      ,EXCL_APAMARKETING
      ,EXCL_BOOKSTORE
      ,EXCL_TUESDAYS
      ,EXCL_CHINA
      ,EXCL_CONFERENCE
      ,EXCL_APAADVOCATE
      ,EXCL_INSURANCE
      ,EXCL_JAPANEWS
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
    SRC.ID
      ,SRC.JOB_MART_BULK
      ,SRC.JOB_MART_ADDRESS
      ,SRC.EXCL_MAIL_LIST
      ,SRC.EXCL_WEBSITE
      ,SRC.EXCL_INTERACT
      ,SRC.EXCL_SURVEY
      ,SRC.EXCL_ALL
      ,SRC.EXCL_NATLCONF
      ,SRC.EXCL_OTHERCONF
      ,SRC.EXCL_PLANNING
      ,SRC.EXCL_JAPA
      ,SRC.EXCL_ZP
      ,SRC.EXCL_PEL
      ,SRC.EXCL_PAC
      ,SRC.EXCL_PAN
      ,SRC.EXCL_PAS
      ,SRC.EXCL_COMMISSIONER
      ,SRC.EXCL_FOUNDATION
      ,SRC.EXCL_LEARN
      ,SRC.EXCL_PLANNING_HOME
      ,SRC.EXCL_PLANNING_PRINT
      ,SRC.SPEAKER_ADDRESS
      ,SRC.LEADERSHIP_ADDRESS
      ,SRC.ROSTER_ADDRESS
      ,SRC.JOB_MART_INVOICE
      ,SRC.EXCL_EMAIL
      ,SRC.EXCL_ALL_SUB_PROMOS
      ,SRC.EXCL_AFFINITY
      ,SRC.EXCL_APAMARKETING
      ,SRC.EXCL_BOOKSTORE
      ,SRC.EXCL_TUESDAYS
      ,SRC.EXCL_CHINA
      ,SRC.EXCL_CONFERENCE
      ,SRC.EXCL_APAADVOCATE
      ,SRC.EXCL_INSURANCE
      ,SRC.EXCL_JAPANEWS
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Mailing_Demographics'
       ,'stg.imis_Mailing_Demographics'
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
		,(select count(*) as RowsRead
				from tmp.imis_Mailing_Demographics
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Mailing_Demographics (
ID
      ,JOB_MART_BULK
      ,JOB_MART_ADDRESS
      ,EXCL_MAIL_LIST
      ,EXCL_WEBSITE
      ,EXCL_INTERACT
      ,EXCL_SURVEY
      ,EXCL_ALL
      ,EXCL_NATLCONF
      ,EXCL_OTHERCONF
      ,EXCL_PLANNING
      ,EXCL_JAPA
      ,EXCL_ZP
      ,EXCL_PEL
      ,EXCL_PAC
      ,EXCL_PAN
      ,EXCL_PAS
      ,EXCL_COMMISSIONER
      ,EXCL_FOUNDATION
      ,EXCL_LEARN
      ,EXCL_PLANNING_HOME
      ,EXCL_PLANNING_PRINT
      ,SPEAKER_ADDRESS
      ,LEADERSHIP_ADDRESS
      ,ROSTER_ADDRESS
      ,JOB_MART_INVOICE
      ,EXCL_EMAIL
      ,EXCL_ALL_SUB_PROMOS
      ,EXCL_AFFINITY
      ,EXCL_APAMARKETING
      ,EXCL_BOOKSTORE
      ,EXCL_TUESDAYS
      ,EXCL_CHINA
      ,EXCL_CONFERENCE
      ,EXCL_APAADVOCATE
      ,EXCL_INSURANCE
      ,EXCL_JAPANEWS
      ,TIME_STAMP
      ,IsActive
      ,StartDate 
                 )
select  
                  ID
      ,JOB_MART_BULK
      ,JOB_MART_ADDRESS
      ,EXCL_MAIL_LIST
      ,EXCL_WEBSITE
      ,EXCL_INTERACT
      ,EXCL_SURVEY
      ,EXCL_ALL
      ,EXCL_NATLCONF
      ,EXCL_OTHERCONF
      ,EXCL_PLANNING
      ,EXCL_JAPA
      ,EXCL_ZP
      ,EXCL_PEL
      ,EXCL_PAC
      ,EXCL_PAN
      ,EXCL_PAS
      ,EXCL_COMMISSIONER
      ,EXCL_FOUNDATION
      ,EXCL_LEARN
      ,EXCL_PLANNING_HOME
      ,EXCL_PLANNING_PRINT
      ,SPEAKER_ADDRESS
      ,LEADERSHIP_ADDRESS
      ,ROSTER_ADDRESS
      ,JOB_MART_INVOICE
      ,EXCL_EMAIL
      ,EXCL_ALL_SUB_PROMOS
      ,EXCL_AFFINITY
      ,EXCL_APAMARKETING
      ,EXCL_BOOKSTORE
      ,EXCL_TUESDAYS
      ,EXCL_CHINA
      ,EXCL_CONFERENCE
      ,EXCL_APAADVOCATE
      ,EXCL_INSURANCE
      ,EXCL_JAPANEWS
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Mailing_Demographics
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*********************************************************************************NAME************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Name AS DST
USING tmp.imis_Name AS SRC
       ON DST.ID = SRC.ID
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.ORG_CODE, '') <> ISNULL(SRC.ORG_CODE, '')
                     OR ISNULL(DST.MEMBER_TYPE, '') <> ISNULL(SRC.MEMBER_TYPE, '')
                     OR ISNULL(DST.CATEGORY, '') <> ISNULL(SRC.CATEGORY, '')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.MAJOR_KEY, '') <> ISNULL(SRC.MAJOR_KEY, '')
                     OR ISNULL(DST.CO_ID, '') <> ISNULL(SRC.CO_ID, '')
                     OR ISNULL(DST.LAST_FIRST, '') <> ISNULL(SRC.LAST_FIRST, '')
                     OR ISNULL(DST.COMPANY_SORT, '') <> ISNULL(SRC.COMPANY_SORT, '')
                     OR ISNULL(DST.BT_ID, '') <> ISNULL(SRC.BT_ID, '')
					 OR ISNULL(DST.DUP_MATCH_KEY, '') <> ISNULL(SRC.DUP_MATCH_KEY, '')
					 OR ISNULL(DST.FULL_NAME, '') <> ISNULL(SRC.FULL_NAME, '')
					 OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
					 OR ISNULL(DST.COMPANY, '') <> ISNULL(SRC.COMPANY, '')
					 OR ISNULL(DST.FULL_ADDRESS, '') <> ISNULL(SRC.FULL_ADDRESS, '')
					 OR ISNULL(DST.PREFIX, '') <> ISNULL(SRC.PREFIX, '')
					 OR ISNULL(DST.FIRST_NAME, '') <> ISNULL(SRC.FIRST_NAME, '')
					 OR ISNULL(DST.MIDDLE_NAME, '') <> ISNULL(SRC.MIDDLE_NAME, '')
					 OR ISNULL(DST.LAST_NAME, '') <> ISNULL(SRC.LAST_NAME, '')
					 OR ISNULL(DST.SUFFIX, '') <> ISNULL(SRC.SUFFIX, '')
					 OR ISNULL(DST.DESIGNATION, '') <> ISNULL(SRC.DESIGNATION, '')
					 OR ISNULL(DST.INFORMAL, '') <> ISNULL(SRC.INFORMAL, '')
					 OR ISNULL(DST.WORK_PHONE, '') <> ISNULL(SRC.WORK_PHONE, '')
					 OR ISNULL(DST.HOME_PHONE, '') <> ISNULL(SRC.HOME_PHONE, '')
					 OR ISNULL(DST.FAX, '') <> ISNULL(SRC.FAX, '')
					 OR ISNULL(DST.TOLL_FREE, '') <> ISNULL(SRC.TOLL_FREE, '')
					 OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
					 OR ISNULL(DST.STATE_PROVINCE, '') <> ISNULL(SRC.STATE_PROVINCE, '')
					 OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
					 OR ISNULL(DST.MAIL_CODE, '') <> ISNULL(SRC.MAIL_CODE, '')
					 OR ISNULL(DST.CRRT, '') <> ISNULL(SRC.CRRT, '')
					 OR ISNULL(DST.BAR_CODE, '') <> ISNULL(SRC.BAR_CODE, '')
					 OR ISNULL(DST.COUNTY, '') <> ISNULL(SRC.COUNTY, '')
					 OR ISNULL(DST.MAIL_ADDRESS_NUM, '') <> ISNULL(SRC.MAIL_ADDRESS_NUM, '')
					 OR ISNULL(DST.BILL_ADDRESS_NUM, '') <> ISNULL(SRC.BILL_ADDRESS_NUM, '')
					 OR ISNULL(DST.GENDER, '') <> ISNULL(SRC.GENDER, '')
					 OR ISNULL(DST.BIRTH_DATE, '') <> ISNULL(SRC.BIRTH_DATE, '')
					 OR ISNULL(DST.US_CONGRESS, '') <> ISNULL(SRC.US_CONGRESS, '')
					 OR ISNULL(DST.STATE_SENATE, '') <> ISNULL(SRC.STATE_SENATE, '')
					 OR ISNULL(DST.STATE_HOUSE, '') <> ISNULL(SRC.STATE_HOUSE, '')
					 OR ISNULL(DST.SIC_CODE, '') <> ISNULL(SRC.SIC_CODE, '')
					 OR ISNULL(DST.CHAPTER, '') <> ISNULL(SRC.CHAPTER, '')
					 OR ISNULL(DST.FUNCTIONAL_TITLE, '') <> ISNULL(SRC.FUNCTIONAL_TITLE, '')
					 OR ISNULL(DST.CONTACT_RANK, '') <> ISNULL(SRC.CONTACT_RANK, '')
					 OR ISNULL(DST.MEMBER_RECORD, '') <> ISNULL(SRC.MEMBER_RECORD, '')
					 OR ISNULL(DST.COMPANY_RECORD, '') <> ISNULL(SRC.COMPANY_RECORD, '')
					 OR ISNULL(DST.JOIN_DATE, '') <> ISNULL(SRC.JOIN_DATE, '')
					 OR ISNULL(DST.SOURCE_CODE, '') <> ISNULL(SRC.SOURCE_CODE, '')
					 OR ISNULL(DST.PAID_THRU, '') <> ISNULL(SRC.PAID_THRU, '')
					 OR ISNULL(DST.MEMBER_STATUS, '') <> ISNULL(SRC.MEMBER_STATUS, '')
					 OR ISNULL(DST.MEMBER_STATUS_DATE, '') <> ISNULL(SRC.MEMBER_STATUS_DATE, '')
					 OR ISNULL(DST.PREVIOUS_MT, '') <> ISNULL(SRC.PREVIOUS_MT, '')
					 OR ISNULL(DST.MT_CHANGE_DATE, '') <> ISNULL(SRC.MT_CHANGE_DATE, '')
					 OR ISNULL(DST.CO_MEMBER_TYPE, '') <> ISNULL(SRC.CO_MEMBER_TYPE, '')
					 OR ISNULL(DST.EXCLUDE_MAIL, '') <> ISNULL(SRC.EXCLUDE_MAIL, '')
					 OR ISNULL(DST.EXCLUDE_DIRECTORY, '') <> ISNULL(SRC.EXCLUDE_DIRECTORY, '')
					 OR ISNULL(DST.DATE_ADDED, '') <> ISNULL(SRC.DATE_ADDED, '')
					 OR ISNULL(DST.LAST_UPDATED, '') <> ISNULL(SRC.LAST_UPDATED, '')
					 OR ISNULL(DST.UPDATED_BY, '') <> ISNULL(SRC.UPDATED_BY, '')
					 OR ISNULL(DST.INTENT_TO_EDIT, '') <> ISNULL(SRC.INTENT_TO_EDIT, '')
					 OR ISNULL(DST.ADDRESS_NUM_1, '') <> ISNULL(SRC.ADDRESS_NUM_1, '')
					 OR ISNULL(DST.ADDRESS_NUM_2, '') <> ISNULL(SRC.ADDRESS_NUM_2, '')
					 OR ISNULL(DST.ADDRESS_NUM_3, '') <> ISNULL(SRC.ADDRESS_NUM_3, '')
					 OR ISNULL(DST.EMAIL, '') <> ISNULL(SRC.EMAIL, '')
					 OR ISNULL(DST.WEBSITE, '') <> ISNULL(SRC.WEBSITE, '')
					 OR ISNULL(DST.SHIP_ADDRESS_NUM, '') <> ISNULL(SRC.SHIP_ADDRESS_NUM, '')
					 OR ISNULL(DST.DISPLAY_CURRENCY, '') <> ISNULL(SRC.DISPLAY_CURRENCY, '')
					 OR ISNULL(DST.MOBILE_PHONE, '') <> ISNULL(SRC.MOBILE_PHONE, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,ORG_CODE
      ,MEMBER_TYPE
      ,CATEGORY
      ,[STATUS]
      ,MAJOR_KEY
      ,CO_ID
      ,LAST_FIRST
      ,COMPANY_SORT
      ,BT_ID
      ,DUP_MATCH_KEY
      ,FULL_NAME
      ,TITLE
      ,COMPANY
      ,FULL_ADDRESS
      ,PREFIX
      ,FIRST_NAME
      ,MIDDLE_NAME
      ,LAST_NAME
      ,SUFFIX
      ,DESIGNATION
      ,INFORMAL
      ,WORK_PHONE
      ,HOME_PHONE
      ,FAX
      ,TOLL_FREE
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,MAIL_CODE
      ,CRRT
      ,BAR_CODE
      ,COUNTY
      ,MAIL_ADDRESS_NUM
      ,BILL_ADDRESS_NUM
      ,GENDER
      ,BIRTH_DATE
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,SIC_CODE
      ,CHAPTER
      ,FUNCTIONAL_TITLE
      ,CONTACT_RANK
      ,MEMBER_RECORD
      ,COMPANY_RECORD
      ,JOIN_DATE
      ,SOURCE_CODE
      ,PAID_THRU
      ,MEMBER_STATUS
      ,MEMBER_STATUS_DATE
      ,PREVIOUS_MT
      ,MT_CHANGE_DATE
      ,CO_MEMBER_TYPE
      ,EXCLUDE_MAIL
      ,EXCLUDE_DIRECTORY
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,ADDRESS_NUM_1
      ,ADDRESS_NUM_2
      ,ADDRESS_NUM_3
      ,EMAIL
      ,WEBSITE
      ,SHIP_ADDRESS_NUM
      ,DISPLAY_CURRENCY
      ,MOBILE_PHONE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                    SRC.ID
      ,SRC.ORG_CODE
      ,SRC.MEMBER_TYPE
      ,SRC.CATEGORY
      ,SRC.[STATUS]
      ,SRC.MAJOR_KEY
      ,SRC.CO_ID
      ,SRC.LAST_FIRST
      ,SRC.COMPANY_SORT
      ,SRC.BT_ID
      ,SRC.DUP_MATCH_KEY
      ,SRC.FULL_NAME
      ,SRC.TITLE
      ,SRC.COMPANY
      ,SRC.FULL_ADDRESS
      ,SRC.PREFIX
      ,SRC.FIRST_NAME
      ,SRC.MIDDLE_NAME
      ,SRC.LAST_NAME
      ,SRC.SUFFIX
      ,SRC.DESIGNATION
      ,SRC.INFORMAL
      ,SRC.WORK_PHONE
      ,SRC.HOME_PHONE
      ,SRC.FAX
      ,SRC.TOLL_FREE
      ,SRC.CITY
      ,SRC.STATE_PROVINCE
      ,SRC.ZIP
      ,SRC.COUNTRY
      ,SRC.MAIL_CODE
      ,SRC.CRRT
      ,SRC.BAR_CODE
      ,SRC.COUNTY
      ,SRC.MAIL_ADDRESS_NUM
      ,SRC.BILL_ADDRESS_NUM
      ,SRC.GENDER
      ,SRC.BIRTH_DATE
      ,SRC.US_CONGRESS
      ,SRC.STATE_SENATE
      ,SRC.STATE_HOUSE
      ,SRC.SIC_CODE
      ,SRC.CHAPTER
      ,SRC.FUNCTIONAL_TITLE
      ,SRC.CONTACT_RANK
      ,SRC.MEMBER_RECORD
      ,SRC.COMPANY_RECORD
      ,SRC.JOIN_DATE
      ,SRC.SOURCE_CODE
      ,SRC.PAID_THRU
      ,SRC.MEMBER_STATUS
      ,SRC.MEMBER_STATUS_DATE
      ,SRC.PREVIOUS_MT
      ,SRC.MT_CHANGE_DATE
      ,SRC.CO_MEMBER_TYPE
      ,SRC.EXCLUDE_MAIL
      ,SRC.EXCLUDE_DIRECTORY
      ,SRC.DATE_ADDED
      ,SRC.LAST_UPDATED
      ,SRC.UPDATED_BY
      ,SRC.INTENT_TO_EDIT
      ,SRC.ADDRESS_NUM_1
      ,SRC.ADDRESS_NUM_2
      ,SRC.ADDRESS_NUM_3
      ,SRC.EMAIL
      ,SRC.WEBSITE
      ,SRC.SHIP_ADDRESS_NUM
      ,SRC.DISPLAY_CURRENCY
      ,SRC.MOBILE_PHONE
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Name'
       ,'stg.imis_Name'
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
			,(select count(*) as RowsRead
				from tmp.imis_Name
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Name (
ID
      ,ORG_CODE
      ,MEMBER_TYPE
      ,CATEGORY
      ,[STATUS]
      ,MAJOR_KEY
      ,CO_ID
      ,LAST_FIRST
      ,COMPANY_SORT
      ,BT_ID
      ,DUP_MATCH_KEY
      ,FULL_NAME
      ,TITLE
      ,COMPANY
      ,FULL_ADDRESS
      ,PREFIX
      ,FIRST_NAME
      ,MIDDLE_NAME
      ,LAST_NAME
      ,SUFFIX
      ,DESIGNATION
      ,INFORMAL
      ,WORK_PHONE
      ,HOME_PHONE
      ,FAX
      ,TOLL_FREE
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,MAIL_CODE
      ,CRRT
      ,BAR_CODE
      ,COUNTY
      ,MAIL_ADDRESS_NUM
      ,BILL_ADDRESS_NUM
      ,GENDER
      ,BIRTH_DATE
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,SIC_CODE
      ,CHAPTER
      ,FUNCTIONAL_TITLE
      ,CONTACT_RANK
      ,MEMBER_RECORD
      ,COMPANY_RECORD
      ,JOIN_DATE
      ,SOURCE_CODE
      ,PAID_THRU
      ,MEMBER_STATUS
      ,MEMBER_STATUS_DATE
      ,PREVIOUS_MT
      ,MT_CHANGE_DATE
      ,CO_MEMBER_TYPE
      ,EXCLUDE_MAIL
      ,EXCLUDE_DIRECTORY
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,ADDRESS_NUM_1
      ,ADDRESS_NUM_2
      ,ADDRESS_NUM_3
      ,EMAIL
      ,WEBSITE
      ,SHIP_ADDRESS_NUM
      ,DISPLAY_CURRENCY
      ,MOBILE_PHONE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  ID
      ,ORG_CODE
      ,MEMBER_TYPE
      ,CATEGORY
      ,[STATUS]
      ,MAJOR_KEY
      ,CO_ID
      ,LAST_FIRST
      ,COMPANY_SORT
      ,BT_ID
      ,DUP_MATCH_KEY
      ,FULL_NAME
      ,TITLE
      ,COMPANY
      ,FULL_ADDRESS
      ,PREFIX
      ,FIRST_NAME
      ,MIDDLE_NAME
      ,LAST_NAME
      ,SUFFIX
      ,DESIGNATION
      ,INFORMAL
      ,WORK_PHONE
      ,HOME_PHONE
      ,FAX
      ,TOLL_FREE
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,MAIL_CODE
      ,CRRT
      ,BAR_CODE
      ,COUNTY
      ,MAIL_ADDRESS_NUM
      ,BILL_ADDRESS_NUM
      ,GENDER
      ,BIRTH_DATE
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,SIC_CODE
      ,CHAPTER
      ,FUNCTIONAL_TITLE
      ,CONTACT_RANK
      ,MEMBER_RECORD
      ,COMPANY_RECORD
      ,JOIN_DATE
      ,SOURCE_CODE
      ,PAID_THRU
      ,MEMBER_STATUS
      ,MEMBER_STATUS_DATE
      ,PREVIOUS_MT
      ,MT_CHANGE_DATE
      ,CO_MEMBER_TYPE
      ,EXCLUDE_MAIL
      ,EXCLUDE_DIRECTORY
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,ADDRESS_NUM_1
      ,ADDRESS_NUM_2
      ,ADDRESS_NUM_3
      ,EMAIL
      ,WEBSITE
      ,SHIP_ADDRESS_NUM
      ,DISPLAY_CURRENCY
      ,MOBILE_PHONE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Name
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp


/********************************************************************************ORG_DEMOGRAPHICS*************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_ORG_Demographics AS DST
USING tmp.imis_ORG_Demographics AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.PAS_CODE, '') <> ISNULL(SRC.PAS_CODE, '')
                     OR ISNULL(DST.ORG_TYPE, '') <> ISNULL(SRC.ORG_TYPE, '')
                     OR ISNULL(DST.[POPULATION], '') <> ISNULL(SRC.[POPULATION], '')
                     OR ISNULL(DST.ANNUAL_BUDGET, '') <> ISNULL(SRC.ANNUAL_BUDGET, '')
                     OR ISNULL(DST.STAFF_SIZE, '') <> ISNULL(SRC.STAFF_SIZE, '')
                     OR ISNULL(DST.PARENT_ID, '') <> ISNULL(SRC.PARENT_ID, '')
                     OR ISNULL(DST.TOP_CITY, '') <> ISNULL(SRC.TOP_CITY, '')
                     OR ISNULL(DST.TOP_COUNTY, '') <> ISNULL(SRC.TOP_COUNTY, '')
                     OR ISNULL(DST.PLANNING_FUNCTION, '') <> ISNULL(SRC.PLANNING_FUNCTION, '')
					 OR ISNULL(DST.SCHOOL_PROGRAM_TYPE, '') <> ISNULL(SRC.SCHOOL_PROGRAM_TYPE, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     ID
      ,PAS_CODE
      ,ORG_TYPE
      ,[POPULATION]
      ,ANNUAL_BUDGET
      ,STAFF_SIZE
      ,PARENT_ID
      ,TOP_CITY
      ,TOP_COUNTY
      ,PLANNING_FUNCTION
      ,SCHOOL_PROGRAM_TYPE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                      SRC.ID
      ,SRC.PAS_CODE
      ,SRC.ORG_TYPE
      ,SRC.[POPULATION]
      ,SRC.ANNUAL_BUDGET
      ,SRC.STAFF_SIZE
      ,SRC.PARENT_ID
      ,SRC.TOP_CITY
      ,SRC.TOP_COUNTY
      ,SRC.PLANNING_FUNCTION
      ,SRC.SCHOOL_PROGRAM_TYPE
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_ORG_Demographics'
       ,'stg.imis_ORG_Demographics'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_ORG_Demographics
				) 
       ,getdate()
       )

INSERT INTO stg.imis_ORG_Demographics (
 ID
      ,PAS_CODE
      ,ORG_TYPE
      ,[POPULATION]
      ,ANNUAL_BUDGET
      ,STAFF_SIZE
      ,PARENT_ID
      ,TOP_CITY
      ,TOP_COUNTY
      ,PLANNING_FUNCTION
      ,SCHOOL_PROGRAM_TYPE
      ,TIME_STAMP
      ,IsActive
      ,StartDate 
                 )
select  
                      ID
      ,PAS_CODE
      ,ORG_TYPE
      ,[POPULATION]
      ,ANNUAL_BUDGET
      ,STAFF_SIZE
      ,PARENT_ID
      ,TOP_CITY
      ,TOP_COUNTY
      ,PLANNING_FUNCTION
      ,SCHOOL_PROGRAM_TYPE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_ORG_Demographics
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/****************************************************************************PAS_Demographics*****************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_PAS_Demographics AS DST
USING tmp.imis_PAS_Demographics AS SRC
       ON DST.ID = SRC.ID
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.PAS_TYPE, '') <> ISNULL(SRC.PAS_TYPE, '')
                     OR ISNULL(DST.POP_CITY_COUNTY, '') <> ISNULL(SRC.POP_CITY_COUNTY, '')
                     OR ISNULL(DST.POP_REG_STATE, '') <> ISNULL(SRC.POP_REG_STATE, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    ID
      ,PAS_TYPE
      ,POP_CITY_COUNTY
      ,POP_REG_STATE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     ID
      ,PAS_TYPE
      ,POP_CITY_COUNTY
      ,POP_REG_STATE
      ,TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_PAS_Demographics'
       ,'stg.imis_PAS_Demographics'
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
			,(select count(*) as RowsRead
				from tmp.imis_PAS_Demographics
				) 
       ,getdate()
       )

INSERT INTO stg.imis_PAS_Demographics (
 ID
      ,PAS_TYPE
      ,POP_CITY_COUNTY
      ,POP_REG_STATE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                      ID
      ,PAS_TYPE
      ,POP_CITY_COUNTY
      ,POP_REG_STATE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_PAS_Demographics
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*******************************************************************************RACE_ORIGIN****************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Race_Origin AS DST
USING tmp.imis_Race_Origin AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.RACE, '') <> ISNULL(SRC.RACE, '')
                     OR ISNULL(DST.ORIGIN, '') <> ISNULL(SRC.ORIGIN, '')
                     OR ISNULL(DST.SPAN_HISP_LATINO, '') <> ISNULL(SRC.SPAN_HISP_LATINO, '')
                     OR ISNULL(DST.AI_AN, '') <> ISNULL(SRC.AI_AN, '')
                     OR ISNULL(DST.ASIAN_PACIFIC, '') <> ISNULL(SRC.ASIAN_PACIFIC, '')
                     OR ISNULL(DST.OTHER, '') <> ISNULL(SRC.OTHER, '')
                     OR ISNULL(DST.ETHNICITY_VERIFYDATE, '') <> ISNULL(SRC.ETHNICITY_VERIFYDATE, '')
                     OR ISNULL(DST.ORIGIN_NOANSWER, '') <> ISNULL(SRC.ORIGIN_NOANSWER, '')
                     
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    ID
      ,RACE
      ,ORIGIN
      ,SPAN_HISP_LATINO
      ,AI_AN
      ,ASIAN_PACIFIC
      ,OTHER
      ,ETHNICITY_VERIFYDATE
      ,ORIGIN_VERIFYDATE
      ,ETHNICITY_NOANSWER
      ,ORIGIN_NOANSWER
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     SRC.ID
      ,SRC.RACE
      ,SRC.ORIGIN
      ,SRC.SPAN_HISP_LATINO
      ,SRC.AI_AN
      ,SRC.ASIAN_PACIFIC
      ,SRC.OTHER
      ,SRC.ETHNICITY_VERIFYDATE
      ,SRC.ORIGIN_VERIFYDATE
      ,SRC.ETHNICITY_NOANSWER
      ,SRC.ORIGIN_NOANSWER
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Race_Origin'
       ,'stg.imis_Race_Origin'
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
		,(select count(*) as RowsRead
				from tmp.imis_Race_Origin
				)
       ,getdate()
       )

INSERT INTO stg.imis_Race_Origin (
ID
      ,RACE
      ,ORIGIN
      ,SPAN_HISP_LATINO
      ,AI_AN
      ,ASIAN_PACIFIC
      ,OTHER
      ,ETHNICITY_VERIFYDATE
      ,ORIGIN_VERIFYDATE
      ,ETHNICITY_NOANSWER
      ,ORIGIN_NOANSWER
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,RACE
      ,ORIGIN
      ,SPAN_HISP_LATINO
      ,AI_AN
      ,ASIAN_PACIFIC
      ,OTHER
      ,ETHNICITY_VERIFYDATE
      ,ORIGIN_VERIFYDATE
      ,ETHNICITY_NOANSWER
      ,ORIGIN_NOANSWER
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Race_Origin
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/***********************************************************************REALMAGNET************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_RealMagnet AS DST
USING tmp.imis_RealMagnet AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.ACTIVITYCODE, '') <> ISNULL(SRC.ACTIVITYCODE, '')
                     OR ISNULL(DST.ACTIVITYSUBCODED, '') <> ISNULL(SRC.ACTIVITYSUBCODED, '')
                     OR ISNULL(DST.EMAILADDRESS, '') <> ISNULL(SRC.EMAILADDRESS, '')
                     OR ISNULL(DST.RECIPIENTID, '') <> ISNULL(SRC.RECIPIENTID, '')
                     OR ISNULL(DST.DATESTAMPUTC, '') <> ISNULL(SRC.DATESTAMPUTC, '')
                     OR ISNULL(DST.CATEGORYNAME, '') <> ISNULL(SRC.CATEGORYNAME, '')
                     OR ISNULL(DST.GROUPNAME, '') <> ISNULL(SRC.GROUPNAME, '')
                     OR ISNULL(DST.MESSAGENAME, '') <> ISNULL(SRC.MESSAGENAME, '')
                     OR ISNULL(DST.LINKURL, '') <> ISNULL(SRC.LINKURL, '')
					 OR ISNULL(DST.LINKLABEL, '') <> ISNULL(SRC.LINKLABEL, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    ID
      ,SEQN
      ,ActivityCode
      ,ActivitySubcoded
      ,EmailAddress
      ,RecipientID
      ,DateStampUTC
      ,CategoryName
      ,GroupName
      ,MessageName
      ,LinkUrl
      ,LinkLabel
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                      SRC.ID
      ,SRC.SEQN
      ,SRC.ActivityCode
      ,SRC.ActivitySubcoded
      ,SRC.EmailAddress
      ,SRC.RecipientID
      ,SRC.DateStampUTC
      ,SRC.CategoryName
      ,SRC.GroupName
      ,SRC.MessageName
      ,SRC.LinkUrl
      ,SRC.LinkLabel
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_RealMagnet'
       ,'stg.imis_RealMagnet'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_RealMagnet
				) 
       ,getdate()
       )

INSERT INTO stg.imis_RealMagnet (
 ID
      ,SEQN
      ,ActivityCode
      ,ActivitySubcoded
      ,EmailAddress
      ,RecipientID
      ,DateStampUTC
      ,CategoryName
      ,GroupName
      ,MessageName
      ,LinkUrl
      ,LinkLabel
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                      ID
      ,SEQN
      ,ActivityCode
      ,ActivitySubcoded
      ,EmailAddress
      ,RecipientID
      ,DateStampUTC
      ,CategoryName
      ,GroupName
      ,MessageName
      ,LinkUrl
      ,LinkLabel
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_RealMagnet
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*************************************************************************SUBSCRIPTIONS*******************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Subscriptions AS DST
USING tmp.imis_Subscriptions AS SRC
       ON DST.ID = SRC.ID and
   DST.Product_Code = SRC.Product_Code
WHEN MATCHED
              AND IsActive = 1
              AND (
                    ISNULL(DST.BT_ID, '') <> ISNULL(SRC.BT_ID, '')
                     OR ISNULL(DST.PROD_TYPE, '') <> ISNULL(SRC.PROD_TYPE, '')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.BEGIN_DATE, '') <> ISNULL(SRC.BEGIN_DATE, '')
                     OR ISNULL(DST.PAID_THRU, '') <> ISNULL(SRC.PAID_THRU, '')
                     OR ISNULL(DST.COPIES, '') <> ISNULL(SRC.COPIES, '')
                     OR ISNULL(DST.SOURCE_CODE, '') <> ISNULL(SRC.SOURCE_CODE, '')
                     OR ISNULL(DST.FIRST_SUBSCRIBED, '') <> ISNULL(SRC.FIRST_SUBSCRIBED, '')
					 OR ISNULL(DST.CONTINUOUS_SINCE, '') <> ISNULL(SRC.CONTINUOUS_SINCE, '')
					 OR ISNULL(DST.PRIOR_YEARS, '') <> ISNULL(SRC.PRIOR_YEARS, '')
					 OR ISNULL(DST.FUTURE_COPIES, '') <> ISNULL(SRC.FUTURE_COPIES, '')
					 OR ISNULL(DST.FUTURE_COPIES_DATE, '') <> ISNULL(SRC.FUTURE_COPIES_DATE, '')
					 OR ISNULL(DST.PREF_MAIL, '') <> ISNULL(SRC.PREF_MAIL, '')
					 OR ISNULL(DST.PREF_BILL, '') <> ISNULL(SRC.PREF_BILL, '')
					 OR ISNULL(DST.RENEW_MONTHS, '') <> ISNULL(SRC.RENEW_MONTHS, '')
					 OR ISNULL(DST.MAIL_CODE, '') <> ISNULL(SRC.MAIL_CODE, '')
					 OR ISNULL(DST.PREVIOUS_BALANCE, '') <> ISNULL(SRC.PREVIOUS_BALANCE, '')
					 OR ISNULL(DST.BILL_DATE, '') <> ISNULL(SRC.BILL_DATE, '')
					 OR ISNULL(DST.REMINDER_DATE, '') <> ISNULL(SRC.REMINDER_DATE, '')
					 OR ISNULL(DST.REMINDER_COUNT, '') <> ISNULL(SRC.REMINDER_COUNT, '')
					 OR ISNULL(DST.BILL_BEGIN, '') <> ISNULL(SRC.BILL_BEGIN, '')
					 OR ISNULL(DST.BILL_THRU, '') <> ISNULL(SRC.BILL_THRU, '')
					 OR ISNULL(DST.BILL_AMOUNT, '') <> ISNULL(SRC.BILL_AMOUNT, '')
					 OR ISNULL(DST.BILL_COPIES, '') <> ISNULL(SRC.BILL_COPIES, '')
					 OR ISNULL(DST.PAYMENT_AMOUNT, '') <> ISNULL(SRC.PAYMENT_AMOUNT, '')
					 OR ISNULL(DST.PAYMENT_DATE, '') <> ISNULL(SRC.PAYMENT_DATE, '')
					 OR ISNULL(DST.PAID_BEGIN, '') <> ISNULL(SRC.PAID_BEGIN, '')
					 OR ISNULL(DST.LAST_PAID_THRU, '') <> ISNULL(SRC.LAST_PAID_THRU, '')
					 OR ISNULL(DST.COPIES_PAID, '') <> ISNULL(SRC.COPIES_PAID, '')
					 OR ISNULL(DST.ADJUSTMENT_AMOUNT, '') <> ISNULL(SRC.ADJUSTMENT_AMOUNT, '')
					 OR ISNULL(DST.LTD_PAYMENTS, '') <> ISNULL(SRC.LTD_PAYMENTS, '')
					 OR ISNULL(DST.ISSUES_PRINTED, '') <> ISNULL(SRC.ISSUES_PRINTED, '')
					 OR ISNULL(DST.BALANCE, '') <> ISNULL(SRC.BALANCE, '')
					 OR ISNULL(DST.CANCEL_REASON, '') <> ISNULL(SRC.CANCEL_REASON, '')
					 OR ISNULL(DST.YEARS_ACTIVE_STRING, '') <> ISNULL(SRC.YEARS_ACTIVE_STRING, '')
					 OR ISNULL(DST.LAST_ISSUE, '') <> ISNULL(SRC.LAST_ISSUE, '')
					 OR ISNULL(DST.LAST_ISSUE_DATE, '') <> ISNULL(SRC.LAST_ISSUE_DATE, '')
					 OR ISNULL(DST.DATE_ADDED, '') <> ISNULL(SRC.DATE_ADDED, '')
					 OR ISNULL(DST.LAST_UPDATED, '') <> ISNULL(SRC.LAST_UPDATED, '')
					 OR ISNULL(DST.UPDATED_BY, '') <> ISNULL(SRC.UPDATED_BY, '')
					 OR ISNULL(DST.INTENT_TO_EDIT, '') <> ISNULL(SRC.INTENT_TO_EDIT, '')
					 OR ISNULL(DST.FLAG, '') <> ISNULL(SRC.FLAG, '')
					 OR ISNULL(DST.BILL_TYPE, '') <> ISNULL(SRC.BILL_TYPE, '')
					 OR ISNULL(DST.COMPLIMENTARY, '') <> ISNULL(SRC.COMPLIMENTARY, '')
					 OR ISNULL(DST.FUTURE_CREDITS, '') <> ISNULL(SRC.FUTURE_CREDITS, '')
					 OR ISNULL(DST.INVOICE_REFERENCE_NUM, '') <> ISNULL(SRC.INVOICE_REFERENCE_NUM, '')
					 OR ISNULL(DST.INVOICE_LINE_NUM, '') <> ISNULL(SRC.INVOICE_LINE_NUM, '')
					 OR ISNULL(DST.CAMPAIGN_CODE, '') <> ISNULL(SRC.CAMPAIGN_CODE, '')
					 OR ISNULL(DST.APPEAL_CODE, '') <> ISNULL(SRC.APPEAL_CODE, '')
					 OR ISNULL(DST.ORG_CODE, '') <> ISNULL(SRC.ORG_CODE, '')
					 OR ISNULL(DST.IS_FR_ITEM, '') <> ISNULL(SRC.IS_FR_ITEM, '')
					 OR ISNULL(DST.FAIR_MARKET_VALUE, 0) <> ISNULL(SRC.FAIR_MARKET_VALUE, 0)
					 OR ISNULL(DST.IS_GROUP_ADMIN, '') <> ISNULL(SRC.IS_GROUP_ADMIN, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     ID
      ,PRODUCT_CODE
      ,BT_ID
      ,PROD_TYPE
      ,[STATUS]
      ,BEGIN_DATE
      ,PAID_THRU
      ,COPIES
      ,SOURCE_CODE
      ,FIRST_SUBSCRIBED
      ,CONTINUOUS_SINCE
      ,PRIOR_YEARS
      ,FUTURE_COPIES
      ,FUTURE_COPIES_DATE
      ,PREF_MAIL
      ,PREF_BILL
      ,RENEW_MONTHS
      ,MAIL_CODE
      ,PREVIOUS_BALANCE
      ,BILL_DATE
      ,REMINDER_DATE
      ,REMINDER_COUNT
      ,BILL_BEGIN
      ,BILL_THRU
      ,BILL_AMOUNT
      ,BILL_COPIES
      ,PAYMENT_AMOUNT
      ,PAYMENT_DATE
      ,PAID_BEGIN
      ,LAST_PAID_THRU
      ,COPIES_PAID
      ,ADJUSTMENT_AMOUNT
      ,LTD_PAYMENTS
      ,ISSUES_PRINTED
      ,BALANCE
      ,CANCEL_REASON
      ,YEARS_ACTIVE_STRING
      ,LAST_ISSUE
      ,LAST_ISSUE_DATE
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,FLAG
      ,BILL_TYPE
      ,COMPLIMENTARY
      ,FUTURE_CREDITS
      ,INVOICE_REFERENCE_NUM
      ,INVOICE_LINE_NUM
      ,CAMPAIGN_CODE
      ,APPEAL_CODE
      ,ORG_CODE
      ,IS_FR_ITEM
      ,FAIR_MARKET_VALUE
      ,IS_GROUP_ADMIN
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                      SRC.ID
      ,SRC.PRODUCT_CODE
      ,SRC.BT_ID
      ,SRC.PROD_TYPE
      ,SRC.[STATUS]
      ,SRC.BEGIN_DATE
      ,SRC.PAID_THRU
      ,SRC.COPIES
      ,SRC.SOURCE_CODE
      ,SRC.FIRST_SUBSCRIBED
      ,SRC.CONTINUOUS_SINCE
      ,SRC.PRIOR_YEARS
      ,SRC.FUTURE_COPIES
      ,SRC.FUTURE_COPIES_DATE
      ,SRC.PREF_MAIL
      ,SRC.PREF_BILL
      ,SRC.RENEW_MONTHS
      ,SRC.MAIL_CODE
      ,SRC.PREVIOUS_BALANCE
      ,SRC.BILL_DATE
      ,SRC.REMINDER_DATE
      ,SRC.REMINDER_COUNT
      ,SRC.BILL_BEGIN
      ,SRC.BILL_THRU
      ,SRC.BILL_AMOUNT
      ,SRC.BILL_COPIES
      ,SRC.PAYMENT_AMOUNT
      ,SRC.PAYMENT_DATE
      ,SRC.PAID_BEGIN
      ,SRC.LAST_PAID_THRU
      ,SRC.COPIES_PAID
      ,SRC.ADJUSTMENT_AMOUNT
      ,SRC.LTD_PAYMENTS
      ,SRC.ISSUES_PRINTED
      ,SRC.BALANCE
      ,SRC.CANCEL_REASON
      ,SRC.YEARS_ACTIVE_STRING
      ,SRC.LAST_ISSUE
      ,SRC.LAST_ISSUE_DATE
      ,SRC.DATE_ADDED
      ,SRC.LAST_UPDATED
      ,SRC.UPDATED_BY
      ,SRC.INTENT_TO_EDIT
      ,SRC.FLAG
      ,SRC.BILL_TYPE
      ,SRC.COMPLIMENTARY
      ,SRC.FUTURE_CREDITS
      ,SRC.INVOICE_REFERENCE_NUM
      ,SRC.INVOICE_LINE_NUM
      ,SRC.CAMPAIGN_CODE
      ,SRC.APPEAL_CODE
      ,SRC.ORG_CODE
      ,SRC.IS_FR_ITEM
      ,SRC.FAIR_MARKET_VALUE
      ,SRC.IS_GROUP_ADMIN
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Subscriptions'
       ,'stg.imis_Subscriptions'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Subscriptions
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Subscriptions (
 ID
      ,PRODUCT_CODE
      ,BT_ID
      ,PROD_TYPE
      ,[STATUS]
      ,BEGIN_DATE
      ,PAID_THRU
      ,COPIES
      ,SOURCE_CODE
      ,FIRST_SUBSCRIBED
      ,CONTINUOUS_SINCE
      ,PRIOR_YEARS
      ,FUTURE_COPIES
      ,FUTURE_COPIES_DATE
      ,PREF_MAIL
      ,PREF_BILL
      ,RENEW_MONTHS
      ,MAIL_CODE
      ,PREVIOUS_BALANCE
      ,BILL_DATE
      ,REMINDER_DATE
      ,REMINDER_COUNT
      ,BILL_BEGIN
      ,BILL_THRU
      ,BILL_AMOUNT
      ,BILL_COPIES
      ,PAYMENT_AMOUNT
      ,PAYMENT_DATE
      ,PAID_BEGIN
      ,LAST_PAID_THRU
      ,COPIES_PAID
      ,ADJUSTMENT_AMOUNT
      ,LTD_PAYMENTS
      ,ISSUES_PRINTED
      ,BALANCE
      ,CANCEL_REASON
      ,YEARS_ACTIVE_STRING
      ,LAST_ISSUE
      ,LAST_ISSUE_DATE
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,FLAG
      ,BILL_TYPE
      ,COMPLIMENTARY
      ,FUTURE_CREDITS
      ,INVOICE_REFERENCE_NUM
      ,INVOICE_LINE_NUM
      ,CAMPAIGN_CODE
      ,APPEAL_CODE
      ,ORG_CODE
      ,IS_FR_ITEM
      ,FAIR_MARKET_VALUE
      ,IS_GROUP_ADMIN
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,PRODUCT_CODE
      ,BT_ID
      ,PROD_TYPE
      ,[STATUS]
      ,BEGIN_DATE
      ,PAID_THRU
      ,COPIES
      ,SOURCE_CODE
      ,FIRST_SUBSCRIBED
      ,CONTINUOUS_SINCE
      ,PRIOR_YEARS
      ,FUTURE_COPIES
      ,FUTURE_COPIES_DATE
      ,PREF_MAIL
      ,PREF_BILL
      ,RENEW_MONTHS
      ,MAIL_CODE
      ,PREVIOUS_BALANCE
      ,BILL_DATE
      ,REMINDER_DATE
      ,REMINDER_COUNT
      ,BILL_BEGIN
      ,BILL_THRU
      ,BILL_AMOUNT
      ,BILL_COPIES
      ,PAYMENT_AMOUNT
      ,PAYMENT_DATE
      ,PAID_BEGIN
      ,LAST_PAID_THRU
      ,COPIES_PAID
      ,ADJUSTMENT_AMOUNT
      ,LTD_PAYMENTS
      ,ISSUES_PRINTED
      ,BALANCE
      ,CANCEL_REASON
      ,YEARS_ACTIVE_STRING
      ,LAST_ISSUE
      ,LAST_ISSUE_DATE
      ,DATE_ADDED
      ,LAST_UPDATED
      ,UPDATED_BY
      ,INTENT_TO_EDIT
      ,FLAG
      ,BILL_TYPE
      ,COMPLIMENTARY
      ,FUTURE_CREDITS
      ,INVOICE_REFERENCE_NUM
      ,INVOICE_LINE_NUM
      ,CAMPAIGN_CODE
      ,APPEAL_CODE
      ,ORG_CODE
      ,IS_FR_ITEM
      ,FAIR_MARKET_VALUE
      ,IS_GROUP_ADMIN
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Subscriptions
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*************************************************************************VERIFICATION***********************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Verification AS DST
USING tmp.imis_Verification AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.VERI_TYPE, '') <> ISNULL(SRC.VERI_TYPE, '')
                     OR ISNULL(DST.VERI_DATE, '') <> ISNULL(SRC.VERI_DATE, '')
                     OR ISNULL(DST.SCHOOL, '') <> ISNULL(SRC.SCHOOL, '')
                     OR ISNULL(DST.SCHOOLS_NOT_LISTED, '') <> ISNULL(SRC.SCHOOLS_NOT_LISTED, '')
                     OR ISNULL(DST.GRAD_DATE, '') <> ISNULL(SRC.GRAD_DATE, '')
                     OR ISNULL(DST.STUDENT_ID, '') <> ISNULL(SRC.STUDENT_ID, '')
                     OR ISNULL(DST.MUNICIPALITY, '') <> ISNULL(SRC.MUNICIPALITY, '')
                     OR ISNULL(DST.BOARD, '') <> ISNULL(SRC.BOARD, '')
                     OR ISNULL(DST.EARNING, '') <> ISNULL(SRC.EARNING, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    ID
      ,VERI_TYPE
      ,VERI_DATE
      ,SCHOOL
      ,SCHOOLS_NOT_LISTED
      ,GRAD_DATE
      ,STUDENT_ID
      ,MUNICIPALITY
      ,BOARD
      ,EARNING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                      SRC.ID
      ,SRC.VERI_TYPE
      ,SRC.VERI_DATE
      ,SRC.SCHOOL
      ,SRC.SCHOOLS_NOT_LISTED
      ,SRC.GRAD_DATE
      ,SRC.STUDENT_ID
      ,SRC.MUNICIPALITY
      ,SRC.BOARD
      ,SRC.EARNING
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Verification'
       ,'stg.imis_Verification'
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
		,(select count(*) as RowsRead
				from tmp.imis_Verification
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Verification (
 ID
      ,VERI_TYPE
      ,VERI_DATE
      ,SCHOOL
      ,SCHOOLS_NOT_LISTED
      ,GRAD_DATE
      ,STUDENT_ID
      ,MUNICIPALITY
      ,BOARD
      ,EARNING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                      ID
      ,VERI_TYPE
      ,VERI_DATE
      ,SCHOOL
      ,SCHOOLS_NOT_LISTED
      ,GRAD_DATE
      ,STUDENT_ID
      ,MUNICIPALITY
      ,BOARD
      ,EARNING
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Verification
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*****************************************************************NAME_ADDRESS*************************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Name_Address AS DST
USING tmp.imis_Name_Address AS SRC
       ON DST.ADDRESS_NUM = SRC.ADDRESS_NUM 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.BAR_CODE, '') <> ISNULL(SRC.BAR_CODE, '')
                     OR ISNULL(DST.PURPOSE, '') <> ISNULL(SRC.PURPOSE, '')
                     OR ISNULL(DST.COMPANY, '') <> ISNULL(SRC.COMPANY, '')
                     OR ISNULL(DST.ADDRESS_1, '') <> ISNULL(SRC.ADDRESS_1, '')
                     OR ISNULL(DST.ADDRESS_2, '') <> ISNULL(SRC.ADDRESS_2, '')
                     OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
                     OR ISNULL(DST.STATE_PROVINCE, '') <> ISNULL(SRC.STATE_PROVINCE, '')
                     OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
                     OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
					 OR ISNULL(DST.CRRT, '') <> ISNULL(SRC.CRRT, '')
					 OR ISNULL(DST.DPB, '') <> ISNULL(SRC.DPB, '')
					 OR ISNULL(DST.COUNTRY_CODE, '') <> ISNULL(SRC.COUNTRY_CODE, '')
					 OR ISNULL(DST.ADDRESS_FORMAT, '') <> ISNULL(SRC.ADDRESS_FORMAT, '')
					 OR ISNULL(DST.FULL_ADDRESS, '') <> ISNULL(SRC.FULL_ADDRESS, '')
					 OR ISNULL(DST.COUNTY, '') <> ISNULL(SRC.COUNTY, '')
					 OR ISNULL(DST.US_CONGRESS, '') <> ISNULL(SRC.US_CONGRESS, '')
					 OR ISNULL(DST.STATE_SENATE, '') <> ISNULL(SRC.STATE_SENATE, '')
					 OR ISNULL(DST.STATE_HOUSE, '') <> ISNULL(SRC.STATE_HOUSE, '')
					 OR ISNULL(DST.MAIL_CODE, '') <> ISNULL(SRC.MAIL_CODE, '')
					 OR ISNULL(DST.PHONE, '') <> ISNULL(SRC.PHONE, '')
					 OR ISNULL(DST.FAX, '') <> ISNULL(SRC.FAX, '')
					 OR ISNULL(DST.TOLL_FREE, '') <> ISNULL(SRC.TOLL_FREE, '')
					 OR ISNULL(DST.COMPANY_SORT, '') <> ISNULL(SRC.COMPANY_SORT, '')
					 OR ISNULL(DST.NOTE, '') LIKE ISNULL(SRC.NOTE, '')
					 OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
					 OR ISNULL(DST.LAST_UPDATED, '') <> ISNULL(SRC.LAST_UPDATED, '')
					 OR ISNULL(DST.LIST_STRING, '') <> ISNULL(SRC.LIST_STRING, '')
					 OR ISNULL(DST.PREFERRED_MAIL, '') <> ISNULL(SRC.PREFERRED_MAIL, '')
					 OR ISNULL(DST.PREFERRED_BILL, '') <> ISNULL(SRC.PREFERRED_BILL, '')
					 OR ISNULL(DST.LAST_VERIFIED, '') <> ISNULL(SRC.LAST_VERIFIED, '')
					 OR ISNULL(DST.EMAIL, '') <> ISNULL(SRC.EMAIL, '')
					 OR ISNULL(DST.BAD_ADDRESS, '') <> ISNULL(SRC.BAD_ADDRESS, '')
					 OR ISNULL(DST.NO_AUTOVERIFY, '') <> ISNULL(SRC.NO_AUTOVERIFY, '')
					 OR ISNULL(DST.LAST_QAS_BATCH, '') <> ISNULL(SRC.LAST_QAS_BATCH, '')
					 OR ISNULL(DST.ADDRESS_3, '') <> ISNULL(SRC.ADDRESS_3, '')
					 OR ISNULL(DST.PREFERRED_SHIP, '') <> ISNULL(SRC.PREFERRED_SHIP, '')
					 OR ISNULL(DST.INFORMAL, '') <> ISNULL(SRC.INFORMAL, '')
					 OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
					 
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     ID
      ,ADDRESS_NUM
      ,PURPOSE
      ,COMPANY
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,CRRT
      ,DPB
      ,BAR_CODE
      ,COUNTRY_CODE
      ,ADDRESS_FORMAT
      ,FULL_ADDRESS
      ,COUNTY
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,MAIL_CODE
      ,PHONE
      ,FAX
      ,TOLL_FREE
      ,COMPANY_SORT
      ,NOTE
      ,[STATUS]
      ,LAST_UPDATED
      ,LIST_STRING
      ,PREFERRED_MAIL
      ,PREFERRED_BILL
      ,LAST_VERIFIED
      ,EMAIL
      ,BAD_ADDRESS
      ,NO_AUTOVERIFY
      ,LAST_QAS_BATCH
      ,ADDRESS_3
      ,PREFERRED_SHIP
      ,INFORMAL
      ,TITLE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                    SRC.ID
      ,SRC.ADDRESS_NUM
      ,SRC.PURPOSE
      ,SRC.COMPANY
      ,SRC.ADDRESS_1
      ,SRC.ADDRESS_2
      ,SRC.CITY
      ,SRC.STATE_PROVINCE
      ,SRC.ZIP
      ,SRC.COUNTRY
      ,SRC.CRRT
      ,SRC.DPB
      ,SRC.BAR_CODE
      ,SRC.COUNTRY_CODE
      ,SRC.ADDRESS_FORMAT
      ,SRC.FULL_ADDRESS
      ,SRC.COUNTY
      ,SRC.US_CONGRESS
      ,SRC.STATE_SENATE
      ,SRC.STATE_HOUSE
      ,SRC.MAIL_CODE
      ,SRC.PHONE
      ,SRC.FAX
      ,SRC.TOLL_FREE
      ,SRC.COMPANY_SORT
      ,SRC.NOTE
      ,SRC.[STATUS]
      ,SRC.LAST_UPDATED
      ,SRC.LIST_STRING
      ,SRC.PREFERRED_MAIL
      ,SRC.PREFERRED_BILL
      ,SRC.LAST_VERIFIED
      ,SRC.EMAIL
      ,SRC.BAD_ADDRESS
      ,SRC.NO_AUTOVERIFY
      ,SRC.LAST_QAS_BATCH
      ,SRC.ADDRESS_3
      ,SRC.PREFERRED_SHIP
      ,SRC.INFORMAL
      ,SRC.TITLE
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Name_Address'
       ,'stg.imis_Name_Address'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Name_Address
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Name_Address (
ID
      ,ADDRESS_NUM
      ,PURPOSE
      ,COMPANY
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,CRRT
      ,DPB
      ,BAR_CODE
      ,COUNTRY_CODE
      ,ADDRESS_FORMAT
      ,FULL_ADDRESS
      ,COUNTY
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,MAIL_CODE
      ,PHONE
      ,FAX
      ,TOLL_FREE
      ,COMPANY_SORT
      ,NOTE
      ,[STATUS]
      ,LAST_UPDATED
      ,LIST_STRING
      ,PREFERRED_MAIL
      ,PREFERRED_BILL
      ,LAST_VERIFIED
      ,EMAIL
      ,BAD_ADDRESS
      ,NO_AUTOVERIFY
      ,LAST_QAS_BATCH
      ,ADDRESS_3
      ,PREFERRED_SHIP
      ,INFORMAL
      ,TITLE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                    ID
      ,ADDRESS_NUM
      ,PURPOSE
      ,COMPANY
      ,ADDRESS_1
      ,ADDRESS_2
      ,CITY
      ,STATE_PROVINCE
      ,ZIP
      ,COUNTRY
      ,CRRT
      ,DPB
      ,BAR_CODE
      ,COUNTRY_CODE
      ,ADDRESS_FORMAT
      ,FULL_ADDRESS
      ,COUNTY
      ,US_CONGRESS
      ,STATE_SENATE
      ,STATE_HOUSE
      ,MAIL_CODE
      ,PHONE
      ,FAX
      ,TOLL_FREE
      ,COMPANY_SORT
      ,NOTE
      ,[STATUS]
      ,LAST_UPDATED
      ,LIST_STRING
      ,PREFERRED_MAIL
      ,PREFERRED_BILL
      ,LAST_VERIFIED
      ,EMAIL
      ,BAD_ADDRESS
      ,NO_AUTOVERIFY
      ,LAST_QAS_BATCH
      ,ADDRESS_3
      ,PREFERRED_SHIP
      ,INFORMAL
      ,TITLE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Name_Address
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp



/*********************************************************************GEN_TABLES*******************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Gen_Tables AS DST
USING tmp.imis_Gen_Tables AS SRC
       ON DST.TABLE_NAME = SRC.TABLE_NAME AND 
	   DST.CODE = SRC.CODE
WHEN MATCHED
              AND IsActive = 1
              AND (
                    ISNULL(DST.SUBSTITUTE, '') <> ISNULL(SRC.SUBSTITUTE, '')
                     OR ISNULL(DST.UPPER_CODE, '') <> ISNULL(SRC.UPPER_CODE, '')
                     OR ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.OBSOLETE_DESCRIPTION, '') <> ISNULL(SRC.OBSOLETE_DESCRIPTION, '')
                     OR ISNULL(DST.NCODE, '') <> ISNULL(SRC.NCODE, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     TABLE_NAME
      ,CODE
      ,SUBSTITUTE
      ,UPPER_CODE
      ,[DESCRIPTION]
      ,OBSOLETE_DESCRIPTION
      ,NCODE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
          
                     )
              VALUES (
                     SRC.TABLE_NAME
      ,SRC.CODE
      ,SRC.SUBSTITUTE
      ,SRC.UPPER_CODE
      ,SRC.[DESCRIPTION]
      ,SRC.OBSOLETE_DESCRIPTION
      ,SRC.NCODE
      ,SRC.TIME_STAMP
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.TABLE_NAME
       ,deleted.TABLE_NAME
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Gen_Tables'
       ,'stg.imis_Gen_Tables'
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
	    ,(select count(*) as RowsRead
				from tmp.imis_Gen_Tables
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Gen_Tables (
TABLE_NAME
      ,CODE
      ,SUBSTITUTE
      ,UPPER_CODE
      ,[DESCRIPTION]
      ,OBSOLETE_DESCRIPTION
      ,NCODE
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                   TABLE_NAME
      ,CODE
      ,SUBSTITUTE
      ,UPPER_CODE
      ,[DESCRIPTION]
      ,OBSOLETE_DESCRIPTION
      ,NCODE
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Gen_Tables
WHERE TABLE_NAME IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*****************************************************************PRODUCT*************************************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Product AS DST
USING tmp.imis_Product AS SRC
       ON DST.PRODUCT_CODE = SRC.PRODUCT_CODE
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.PRODUCT_MAJOR, '') <> ISNULL(SRC.PRODUCT_MAJOR, '')
                     OR ISNULL(DST.PRODUCT_MINOR, '') <> ISNULL(SRC.PRODUCT_MINOR, '')
                     OR ISNULL(DST.PROD_TYPE, '') <> ISNULL(SRC.PROD_TYPE, '')
                     OR ISNULL(DST.CATEGORY, '') <> ISNULL(SRC.CATEGORY, '')
                     OR ISNULL(DST.TITLE_KEY, '') <> ISNULL(SRC.TITLE_KEY, '')
                     OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
                     OR ISNULL(DST.[DESCRIPTION], '') LIKE ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.NOTE, '') LIKE ISNULL(SRC.NOTE, '')
					 OR ISNULL(DST.GROUP_1, '') <> ISNULL(SRC.GROUP_1, '')
					 OR ISNULL(DST.GROUP_2, '') <> ISNULL(SRC.GROUP_2, '')
					 OR ISNULL(DST.GROUP_3, '') <> ISNULL(SRC.GROUP_3, '')
					 OR ISNULL(DST.PRICE_RULES_EXIST, '') <> ISNULL(SRC.PRICE_RULES_EXIST, '')
					 OR ISNULL(DST.LOT_SERIAL_EXIST, '') <> ISNULL(SRC.LOT_SERIAL_EXIST, '')
					 OR ISNULL(DST.PAYMENT_PRIORITY, '') <> ISNULL(SRC.PAYMENT_PRIORITY, '')
					 OR ISNULL(DST.RENEW_MONTHS, '') <> ISNULL(SRC.RENEW_MONTHS, '')
					 OR ISNULL(DST.PRORATE, '') <> ISNULL(SRC.PRORATE, '')
					 OR ISNULL(DST.STOCK_ITEM, '') <> ISNULL(SRC.STOCK_ITEM, '')
					 OR ISNULL(DST.UNIT_OF_MEASURE, '') <> ISNULL(SRC.UNIT_OF_MEASURE, '')
					 OR ISNULL(DST.[WEIGHT], 0) <> ISNULL(SRC.[WEIGHT], 0)
					 OR ISNULL(DST.TAXABLE, '') <> ISNULL(SRC.TAXABLE, '')
					 OR ISNULL(DST.COMMISIONABLE, '') <> ISNULL(SRC.COMMISIONABLE, '')
					 OR ISNULL(DST.COMMISION_PERCENT, 0) <> ISNULL(SRC.COMMISION_PERCENT, 0)
					 OR ISNULL(DST.DECIMAL_POINTS, '') <> ISNULL(SRC.TITLE_KEY, '')
					 OR ISNULL(DST.INCOME_ACCOUNT, '') <> ISNULL(SRC.INCOME_ACCOUNT, '')
					 OR ISNULL(DST.DEFERRED_INCOME_ACCOUNT, '') <> ISNULL(SRC.DEFERRED_INCOME_ACCOUNT, '')
					 OR ISNULL(DST.INVENTORY_ACCOUNT, '') <> ISNULL(SRC.INVENTORY_ACCOUNT, '')
					 OR ISNULL(DST.ADJUSTMENT_ACCOUNT, '') <> ISNULL(SRC.ADJUSTMENT_ACCOUNT, '')
					 OR ISNULL(DST.COG_ACCOUNT, '') <> ISNULL(SRC.COG_ACCOUNT, '')
					 OR ISNULL(DST.INTENT_TO_EDIT, '') <> ISNULL(SRC.INTENT_TO_EDIT, '')
					 OR ISNULL(DST.PRICE_1, '') <> ISNULL(SRC.PRICE_1, '')
					 OR ISNULL(DST.PRICE_2, '') <> ISNULL(SRC.PRICE_2, '')
					 OR ISNULL(DST.PRICE_3, '') <> ISNULL(SRC.PRICE_3, '')
					 OR ISNULL(DST.COMPLIMENTARY, '') <> ISNULL(SRC.COMPLIMENTARY, '')
					 OR ISNULL(DST.ATTRIBUTES, '') <> ISNULL(SRC.ATTRIBUTES, '')
					 OR ISNULL(DST.PST_TAXABLE, '') <> ISNULL(SRC.PST_TAXABLE, '')
					 OR ISNULL(DST.TAXABLE_VALUE, '') <> ISNULL(SRC.TAXABLE_VALUE, '')
					 OR ISNULL(DST.ORG_CODE, '') <> ISNULL(SRC.ORG_CODE, '')
					 OR ISNULL(DST.TAX_AUTHORITY, '') <> ISNULL(SRC.TAX_AUTHORITY, '')
					 OR ISNULL(DST.WEB_OPTION, '') <> ISNULL(SRC.WEB_OPTION, '')
					 OR ISNULL(DST.IMAGE_URL, '') <> ISNULL(SRC.IMAGE_URL, '')
					 OR ISNULL(DST.APPLY_IMAGE, '') <> ISNULL(SRC.APPLY_IMAGE, '')
					 OR ISNULL(DST.IS_KIT, '') <> ISNULL(SRC.IS_KIT, '')
					 OR ISNULL(DST.INFO_URL, '') <> ISNULL(SRC.INFO_URL, '')
					 OR ISNULL(DST.APPLY_INFO, '') <> ISNULL(SRC.APPLY_INFO, '')
					 OR ISNULL(DST.PLP_CODE, '') <> ISNULL(SRC.PLP_CODE, '')
					 OR ISNULL(DST.PROMOTE, '') <> ISNULL(SRC.PROMOTE, '')
					 OR ISNULL(DST.THUMBNAIL_URL, '') <> ISNULL(SRC.THUMBNAIL_URL, '')
					 OR ISNULL(DST.APPLY_THUMBNAIL, '') <> ISNULL(SRC.APPLY_THUMBNAIL, '')
					 OR ISNULL(DST.CATALOG_DESC, '') LIKE ISNULL(SRC.CATALOG_DESC, '')
					 OR ISNULL(DST.WEB_DESC, '') LIKE ISNULL(SRC.WEB_DESC, '')
					 OR ISNULL(DST.OTHER_DESC, '') LIKE ISNULL(SRC.OTHER_DESC, '')
					 OR ISNULL(DST.[LOCATION], '') <> ISNULL(SRC.[LOCATION], '')
					 OR ISNULL(DST.PREMIUM, '') <> ISNULL(SRC.PREMIUM, '')
					 OR ISNULL(DST.FAIR_MARKET_VALUE, 0) <> ISNULL(SRC.FAIR_MARKET_VALUE, 0)
					 OR ISNULL(DST.IS_FR_ITEM, '') <> ISNULL(SRC.IS_FR_ITEM, '')
					 OR ISNULL(DST.APPEAL_CODE, '') <> ISNULL(SRC.APPEAL_CODE, '')
					 OR ISNULL(DST.CAMPAIGN_CODE, '') <> ISNULL(SRC.CAMPAIGN_CODE, '')
					 OR ISNULL(DST.PRICE_FROM_COMPONENTS, '') <> ISNULL(SRC.PRICE_FROM_COMPONENTS, '')
					 OR ISNULL(DST.PUBLISH_START_DATE, '') <> ISNULL(SRC.PUBLISH_START_DATE, '')
					 OR ISNULL(DST.PUBLISH_END_DATE, '') <> ISNULL(SRC.PUBLISH_END_DATE, '')
					 OR ISNULL(DST.TAX_BY_LOCATION, '') <> ISNULL(SRC.TAX_BY_LOCATION, '')
					 OR ISNULL(DST.TAXCATEGORY_CODE, '') <> ISNULL(SRC.TAXCATEGORY_CODE, '')
					 OR ISNULL(DST.RELATED_CONTENT_MESSAGE, '') <> ISNULL(SRC.RELATED_CONTENT_MESSAGE, '')
					 OR ISNULL(DST.MINIMUM_GIFT_AMOUNT, '') <> ISNULL(SRC.MINIMUM_GIFT_AMOUNT, '')
					 OR ISNULL(DST.PRODUCTKEY, '') <> ISNULL(SRC.PRODUCTKEY, '')
					 OR ISNULL(DST.ALLOWORDERLINENOTE, '') <> ISNULL(SRC.ALLOWORDERLINENOTE, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [PRODUCT_CODE]
      ,[PRODUCT_MAJOR]
      ,[PRODUCT_MINOR]
      ,[PROD_TYPE]
      ,[CATEGORY]
      ,[TITLE_KEY]
      ,[TITLE]
      ,[DESCRIPTION]
      ,[STATUS]
      ,[NOTE]
      ,[GROUP_1]
      ,[GROUP_2]
      ,[GROUP_3]
      ,[PRICE_RULES_EXIST]
      ,[LOT_SERIAL_EXIST]
      ,[PAYMENT_PRIORITY]
      ,[RENEW_MONTHS]
      ,[PRORATE]
      ,[STOCK_ITEM]
      ,[UNIT_OF_MEASURE]
      ,[WEIGHT]
      ,[TAXABLE]
      ,[COMMISIONABLE]
      ,[COMMISION_PERCENT]
      ,[DECIMAL_POINTS]
      ,[INCOME_ACCOUNT]
      ,[DEFERRED_INCOME_ACCOUNT]
      ,[INVENTORY_ACCOUNT]
      ,[ADJUSTMENT_ACCOUNT]
      ,[COG_ACCOUNT]
      ,[INTENT_TO_EDIT]
      ,[PRICE_1]
      ,[PRICE_2]
      ,[PRICE_3]
      ,[COMPLIMENTARY]
      ,[ATTRIBUTES]
      ,[PST_TAXABLE]
      ,[TAXABLE_VALUE]
      ,[ORG_CODE]
      ,[TAX_AUTHORITY]
      ,[WEB_OPTION]
      ,[IMAGE_URL]
      ,[APPLY_IMAGE]
      ,[IS_KIT]
      ,[INFO_URL]
      ,[APPLY_INFO]
      ,[PLP_CODE]
      ,[PROMOTE]
      ,[THUMBNAIL_URL]
      ,[APPLY_THUMBNAIL]
      ,[CATALOG_DESC]
      ,[WEB_DESC]
      ,[OTHER_DESC]
      ,[LOCATION]
      ,[PREMIUM]
      ,[FAIR_MARKET_VALUE]
      ,[IS_FR_ITEM]
      ,[APPEAL_CODE]
      ,[CAMPAIGN_CODE]
      ,[PRICE_FROM_COMPONENTS]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[TAX_BY_LOCATION]
      ,[TAXCATEGORY_CODE]
      ,[RELATED_CONTENT_MESSAGE]
      ,[MINIMUM_GIFT_AMOUNT]
      ,[ProductKey]
      ,[AllowOrderLineNote]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
       SRC.[PRODUCT_CODE]
      ,SRC.[PRODUCT_MAJOR]
      ,SRC.[PRODUCT_MINOR]
      ,SRC.[PROD_TYPE]
      ,SRC.[CATEGORY]
      ,SRC.[TITLE_KEY]
      ,SRC.[TITLE]
      ,SRC.[DESCRIPTION]
      ,SRC.[STATUS]
      ,SRC.[NOTE]
      ,SRC.[GROUP_1]
      ,SRC.[GROUP_2]
      ,SRC.[GROUP_3]
      ,SRC.[PRICE_RULES_EXIST]
      ,SRC.[LOT_SERIAL_EXIST]
      ,SRC.[PAYMENT_PRIORITY]
      ,SRC.[RENEW_MONTHS]
      ,SRC.[PRORATE]
      ,SRC.[STOCK_ITEM]
      ,SRC.[UNIT_OF_MEASURE]
      ,SRC.[WEIGHT]
      ,SRC.[TAXABLE]
      ,SRC.[COMMISIONABLE]
      ,SRC.[COMMISION_PERCENT]
      ,SRC.[DECIMAL_POINTS]
      ,SRC.[INCOME_ACCOUNT]
      ,SRC.[DEFERRED_INCOME_ACCOUNT]
      ,SRC.[INVENTORY_ACCOUNT]
      ,SRC.[ADJUSTMENT_ACCOUNT]
      ,SRC.[COG_ACCOUNT]
      ,SRC.[INTENT_TO_EDIT]
      ,SRC.[PRICE_1]
      ,SRC.[PRICE_2]
      ,SRC.[PRICE_3]
      ,SRC.[COMPLIMENTARY]
      ,SRC.[ATTRIBUTES]
      ,SRC.[PST_TAXABLE]
      ,SRC.[TAXABLE_VALUE]
      ,SRC.[ORG_CODE]
      ,SRC.[TAX_AUTHORITY]
      ,SRC.[WEB_OPTION]
      ,SRC.[IMAGE_URL]
      ,SRC.[APPLY_IMAGE]
      ,SRC.[IS_KIT]
      ,SRC.[INFO_URL]
      ,SRC.[APPLY_INFO]
      ,SRC.[PLP_CODE]
      ,SRC.[PROMOTE]
      ,SRC.[THUMBNAIL_URL]
      ,SRC.[APPLY_THUMBNAIL]
      ,SRC.[CATALOG_DESC]
      ,SRC.[WEB_DESC]
      ,SRC.[OTHER_DESC]
      ,SRC.[LOCATION]
      ,SRC.[PREMIUM]
      ,SRC.[FAIR_MARKET_VALUE]
      ,SRC.[IS_FR_ITEM]
      ,SRC.[APPEAL_CODE]
      ,SRC.[CAMPAIGN_CODE]
      ,SRC.[PRICE_FROM_COMPONENTS]
      ,SRC.[PUBLISH_START_DATE]
      ,SRC.[PUBLISH_END_DATE]
      ,SRC.[TAX_BY_LOCATION]
      ,SRC.[TAXCATEGORY_CODE]
      ,SRC.[RELATED_CONTENT_MESSAGE]
      ,SRC.[MINIMUM_GIFT_AMOUNT]
      ,SRC.[ProductKey]
      ,SRC.[AllowOrderLineNote]
      ,SRC.[TIME_STAMP]
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.PRODUCT_CODE
       ,deleted.PRODUCT_CODE
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Product'
       ,'stg.imis_Product'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_Product
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Product (
 [PRODUCT_CODE]
      ,[PRODUCT_MAJOR]
      ,[PRODUCT_MINOR]
      ,[PROD_TYPE]
      ,[CATEGORY]
      ,[TITLE_KEY]
      ,[TITLE]
      ,[DESCRIPTION]
      ,[STATUS]
      ,[NOTE]
      ,[GROUP_1]
      ,[GROUP_2]
      ,[GROUP_3]
      ,[PRICE_RULES_EXIST]
      ,[LOT_SERIAL_EXIST]
      ,[PAYMENT_PRIORITY]
      ,[RENEW_MONTHS]
      ,[PRORATE]
      ,[STOCK_ITEM]
      ,[UNIT_OF_MEASURE]
      ,[WEIGHT]
      ,[TAXABLE]
      ,[COMMISIONABLE]
      ,[COMMISION_PERCENT]
      ,[DECIMAL_POINTS]
      ,[INCOME_ACCOUNT]
      ,[DEFERRED_INCOME_ACCOUNT]
      ,[INVENTORY_ACCOUNT]
      ,[ADJUSTMENT_ACCOUNT]
      ,[COG_ACCOUNT]
      ,[INTENT_TO_EDIT]
      ,[PRICE_1]
      ,[PRICE_2]
      ,[PRICE_3]
      ,[COMPLIMENTARY]
      ,[ATTRIBUTES]
      ,[PST_TAXABLE]
      ,[TAXABLE_VALUE]
      ,[ORG_CODE]
      ,[TAX_AUTHORITY]
      ,[WEB_OPTION]
      ,[IMAGE_URL]
      ,[APPLY_IMAGE]
      ,[IS_KIT]
      ,[INFO_URL]
      ,[APPLY_INFO]
      ,[PLP_CODE]
      ,[PROMOTE]
      ,[THUMBNAIL_URL]
      ,[APPLY_THUMBNAIL]
      ,[CATALOG_DESC]
      ,[WEB_DESC]
      ,[OTHER_DESC]
      ,[LOCATION]
      ,[PREMIUM]
      ,[FAIR_MARKET_VALUE]
      ,[IS_FR_ITEM]
      ,[APPEAL_CODE]
      ,[CAMPAIGN_CODE]
      ,[PRICE_FROM_COMPONENTS]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[TAX_BY_LOCATION]
      ,[TAXCATEGORY_CODE]
      ,[RELATED_CONTENT_MESSAGE]
      ,[MINIMUM_GIFT_AMOUNT]
      ,[ProductKey]
      ,[AllowOrderLineNote]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
       [PRODUCT_CODE]
      ,[PRODUCT_MAJOR]
      ,[PRODUCT_MINOR]
      ,[PROD_TYPE]
      ,[CATEGORY]
      ,[TITLE_KEY]
      ,[TITLE]
      ,[DESCRIPTION]
      ,[STATUS]
      ,[NOTE]
      ,[GROUP_1]
      ,[GROUP_2]
      ,[GROUP_3]
      ,[PRICE_RULES_EXIST]
      ,[LOT_SERIAL_EXIST]
      ,[PAYMENT_PRIORITY]
      ,[RENEW_MONTHS]
      ,[PRORATE]
      ,[STOCK_ITEM]
      ,[UNIT_OF_MEASURE]
      ,[WEIGHT]
      ,[TAXABLE]
      ,[COMMISIONABLE]
      ,[COMMISION_PERCENT]
      ,[DECIMAL_POINTS]
      ,[INCOME_ACCOUNT]
      ,[DEFERRED_INCOME_ACCOUNT]
      ,[INVENTORY_ACCOUNT]
      ,[ADJUSTMENT_ACCOUNT]
      ,[COG_ACCOUNT]
      ,[INTENT_TO_EDIT]
      ,[PRICE_1]
      ,[PRICE_2]
      ,[PRICE_3]
      ,[COMPLIMENTARY]
      ,[ATTRIBUTES]
      ,[PST_TAXABLE]
      ,[TAXABLE_VALUE]
      ,[ORG_CODE]
      ,[TAX_AUTHORITY]
      ,[WEB_OPTION]
      ,[IMAGE_URL]
      ,[APPLY_IMAGE]
      ,[IS_KIT]
      ,[INFO_URL]
      ,[APPLY_INFO]
      ,[PLP_CODE]
      ,[PROMOTE]
      ,[THUMBNAIL_URL]
      ,[APPLY_THUMBNAIL]
      ,[CATALOG_DESC]
      ,[WEB_DESC]
      ,[OTHER_DESC]
      ,[LOCATION]
      ,[PREMIUM]
      ,[FAIR_MARKET_VALUE]
      ,[IS_FR_ITEM]
      ,[APPEAL_CODE]
      ,[CAMPAIGN_CODE]
      ,[PRICE_FROM_COMPONENTS]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[TAX_BY_LOCATION]
      ,[TAXCATEGORY_CODE]
      ,[RELATED_CONTENT_MESSAGE]
      ,[MINIMUM_GIFT_AMOUNT]
      ,[ProductKey]
      ,[AllowOrderLineNote]
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Product
WHERE PRODUCT_CODE IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp

/*******************************************************UD_TABLE***********************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_UD_Table AS DST
USING tmp.imis_UD_Table AS SRC
       ON DST.TABLE_NAME = SRC.TABLE_NAME
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.[APPLICATION], '') <> ISNULL(SRC.[APPLICATION], '')
                     OR ISNULL(DST.EXTERNAL_FLAG, '') <> ISNULL(SRC.EXTERNAL_FLAG, '')
                     OR ISNULL(DST.LINK_VIA, '') <> ISNULL(SRC.LINK_VIA, '')
                     OR ISNULL(DST.ALLOW_MULTIPLE_INSTANCES, '') <> ISNULL(SRC.ALLOW_MULTIPLE_INSTANCES, '')
                     OR ISNULL(DST.[REQUIRED], '') <> ISNULL(SRC.[REQUIRED], '')
                     OR ISNULL(DST.EDIT_TYPES, '') <> ISNULL(SRC.EDIT_TYPES, '')
                     OR ISNULL(DST.NAME_ALL_TABLE, '') <> ISNULL(SRC.NAME_ALL_TABLE, '')
                
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [TABLE_NAME]
      ,[DESCRIPTION]
      ,[APPLICATION]
      ,[EXTERNAL_FLAG]
      ,[LINK_VIA]
      ,[ALLOW_MULTIPLE_INSTANCES]
      ,[REQUIRED]
      ,[EDIT_TYPES]
      ,[NAME_ALL_TABLE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                    [TABLE_NAME]
      ,[DESCRIPTION]
      ,[APPLICATION]
      ,[EXTERNAL_FLAG]
      ,[LINK_VIA]
      ,[ALLOW_MULTIPLE_INSTANCES]
      ,[REQUIRED]
      ,[EDIT_TYPES]
      ,[NAME_ALL_TABLE]
      ,[TIME_STAMP]
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.TABLE_NAME
       ,deleted.TABLE_NAME
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_UD_Table'
       ,'stg.imis_UD_Table'
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
		 ,(select count(*) as RowsRead
				from tmp.imis_UD_Table
				)
       ,getdate()
       )

INSERT INTO stg.imis_UD_Table (
[TABLE_NAME]
      ,[DESCRIPTION]
      ,[APPLICATION]
      ,[EXTERNAL_FLAG]
      ,[LINK_VIA]
      ,[ALLOW_MULTIPLE_INSTANCES]
      ,[REQUIRED]
      ,[EDIT_TYPES]
      ,[NAME_ALL_TABLE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate] 
                 )
select  
                     [TABLE_NAME]
      ,[DESCRIPTION]
      ,[APPLICATION]
      ,[EXTERNAL_FLAG]
      ,[LINK_VIA]
      ,[ALLOW_MULTIPLE_INSTANCES]
      ,[REQUIRED]
      ,[EDIT_TYPES]
      ,[NAME_ALL_TABLE]
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_UD_Table
WHERE TABLE_NAME IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp



/**************************************************ContactMain******************************/
--MERGE stg.imis_ContactMain AS DST
--USING tmp.imis_ContactMain AS SRC
--       ON DST.ContactKey = SRC.ContactKey
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.CONTACTSTATUSCODE, '') <> ISNULL(SRC.CONTACTSTATUSCODE, '')
--                     OR ISNULL(DST.FULLNAME, '') <> ISNULL(SRC.FULLNAME, '')
--                     OR ISNULL(DST.SORTNAME, '') <> ISNULL(SRC.SORTNAME, '')
--                     OR ISNULL(DST.ISINSTITUTE, '') <> ISNULL(SRC.ISINSTITUTE, '')
--                     OR ISNULL(DST.TAXIDNUMBER, '') <> ISNULL(SRC.TAXIDNUMBER, '')
--					 OR ISNULL(DST.NOSOLICITATIONFLAG, '') <> ISNULL(SRC.NOSOLICITATIONFLAG, '')
--					 OR ISNULL(DST.SYNCCONTACTID, '') <> ISNULL(SRC.SYNCCONTACTID, '')
--					 OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
--					 OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
--					 OR ISNULL(DST.ISIDEDITABLE, '') <> ISNULL(SRC.ISIDEDITABLE, '')
--					 OR ISNULL(DST.ID, '') <> ISNULL(SRC.ID, '')
--					 OR ISNULL(DST.PREFERREDADDRESSCATEGORYCODE, '') <> ISNULL(SRC.PREFERREDADDRESSCATEGORYCODE, '')
--					 OR ISNULL(DST.ISSORTNAMEOVERRIDDEN, '') <> ISNULL(SRC.ISSORTNAMEOVERRIDDEN, '')
--					 OR ISNULL(DST.PRIMARYMEMBERSHIPGROUPKEY, '') <> ISNULL(SRC.PRIMARYMEMBERSHIPGROUPKEY, '')
--					 OR ISNULL(DST.MAJORKEY, '') <> ISNULL(SRC.MAJORKEY, '')
--					 OR ISNULL(DST.ACCESSKEY, '') <> ISNULL(SRC.ACCESSKEY, '')
--					 OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
--					 OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
--					 OR ISNULL(DST.TEXTONLYEMAILFLAG, '') <> ISNULL(SRC.TEXTONLYEMAILFLAG, '')
--					 OR ISNULL(DST.CONTACTTYPEKEY, '') <> ISNULL(SRC.CONTACTTYPEKEY, '')
--					 OR ISNULL(DST.OPTOUTFLAG, '') <> ISNULL(SRC.OPTOUTFLAG, '')
--					 OR ISNULL(DST.MARKEDFORDELETEON, '') <> ISNULL(SRC.MARKEDFORDELETEON, '')
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                    [ContactKey]
--      ,[ContactStatusCode]
--      ,[FullName]
--      ,[SortName]
--      ,[IsInstitute]
--      ,[TaxIDNumber]
--      ,[NoSolicitationFlag]
--      ,[SyncContactID]
--      ,[UpdatedOn]
--      ,[UpdatedByUserKey]
--      ,[IsIDEditable]
--      ,[ID]
--      ,[PreferredAddressCategoryCode]
--      ,[IsSortNameOverridden]
--      ,[PrimaryMembershipGroupKey]
--      ,[MajorKey]
--      ,[AccessKey]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[TextOnlyEmailFlag]
--      ,[ContactTypeKey]
--      ,[OptOutFlag]
--      ,[MarkedForDeleteOn]
--      ,[IsActive]
--      ,[StartDate]
 
          
--                     )
--              VALUES (
--                   SRC.[ContactKey]
--      ,SRC.[ContactStatusCode]
--      ,SRC.[FullName]
--      ,SRC.[SortName]
--      ,SRC.[IsInstitute]
--      ,SRC.[TaxIDNumber]
--      ,SRC.[NoSolicitationFlag]
--      ,SRC.[SyncContactID]
--      ,SRC.[UpdatedOn]
--      ,SRC.[UpdatedByUserKey]
--      ,SRC.[IsIDEditable]
--      ,SRC.[ID]
--      ,SRC.[PreferredAddressCategoryCode]
--      ,SRC.[IsSortNameOverridden]
--      ,SRC.[PrimaryMembershipGroupKey]
--      ,SRC.[MajorKey]
--      ,SRC.[AccessKey]
--      ,SRC.[CreatedByUserKey]
--      ,SRC.[CreatedOn]
--      ,SRC.[TextOnlyEmailFlag]
--      ,SRC.[ContactTypeKey]
--      ,SRC.[OptOutFlag]
--      ,SRC.[MarkedForDeleteOn]
--	  ,1
--      ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.contactkey
--       ,deleted.contactkey
--INTO #audittemp;



--INSERT INTO etl.executionlog
--VALUES (
--		'@PipelineName'
--		,'tmp.imis_ContactMain'       
--       ,'stg.imis_ContactMain'
--	          ,(
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
--		)
--		,
--		(select count(*) as RowsRead
--		from tmp.imis_ContactMain
--		)
--		,getdate()
--       )

--INSERT INTO stg.ContactMain (
--[ContactKey]
--      ,[ContactStatusCode]
--      ,[FullName]
--      ,[SortName]
--      ,[IsInstitute]
--      ,[TaxIDNumber]
--      ,[NoSolicitationFlag]
--      ,[SyncContactID]
--      ,[UpdatedOn]
--      ,[UpdatedByUserKey]
--      ,[IsIDEditable]
--      ,[ID]
--      ,[PreferredAddressCategoryCode]
--      ,[IsSortNameOverridden]
--      ,[PrimaryMembershipGroupKey]
--      ,[MajorKey]
--      ,[AccessKey]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[TextOnlyEmailFlag]
--      ,[ContactTypeKey]
--      ,[OptOutFlag]
--      ,[MarkedForDeleteOn]
--      ,[IsActive]
--      ,[StartDate]
 
--                 )
--select  
--                    [ContactKey]
--      ,[ContactStatusCode]
--      ,[FullName]
--      ,[SortName]
--      ,[IsInstitute]
--      ,[TaxIDNumber]
--      ,[NoSolicitationFlag]
--      ,[SyncContactID]
--      ,[UpdatedOn]
--      ,[UpdatedByUserKey]
--      ,[IsIDEditable]
--      ,[ID]
--      ,[PreferredAddressCategoryCode]
--      ,[IsSortNameOverridden]
--      ,[PrimaryMembershipGroupKey]
--      ,[MajorKey]
--      ,[AccessKey]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[TextOnlyEmailFlag]
--      ,[ContactTypeKey]
--      ,[OptOutFlag]
--      ,[MarkedForDeleteOn]
--      ,1
--      ,@Today
           
         
--FROM 
--tmp.imis_ContactMain
--WHERE ContactKey IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )

--			  Truncate TABLE #audittemp



/*************************************************USER MAIN*******************************/
--MERGE stg.imis_UserMain AS DST
--USING tmp.imis_UserMain AS SRC
--       ON DST.USERKEY = SRC.USERKEY 
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.CONTACTMASTER, '') <> ISNULL(SRC.CONTACTMASTER, '')
--                     OR ISNULL(DST.USERID, '') <> ISNULL(SRC.USERID, '')
--                     OR ISNULL(DST.ISDISABLED, '') <> ISNULL(SRC.ISDISABLED, '')
--                     OR ISNULL(DST.EFFECTIVEDATE, '') <> ISNULL(SRC.EFFECTIVEDATE, '')
--                     OR ISNULL(DST.EXPIRATIONDATE, '') <> ISNULL(SRC.EXPIRATIONDATE, '')
--                     OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
--                     OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
--                     OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
--                     OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
--					 OR ISNULL(DST.MARKEDFORDELETEON, '') <> ISNULL(SRC.MARKEDFORDELETEON, '')
--					 OR ISNULL(DST.DEFAULTDEPARTMENTGROUPKEY, '') <> ISNULL(SRC.DEFAULTDEPARTMENTGROUPKEY, '')
--					 OR ISNULL(DST.DEFAULTPERSPECTIVEKEY, '') <> ISNULL(SRC.DEFAULTPERSPECTIVEKEY, '')
--					 OR ISNULL(DST.PROVIDERKEY, '') <> ISNULL(SRC.PROVIDERKEY, '')
--					 OR ISNULL(DST.MULTIFACTORINFO, '') <> ISNULL(SRC.MULTIFACTORINFO, '')
                     
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [UserKey]
--      ,[ContactMaster]
--      ,[UserId]
--      ,[IsDisabled]
--      ,[EffectiveDate]
--      ,[ExpirationDate]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[MarkedForDeleteOn]
--      ,[DefaultDepartmentGroupKey]
--      ,[DefaultPerspectiveKey]
--      ,[ProviderKey]
--      ,[MultiFactorInfo]
--      ,[IsActive]
--      ,[StartDate]

          
--                     )
--              VALUES (
--                    SRC.[UserKey]
--      ,SRC.[ContactMaster]
--      ,SRC.[UserId]
--      ,SRC.[IsDisabled]
--      ,SRC.[EffectiveDate]
--      ,SRC.[ExpirationDate]
--      ,SRC.[UpdatedByUserKey]
--      ,SRC.[UpdatedOn]
--      ,SRC.[CreatedByUserKey]
--      ,SRC.[CreatedOn]
--      ,SRC.[MarkedForDeleteOn]
--      ,SRC.[DefaultDepartmentGroupKey]
--      ,SRC.[DefaultPerspectiveKey]
--      ,SRC.[ProviderKey]
--      ,SRC.[MultiFactorInfo]
--                 ,1
--                 ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.USERKEY
--       ,deleted.USERKEY
--INTO #audittemp;



--INSERT INTO etl.executionlog
--VALUES (
--		'@Pipelinename'
--	   ,'tmp.imis_UserMain'
--       ,'stg.imis_UserMain'
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
--					 )
--			,(select count(*) as RowsRead
--				from tmp.imis_UserMain
--				)
--       ,getdate()
--       )

--INSERT INTO stg.imis_UserMain (
--[UserKey]
--      ,[ContactMaster]
--      ,[UserId]
--      ,[IsDisabled]
--      ,[EffectiveDate]
--      ,[ExpirationDate]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[MarkedForDeleteOn]
--      ,[DefaultDepartmentGroupKey]
--      ,[DefaultPerspectiveKey]
--      ,[ProviderKey]
--      ,[MultiFactorInfo]
--      ,[IsActive]
--      ,[StartDate]

--                 )
--select  
--                    [UserKey]
--      ,[ContactMaster]
--      ,[UserId]
--      ,[IsDisabled]
--      ,[EffectiveDate]
--      ,[ExpirationDate]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[MarkedForDeleteOn]
--      ,[DefaultDepartmentGroupKey]
--      ,[DefaultPerspectiveKey]
--      ,[ProviderKey]
--      ,[MultiFactorInfo]
--      ,1
--      ,@Today
           
         
--FROM 
--tmp.imis_UserMain
--WHERE userkey IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp


/**************************************************Member_Types**************************************/
MERGE stg.imis_Member_Types AS DST
USING tmp.imis_Member_Types AS SRC
       ON DST.member_type = SRC.member_type 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.MEMBER_RECORD, '') <> ISNULL(SRC.MEMBER_RECORD, '')
                     OR ISNULL(DST.COMPANY_RECORD, '') <> ISNULL(SRC.COMPANY_RECORD, '')
                     OR ISNULL(DST.DUES_CODE_1, '') <> ISNULL(SRC.DUES_CODE_1, '')
                     OR ISNULL(DST.DUES_CODE_2, '') <> ISNULL(SRC.DUES_CODE_2, '')
                     OR ISNULL(DST.DUES_CODE_3, '') <> ISNULL(SRC.DUES_CODE_3, '')
                     OR ISNULL(DST.DUES_CODE_4, '') <> ISNULL(SRC.DUES_CODE_4, '')
                     OR ISNULL(DST.DUES_CODE_5, '') <> ISNULL(SRC.DUES_CODE_5, '')
                     OR ISNULL(DST.DUES_CODE_6, '') <> ISNULL(SRC.DUES_CODE_6, '')
					 OR ISNULL(DST.DUES_CODE_7, '') <> ISNULL(SRC.DUES_CODE_7, '')
					 OR ISNULL(DST.DUES_CODE_8, '') <> ISNULL(SRC.DUES_CODE_8, '')
					 OR ISNULL(DST.DUES_CODE_9, '') <> ISNULL(SRC.DUES_CODE_9, '')
					 OR ISNULL(DST.DUES_CODE_10, '') <> ISNULL(SRC.DUES_CODE_10, '')
					 OR ISNULL(DST.RATE_1, 0) <> ISNULL(SRC.RATE_1, 0)
                     OR ISNULL(DST.RATE_2, 0) <> ISNULL(SRC.RATE_2, 0)
					 OR ISNULL(DST.RATE_3, 0) <> ISNULL(SRC.RATE_3, 0)
					 OR ISNULL(DST.RATE_4, 0) <> ISNULL(SRC.RATE_4, 0)
					 OR ISNULL(DST.RATE_5, 0) <> ISNULL(SRC.RATE_5, 0)
					 OR ISNULL(DST.RATE_6, 0) <> ISNULL(SRC.RATE_6, 0)
					 OR ISNULL(DST.RATE_7, 0) <> ISNULL(SRC.RATE_7, 0)
					 OR ISNULL(DST.RATE_8, 0) <> ISNULL(SRC.RATE_8, 0)
					 OR ISNULL(DST.RATE_9, 0) <> ISNULL(SRC.RATE_9, 0)
					 OR ISNULL(DST.RATE_10, 0) <> ISNULL(SRC.RATE_10, 0)
					 OR ISNULL(DST.BILL_COMPANY, '') <> ISNULL(SRC.BILL_COMPANY, '')
					 OR ISNULL(DST.SPECIAL_FORM, '') <> ISNULL(SRC.SPECIAL_FORM, '')
					 OR ISNULL(DST.COMP_1, '') <> ISNULL(SRC.COMP_1, '')
					 OR ISNULL(DST.COMP_2, '') <> ISNULL(SRC.COMP_2, '')
					 OR ISNULL(DST.COMP_3, '') <> ISNULL(SRC.COMP_3, '')
					 OR ISNULL(DST.COMP_4, '') <> ISNULL(SRC.COMP_4, '')
					 OR ISNULL(DST.COMP_5, '') <> ISNULL(SRC.COMP_5, '')
					 OR ISNULL(DST.COMP_6, '') <> ISNULL(SRC.COMP_6, '')
					 OR ISNULL(DST.COMP_7, '') <> ISNULL(SRC.COMP_7, '')
					 OR ISNULL(DST.COMP_8, '') <> ISNULL(SRC.COMP_8, '')
					 OR ISNULL(DST.COMP_9, '') <> ISNULL(SRC.COMP_9, '')
					 OR ISNULL(DST.COMP_10, '') <> ISNULL(SRC.COMP_10, '')
					 OR ISNULL(DST.DEFAULT_SECURITY_GROUP, '') <> ISNULL(SRC.DEFAULT_SECURITY_GROUP, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [MEMBER_TYPE]
      ,[DESCRIPTION]
      ,[MEMBER_RECORD]
      ,[COMPANY_RECORD]
      ,[DUES_CODE_1]
      ,[DUES_CODE_2]
      ,[DUES_CODE_3]
      ,[DUES_CODE_4]
      ,[DUES_CODE_5]
      ,[DUES_CODE_6]
      ,[DUES_CODE_7]
      ,[DUES_CODE_8]
      ,[DUES_CODE_9]
      ,[DUES_CODE_10]
      ,[RATE_1]
      ,[RATE_2]
      ,[RATE_3]
      ,[RATE_4]
      ,[RATE_5]
      ,[RATE_6]
      ,[RATE_7]
      ,[RATE_8]
      ,[RATE_9]
      ,[RATE_10]
      ,[BILL_COMPANY]
      ,[SPECIAL_FORM]
      ,[COMP_1]
      ,[COMP_2]
      ,[COMP_3]
      ,[COMP_4]
      ,[COMP_5]
      ,[COMP_6]
      ,[COMP_7]
      ,[COMP_8]
      ,[COMP_9]
      ,[COMP_10]
      ,[DEFAULT_SECURITY_GROUP]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                      src.[MEMBER_TYPE]
      ,src.[DESCRIPTION]
      ,src.[MEMBER_RECORD]
      ,src.[COMPANY_RECORD]
      ,src.[DUES_CODE_1]
      ,src.[DUES_CODE_2]
      ,src.[DUES_CODE_3]
      ,src.[DUES_CODE_4]
      ,src.[DUES_CODE_5]
      ,src.[DUES_CODE_6]
      ,src.[DUES_CODE_7]
      ,src.[DUES_CODE_8]
      ,src.[DUES_CODE_9]
      ,src.[DUES_CODE_10]
      ,src.[RATE_1]
      ,src.[RATE_2]
      ,src.[RATE_3]
      ,src.[RATE_4]
      ,src.[RATE_5]
      ,src.[RATE_6]
      ,src.[RATE_7]
      ,src.[RATE_8]
      ,src.[RATE_9]
      ,src.[RATE_10]
      ,src.[BILL_COMPANY]
      ,src.[SPECIAL_FORM]
      ,src.[COMP_1]
      ,src.[COMP_2]
      ,src.[COMP_3]
      ,src.[COMP_4]
      ,src.[COMP_5]
      ,src.[COMP_6]
      ,src.[COMP_7]
      ,src.[COMP_8]
      ,src.[COMP_9]
      ,src.[COMP_10]
      ,src.[DEFAULT_SECURITY_GROUP]
      ,src.[TIME_STAMP]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.member_type
       ,deleted.member_type
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_member_types'
       ,'stg.imis_member_types'
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
			,(select count(*) as RowsRead
				from tmp.imis_member_types
				) 
       ,getdate()
       )

INSERT INTO stg.imis_member_types (
 [MEMBER_TYPE]
      ,[DESCRIPTION]
      ,[MEMBER_RECORD]
      ,[COMPANY_RECORD]
      ,[DUES_CODE_1]
      ,[DUES_CODE_2]
      ,[DUES_CODE_3]
      ,[DUES_CODE_4]
      ,[DUES_CODE_5]
      ,[DUES_CODE_6]
      ,[DUES_CODE_7]
      ,[DUES_CODE_8]
      ,[DUES_CODE_9]
      ,[DUES_CODE_10]
      ,[RATE_1]
      ,[RATE_2]
      ,[RATE_3]
      ,[RATE_4]
      ,[RATE_5]
      ,[RATE_6]
      ,[RATE_7]
      ,[RATE_8]
      ,[RATE_9]
      ,[RATE_10]
      ,[BILL_COMPANY]
      ,[SPECIAL_FORM]
      ,[COMP_1]
      ,[COMP_2]
      ,[COMP_3]
      ,[COMP_4]
      ,[COMP_5]
      ,[COMP_6]
      ,[COMP_7]
      ,[COMP_8]
      ,[COMP_9]
      ,[COMP_10]
      ,[DEFAULT_SECURITY_GROUP]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                     [MEMBER_TYPE]
      ,[DESCRIPTION]
      ,[MEMBER_RECORD]
      ,[COMPANY_RECORD]
      ,[DUES_CODE_1]
      ,[DUES_CODE_2]
      ,[DUES_CODE_3]
      ,[DUES_CODE_4]
      ,[DUES_CODE_5]
      ,[DUES_CODE_6]
      ,[DUES_CODE_7]
      ,[DUES_CODE_8]
      ,[DUES_CODE_9]
      ,[DUES_CODE_10]
      ,[RATE_1]
      ,[RATE_2]
      ,[RATE_3]
      ,[RATE_4]
      ,[RATE_5]
      ,[RATE_6]
      ,[RATE_7]
      ,[RATE_8]
      ,[RATE_9]
      ,[RATE_10]
      ,[BILL_COMPANY]
      ,[SPECIAL_FORM]
      ,[COMP_1]
      ,[COMP_2]
      ,[COMP_3]
      ,[COMP_4]
      ,[COMP_5]
      ,[COMP_6]
      ,[COMP_7]
      ,[COMP_8]
      ,[COMP_9]
      ,[COMP_10]
      ,[DEFAULT_SECURITY_GROUP]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_member_types
WHERE member_type IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp


/*************************************************Csi_MergedRecords********************************/
--MERGE stg.imis_Csi_MergedRecords AS DST
--USING tmp.imis_Csi_MergedRecords AS SRC
--       ON DST.DuplicateID = SRC.DuplicateID and
--	   DST.MergeToID = SRC.MergeToID
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.USERNAME, '') <> ISNULL(SRC.USERNAME, '')
--                     OR ISNULL(DST.DATEOFMERGE, '') <> ISNULL(SRC.DATEOFMERGE, '')
          
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [DuplicateID]
--      ,[MergeToID]
--      ,[UserName]
--      ,[DateOfMerge]
--      ,[IsActive]
--      ,[StartDate]
          
--                     )
--              VALUES (
--                     src.[DuplicateID]
--      ,src.[MergeToID]
--      ,src.[UserName]
--      ,src.[DateOfMerge]
--      ,1
--      ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.DuplicateID
--       ,deleted.DuplicateID
--INTO #audittemp;



--INSERT INTO etl.executionlog
--VALUES (
--		'@PipelineName'
--	   ,'tmp.imis_Csi_MergedRecords'
--       ,'stg.imis_Csi_MergedRecords'
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
--					 )
--			,(select count(*) as RowsRead
--				from tmp.imis_Csi_MergedRecords
--				) 
--       ,getdate()
--       )

--INSERT INTO stg.imis_Csi_MergedRecords (
--[DuplicateID]
--      ,[MergeToID]
--      ,[UserName]
--      ,[DateOfMerge]
--      ,[IsActive]
--      ,[StartDate]

--                 )
--select  
--                   [DuplicateID]
--      ,[MergeToID]
--      ,[UserName]
--      ,[DateOfMerge]
--                 ,1
--                 ,@Today
           
         
--FROM 
--tmp.imis_Csi_MergedRecords
--WHERE DuplicateID IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp

/**********************************************GroupMember*********************************/
--MERGE stg.imis_GroupMember AS DST
--USING tmp.imis_GroupMember AS SRC
--       ON DST.GroupMemberKey = SRC.GroupMemberKey 
--WHEN MATCHED
--              AND DST.IsActive = 1
--              AND (
                   
--                     ISNULL(DST.GROUPKEY, '') <> ISNULL(SRC.GROUPKEY, '')
--                     OR ISNULL(DST.MEMBERCONTACTKEY, '') <> ISNULL(SRC.MEMBERCONTACTKEY, '')
--                     OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
--                     OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
--                     OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
--                     OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
--                     OR ISNULL(DST.DROPDATE, '') <> ISNULL(SRC.DROPDATE, '')
--                     OR ISNULL(DST.JOINDATE, '') <> ISNULL(SRC.JOINDATE, '')
--                     OR ISNULL(DST.[MarkedForDeleteOn], '') <> ISNULL(SRC.[MarkedForDeleteOn], '')
                     
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [GroupMemberKey]
--      ,[GroupKey]
--      ,[MemberContactKey]
--      ,[IsActive]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[DropDate]
--      ,[JoinDate]
--      ,[MarkedForDeleteOn]
--      ,[StartDate]
          
--                     )
--              VALUES (
--                      src.[GroupMemberKey]
--      ,src.[GroupKey]
--      ,src.[MemberContactKey]
--      ,1
--      ,src.[CreatedByUserKey]
--      ,src.[CreatedOn]
--      ,src.[UpdatedByUserKey]
--      ,src.[UpdatedOn]
--      ,src.[DropDate]
--      ,src.[JoinDate]
--      ,src.[MarkedForDeleteOn]
--      ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.GroupMemberKey
--       ,deleted.GroupMemberKey
--INTO #audittemp;



--INSERT INTO etl.executionlog
--VALUES (
--		'@PipelineName'
--	   ,'tmp.imis_GroupMember'
--       ,'stg.imis_GroupMember'
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
--					 )
--			,(select count(*) as RowsRead
--				from tmp.imis_GroupMember
--				)
--       ,getdate()
--       )

--INSERT INTO stg.imis_GroupMember (
-- [GroupMemberKey]
--      ,[GroupKey]
--      ,[MemberContactKey]
--      ,[IsActive]
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[DropDate]
--      ,[JoinDate]
--      ,[MarkedForDeleteOn]
--      ,[StartDate]
--                 )
--select  
--                     [GroupMemberKey]
--      ,[GroupKey]
--      ,[MemberContactKey]
--      ,1
--      ,[CreatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedByUserKey]
--      ,[UpdatedOn]
--      ,[DropDate]
--      ,[JoinDate]
--      ,[MarkedForDeleteOn]
--      ,@Today
           
         
--FROM 
--tmp.imis_GroupMember
--WHERE GroupMemberKey IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp



/********************************************GroupTypeRef***********************************************/
--MERGE stg.imis_GroupTypeRef AS DST
--USING tmp.imis_GroupTypeRef AS SRC
--       ON DST.GroupTypeKey = SRC.GroupTypeKey 
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.GROUPTYPENAME, '') <> ISNULL(SRC.GROUPTYPENAME, '')
--                     OR ISNULL(DST.ISSYSTEM, '') <> ISNULL(SRC.ISSYSTEM, '')
--                     OR ISNULL(DST.ISPAYMENTREQUIRED, '') <> ISNULL(SRC.ISPAYMENTREQUIRED, '')
--                     OR ISNULL(DST.ISDATELIMITED, '') <> ISNULL(SRC.ISDATELIMITED, '')
--                     OR ISNULL(DST.GROUPMEMBERBRANCHNAME, '') <> ISNULL(SRC.GROUPMEMBERBRANCHNAME, '')
--                     OR ISNULL(DST.ISINVITATIONONLY, '') <> ISNULL(SRC.ISINVITATIONONLY, '')
--                     OR ISNULL(DST.DEFAULTGROUPSTATUSCODE, '') <> ISNULL(SRC.DEFAULTGROUPSTATUSCODE, '')
--                     OR ISNULL(DST.ISSIMPLEGROUP, '') <> ISNULL(SRC.ISSIMPLEGROUP, '')
--                     OR ISNULL(DST.MEMBERQUERYFOLDERKEY, '') <> ISNULL(SRC.MEMBERQUERYFOLDERKEY, '')
--					 OR ISNULL(DST.INHERITROLESFLAG, '') <> ISNULL(SRC.INHERITROLESFLAG, '')
--					 OR ISNULL(DST.ISSINGLEROLE, '') <> ISNULL(SRC.ISSINGLEROLE, '')
--					 OR ISNULL(DST.GROUPTYPEDESC, '') <> ISNULL(SRC.GROUPTYPEDESC, '')
--					 OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
--					 OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
--					 OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
--					 OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
--					 OR ISNULL(DST.LANDINGPAGECONTENTKEY, '') <> ISNULL(SRC.LANDINGPAGECONTENTKEY, '')
--					 OR ISNULL(DST.ISRELATIONSHIPGROUP, '') <> ISNULL(SRC.ISRELATIONSHIPGROUP, '')
--					 OR ISNULL(DST.EXTENDACTIVEMEMBERSHIPTERMFLAG, '') <> ISNULL(SRC.EXTENDACTIVEMEMBERSHIPTERMFLAG, '')
                     
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [GroupTypeKey]
--      ,[GroupTypeName]
--      ,[IsSystem]
--      ,[IsPaymentRequired]
--      ,[IsDateLimited]
--      ,[GroupMemberBranchName]
--      ,[IsInvitationOnly]
--      ,[DefaultGroupStatusCode]
--      ,[IsSimpleGroup]
--      ,[MemberQueryFolderKey]
--      ,[InheritRolesFlag]
--      ,[IsSingleRole]
--      ,[GroupTypeDesc]
--      ,[CreatedByUserKey]
--      ,[UpdatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedOn]
--      ,[LandingPageContentKey]
--      ,[IsRelationshipGroup]
--      ,[ExtendActiveMembershipTermFlag]
--      ,[IsActive]
--      ,[StartDate]
          
--                     )
--              VALUES (
--                     src.[GroupTypeKey]
--      ,src.[GroupTypeName]
--      ,src.[IsSystem]
--      ,src.[IsPaymentRequired]
--      ,src.[IsDateLimited]
--      ,src.[GroupMemberBranchName]
--      ,src.[IsInvitationOnly]
--      ,src.[DefaultGroupStatusCode]
--      ,src.[IsSimpleGroup]
--      ,src.[MemberQueryFolderKey]
--      ,src.[InheritRolesFlag]
--      ,src.[IsSingleRole]
--      ,src.[GroupTypeDesc]
--      ,src.[CreatedByUserKey]
--      ,src.[UpdatedByUserKey]
--      ,src.[CreatedOn]
--      ,src.[UpdatedOn]
--      ,src.[LandingPageContentKey]
--      ,src.[IsRelationshipGroup]
--      ,src.[ExtendActiveMembershipTermFlag]
--                 ,1
--                 ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.GroupTypeKey
--       ,deleted.GroupTypeKey
--INTO #audittemp;



--INSERT INTO etl.executionlog
--VALUES (
--		'@PipelineName'
--	   ,'tmp.imis_GroupTypeRef'
--       ,'stg.imis_GroupTypeRef'
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
--					 )
--			,(select count(*) as RowsRead
--				from tmp.imis_GroupTypeRef
--				) 
--       ,getdate()
--       )

--INSERT INTO stg.imis_GroupTypeRef (
--[GroupTypeKey]
--      ,[GroupTypeName]
--      ,[IsSystem]
--      ,[IsPaymentRequired]
--      ,[IsDateLimited]
--      ,[GroupMemberBranchName]
--      ,[IsInvitationOnly]
--      ,[DefaultGroupStatusCode]
--      ,[IsSimpleGroup]
--      ,[MemberQueryFolderKey]
--      ,[InheritRolesFlag]
--      ,[IsSingleRole]
--      ,[GroupTypeDesc]
--      ,[CreatedByUserKey]
--      ,[UpdatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedOn]
--      ,[LandingPageContentKey]
--      ,[IsRelationshipGroup]
--      ,[ExtendActiveMembershipTermFlag]
--      ,[IsActive]
--      ,[StartDate]
--                 )
--select  
--                  [GroupTypeKey]
--      ,[GroupTypeName]
--      ,[IsSystem]
--      ,[IsPaymentRequired]
--      ,[IsDateLimited]
--      ,[GroupMemberBranchName]
--      ,[IsInvitationOnly]
--      ,[DefaultGroupStatusCode]
--      ,[IsSimpleGroup]
--      ,[MemberQueryFolderKey]
--      ,[InheritRolesFlag]
--      ,[IsSingleRole]
--      ,[GroupTypeDesc]
--      ,[CreatedByUserKey]
--      ,[UpdatedByUserKey]
--      ,[CreatedOn]
--      ,[UpdatedOn]
--      ,[LandingPageContentKey]
--      ,[IsRelationshipGroup]
--      ,[ExtendActiveMembershipTermFlag]
--                 ,1
--                 ,@Today
           
         
--FROM 
--tmp.imis_GroupTypeRef
--WHERE GroupTypeKey IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp




/*************************************************Meet_Master***********************************/
MERGE stg.imis_Meet_Master AS DST
USING tmp.imis_Meet_Master AS SRC
       ON DST.meeting = SRC.meeting
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
                     OR ISNULL(DST.MEETING_TYPE, '') <> ISNULL(SRC.MEETING_TYPE, '')
                     OR ISNULL(DST.[DESCRIPTION], '') LIKE ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.BEGIN_DATE, '') <> ISNULL(SRC.BEGIN_DATE, '')
                     OR ISNULL(DST.END_DATE, '') <> ISNULL(SRC.END_DATE, '')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.ADDRESS_1, '') <> ISNULL(SRC.ADDRESS_1, '')
                     OR ISNULL(DST.ADDRESS_2, '') <> ISNULL(SRC.ADDRESS_2, '')
                     OR ISNULL(DST.ADDRESS_3, '') <> ISNULL(SRC.ADDRESS_3, '')
					 OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
					 OR ISNULL(DST.STATE_PROVINCE, '') <> ISNULL(SRC.STATE_PROVINCE, '')
					 OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
					 OR ISNULL(DST.DIRECTIONS, '') LIKE ISNULL(SRC.DIRECTIONS, '')
					 OR ISNULL(DST.COORDINATORS, '') <> ISNULL(SRC.COORDINATORS, '')
					 OR ISNULL(DST.NOTES, '') LIKE ISNULL(SRC.NOTES, '')
					 OR ISNULL(DST.ALLOW_REG_STRING, '') LIKE ISNULL(SRC.ALLOW_REG_STRING, '')
					 OR ISNULL(DST.EARLY_CUTOFF, '') <> ISNULL(SRC.EARLY_CUTOFF, '')
					 OR ISNULL(DST.REG_CUTOFF, '') <> ISNULL(SRC.REG_CUTOFF, '')
					 OR ISNULL(DST.LATE_CUTOFF, '') <> ISNULL(SRC.LATE_CUTOFF, '')
					 OR ISNULL(DST.ORG_CODE, '') <> ISNULL(SRC.ORG_CODE, '')
					-- OR ISNULL(DST.LOGO, '') LIKE ISNULL(SRC.LOGO, '')
					 OR ISNULL(DST.MAX_REGISTRANTS, '') <> ISNULL(SRC.MAX_REGISTRANTS, '')
					 OR ISNULL(DST.TOTAL_REGISTRANTS, '') <> ISNULL(SRC.TOTAL_REGISTRANTS, '')
					 OR ISNULL(DST.TOTAL_CANCELATIONS, '') <> ISNULL(SRC.TOTAL_CANCELATIONS, '')
					 OR ISNULL(DST.TOTAL_REVENUE, 0) <> ISNULL(SRC.TOTAL_REVENUE, 0)
					 OR ISNULL(DST.HEAD_COUNT, '') <> ISNULL(SRC.HEAD_COUNT, '')
					 OR ISNULL(DST.TAX_AUTHORITY_1, '') <> ISNULL(SRC.TAX_AUTHORITY_1, '')
					 OR ISNULL(DST.[SUPPRESS_COOR], '') <> ISNULL(SRC.[SUPPRESS_COOR], '')
					 OR ISNULL(DST.SUPPRESS_DIR, '') <> ISNULL(SRC.SUPPRESS_DIR, '')
					 OR ISNULL(DST.SUPPRESS_NOTES, '') <> ISNULL(SRC.SUPPRESS_NOTES, '')
					 OR ISNULL(DST.MUF_1, '') <> ISNULL(SRC.MUF_1, '')
					 OR ISNULL(DST.MUF_2, '') <> ISNULL(SRC.MUF_2, '')
					 OR ISNULL(DST.MUF_3, '') <> ISNULL(SRC.MUF_3, '')
					 OR ISNULL(DST.MUF_4, '') <> ISNULL(SRC.MUF_4, '')
					 OR ISNULL(DST.MUF_5, '') <> ISNULL(SRC.MUF_5, '')
					 OR ISNULL(DST.MUF_6, '') <> ISNULL(SRC.MUF_6, '')
					 OR ISNULL(DST.MUF_7, '') <> ISNULL(SRC.MUF_7, '')
					 OR ISNULL(DST.MUF_8, '') <> ISNULL(SRC.MUF_8, '')
					 OR ISNULL(DST.MUF_9, '') <> ISNULL(SRC.MUF_9, '')
					 OR ISNULL(DST.MUF_10, '') <> ISNULL(SRC.MUF_10, '')
					 OR ISNULL(DST.INTENT_TO_EDIT, '') <> ISNULL(SRC.INTENT_TO_EDIT, '')
					 OR ISNULL(DST.SUPPRESS_CONFIRM, '') <> ISNULL(SRC.SUPPRESS_CONFIRM, '')
					 OR ISNULL(DST.WEB_VIEW_ONLY, '') <> ISNULL(SRC.WEB_VIEW_ONLY, '')
					 OR ISNULL(DST.WEB_ENABLED, '') <> ISNULL(SRC.WEB_ENABLED, '')
					 OR ISNULL(DST.POST_REGISTRATION, '') <> ISNULL(SRC.POST_REGISTRATION, '')
					 OR ISNULL(DST.EMAIL_REGISTRATION, '') <> ISNULL(SRC.EMAIL_REGISTRATION, '')
					 OR ISNULL(DST.MEETING_URL, '') <> ISNULL(SRC.MEETING_URL, '')
					 OR ISNULL(DST.MEETING_IMAGE_NAME, '') <> ISNULL(SRC.MEETING_IMAGE_NAME, '')
					 OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
					 OR ISNULL(DST.IS_FR_MEET, '') <> ISNULL(SRC.IS_FR_MEET, '')
					 OR ISNULL(DST.MEET_APPEAL, '') <> ISNULL(SRC.MEET_APPEAL, '')
					 OR ISNULL(DST.MEET_CAMPAIGN, '') <> ISNULL(SRC.MEET_CAMPAIGN, '')
					 OR ISNULL(DST.MEET_CATEGORY, '') <> ISNULL(SRC.MEET_CATEGORY, '')
					 OR ISNULL(DST.COMP_REG_REG_CLASS, '') <> ISNULL(SRC.COMP_REG_REG_CLASS, '')
					 OR ISNULL(DST.COMP_REG_CALCULATION, '') LIKE ISNULL(SRC.COMP_REG_CALCULATION, '')
					 OR ISNULL(DST.SQUARE_FOOT_RULES, '') LIKE ISNULL(SRC.SQUARE_FOOT_RULES, '')
					 OR ISNULL(DST.TAX_BY_ADDRESS, '') <> ISNULL(SRC.TAX_BY_ADDRESS, '')
					 OR ISNULL(DST.VAT_RULESET, '') <> ISNULL(SRC.VAT_RULESET, '')
					 OR ISNULL(DST.REG_CLASS_STORED_PROC, '') <> ISNULL(SRC.REG_CLASS_STORED_PROC, '')
					 OR ISNULL(DST.WEB_REG_CLASS_METHOD, '') <> ISNULL(SRC.WEB_REG_CLASS_METHOD, '')
					 OR ISNULL(DST.REG_OTHERS, '') <> ISNULL(SRC.REG_OTHERS, '')
					 OR ISNULL(DST.ADD_GUESTS, '') <> ISNULL(SRC.ADD_GUESTS, '')
					 OR ISNULL(DST.WEB_DESC, '') LIKE ISNULL(SRC.WEB_DESC, '')
					 OR ISNULL(DST.ALLOW_REG_EDIT, '') <> ISNULL(SRC.ALLOW_REG_EDIT, '')
					 OR ISNULL(DST.REG_EDIT_CUTOFF, '') <> ISNULL(SRC.REG_EDIT_CUTOFF, '')
					 OR ISNULL(DST.FORM_DEFINITION_ID, '') <> ISNULL(SRC.FORM_DEFINITION_ID, '')
					 OR ISNULL(DST.FORM_DEFINITION_SECTION_ID, '') <> ISNULL(SRC.FORM_DEFINITION_SECTION_ID, '')
					 OR ISNULL(DST.PUBLISH_START_DATE, '') <> ISNULL(SRC.PUBLISH_START_DATE, '')
					 OR ISNULL(DST.PUBLISH_END_DATE, '') <> ISNULL(SRC.PUBLISH_END_DATE, '')
					 OR ISNULL(DST.REGISTRATION_START_DATE, '') <> ISNULL(SRC.REGISTRATION_START_DATE, '')
					 OR ISNULL(DST.REGISTRATION_END_DATE, '') <> ISNULL(SRC.REGISTRATION_END_DATE, '')
					 OR ISNULL(DST.REGISTRATION_CLOSED_MESSAGE, '') <> ISNULL(SRC.REGISTRATION_CLOSED_MESSAGE, '')
					 OR ISNULL(DST.DEFAULT_PROGRAMITEM_DISPLAYMODE, '') <> ISNULL(SRC.DEFAULT_PROGRAMITEM_DISPLAYMODE, '')
					 OR ISNULL(DST.TEMPLATE_STATE_CODE, '') <> ISNULL(SRC.TEMPLATE_STATE_CODE, '')
					 OR ISNULL(DST.ENABLE_TIME_CONFLICTS, '') <> ISNULL(SRC.ENABLE_TIME_CONFLICTS, '')
					 OR ISNULL(DST.ALLOW_REGISTRANT_CONFLICTS, '') <> ISNULL(SRC.ALLOW_REGISTRANT_CONFLICTS, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [MEETING]
      ,[TITLE]
      ,[MEETING_TYPE]
      ,[DESCRIPTION]
      ,[BEGIN_DATE]
      ,[END_DATE]
      ,[STATUS]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[ADDRESS_3]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DIRECTIONS]
      ,[COORDINATORS]
      ,[NOTES]
      ,[ALLOW_REG_STRING]
      ,[EARLY_CUTOFF]
      ,[REG_CUTOFF]
      ,[LATE_CUTOFF]
      ,[ORG_CODE]
      ,[LOGO]
      ,[MAX_REGISTRANTS]
      ,[TOTAL_REGISTRANTS]
      ,[TOTAL_CANCELATIONS]
      ,[TOTAL_REVENUE]
      ,[HEAD_COUNT]
      ,[TAX_AUTHORITY_1]
      ,[SUPPRESS_COOR]
      ,[SUPPRESS_DIR]
      ,[SUPPRESS_NOTES]
      ,[MUF_1]
      ,[MUF_2]
      ,[MUF_3]
      ,[MUF_4]
      ,[MUF_5]
      ,[MUF_6]
      ,[MUF_7]
      ,[MUF_8]
      ,[MUF_9]
      ,[MUF_10]
      ,[INTENT_TO_EDIT]
      ,[SUPPRESS_CONFIRM]
      ,[WEB_VIEW_ONLY]
      ,[WEB_ENABLED]
      ,[POST_REGISTRATION]
      ,[EMAIL_REGISTRATION]
      ,[MEETING_URL]
      ,[MEETING_IMAGE_NAME]
      ,[CONTACT_ID]
      ,[IS_FR_MEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[MEET_CATEGORY]
      ,[COMP_REG_REG_CLASS]
      ,[COMP_REG_CALCULATION]
      ,[SQUARE_FOOT_RULES]
      ,[TAX_BY_ADDRESS]
      ,[VAT_RULESET]
      ,[REG_CLASS_STORED_PROC]
      ,[WEB_REG_CLASS_METHOD]
      ,[REG_OTHERS]
      ,[ADD_GUESTS]
      ,[WEB_DESC]
      ,[ALLOW_REG_EDIT]
      ,[REG_EDIT_CUTOFF]
      ,[FORM_DEFINITION_ID]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[REGISTRATION_START_DATE]
      ,[REGISTRATION_END_DATE]
      ,[REGISTRATION_CLOSED_MESSAGE]
      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
      ,[TEMPLATE_STATE_CODE]
      ,[ENABLE_TIME_CONFLICTS]
      ,[ALLOW_REGISTRANT_CONFLICTS]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]

          
                     )
              VALUES (
                    src.[MEETING]
      ,src.[TITLE]
      ,src.[MEETING_TYPE]
      ,src.[DESCRIPTION]
      ,src.[BEGIN_DATE]
      ,src.[END_DATE]
      ,src.[STATUS]
      ,src.[ADDRESS_1]
      ,src.[ADDRESS_2]
      ,src.[ADDRESS_3]
      ,src.[CITY]
      ,src.[STATE_PROVINCE]
      ,src.[ZIP]
      ,src.[COUNTRY]
      ,src.[DIRECTIONS]
      ,src.[COORDINATORS]
      ,src.[NOTES]
      ,src.[ALLOW_REG_STRING]
      ,src.[EARLY_CUTOFF]
      ,src.[REG_CUTOFF]
      ,src.[LATE_CUTOFF]
      ,src.[ORG_CODE]
      ,src.[LOGO]
      ,src.[MAX_REGISTRANTS]
      ,src.[TOTAL_REGISTRANTS]
      ,src.[TOTAL_CANCELATIONS]
      ,src.[TOTAL_REVENUE]
      ,src.[HEAD_COUNT]
      ,src.[TAX_AUTHORITY_1]
      ,src.[SUPPRESS_COOR]
      ,src.[SUPPRESS_DIR]
      ,src.[SUPPRESS_NOTES]
      ,src.[MUF_1]
      ,src.[MUF_2]
      ,src.[MUF_3]
      ,src.[MUF_4]
      ,src.[MUF_5]
      ,src.[MUF_6]
      ,src.[MUF_7]
      ,src.[MUF_8]
      ,src.[MUF_9]
      ,src.[MUF_10]
      ,src.[INTENT_TO_EDIT]
      ,src.[SUPPRESS_CONFIRM]
      ,src.[WEB_VIEW_ONLY]
      ,src.[WEB_ENABLED]
      ,src.[POST_REGISTRATION]
      ,src.[EMAIL_REGISTRATION]
      ,src.[MEETING_URL]
      ,src.[MEETING_IMAGE_NAME]
      ,src.[CONTACT_ID]
      ,src.[IS_FR_MEET]
      ,src.[MEET_APPEAL]
      ,src.[MEET_CAMPAIGN]
      ,src.[MEET_CATEGORY]
      ,src.[COMP_REG_REG_CLASS]
      ,src.[COMP_REG_CALCULATION]
      ,src.[SQUARE_FOOT_RULES]
      ,src.[TAX_BY_ADDRESS]
      ,src.[VAT_RULESET]
      ,src.[REG_CLASS_STORED_PROC]
      ,src.[WEB_REG_CLASS_METHOD]
      ,src.[REG_OTHERS]
      ,src.[ADD_GUESTS]
      ,src.[WEB_DESC]
      ,src.[ALLOW_REG_EDIT]
      ,src.[REG_EDIT_CUTOFF]
      ,src.[FORM_DEFINITION_ID]
      ,src.[FORM_DEFINITION_SECTION_ID]
      ,src.[PUBLISH_START_DATE]
      ,src.[PUBLISH_END_DATE]
      ,src.[REGISTRATION_START_DATE]
      ,src.[REGISTRATION_END_DATE]
      ,src.[REGISTRATION_CLOSED_MESSAGE]
      ,src.[DEFAULT_PROGRAMITEM_DISPLAYMODE]
      ,src.[TEMPLATE_STATE_CODE]
      ,src.[ENABLE_TIME_CONFLICTS]
      ,src.[ALLOW_REGISTRANT_CONFLICTS]
      ,src.[TIME_STAMP]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.meeting
       ,deleted.meeting
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_Meet_Master'
       ,'stg.imis_Meet_Master'
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
			,(select count(*) as RowsRead
				from tmp.imis_Meet_Master
				)
       ,getdate()
       )

INSERT INTO stg.imis_Meet_Master (
[MEETING]
      ,[TITLE]
      ,[MEETING_TYPE]
      ,[DESCRIPTION]
      ,[BEGIN_DATE]
      ,[END_DATE]
      ,[STATUS]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[ADDRESS_3]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DIRECTIONS]
      ,[COORDINATORS]
      ,[NOTES]
      ,[ALLOW_REG_STRING]
      ,[EARLY_CUTOFF]
      ,[REG_CUTOFF]
      ,[LATE_CUTOFF]
      ,[ORG_CODE]
      ,[LOGO]
      ,[MAX_REGISTRANTS]
      ,[TOTAL_REGISTRANTS]
      ,[TOTAL_CANCELATIONS]
      ,[TOTAL_REVENUE]
      ,[HEAD_COUNT]
      ,[TAX_AUTHORITY_1]
      ,[SUPPRESS_COOR]
      ,[SUPPRESS_DIR]
      ,[SUPPRESS_NOTES]
      ,[MUF_1]
      ,[MUF_2]
      ,[MUF_3]
      ,[MUF_4]
      ,[MUF_5]
      ,[MUF_6]
      ,[MUF_7]
      ,[MUF_8]
      ,[MUF_9]
      ,[MUF_10]
      ,[INTENT_TO_EDIT]
      ,[SUPPRESS_CONFIRM]
      ,[WEB_VIEW_ONLY]
      ,[WEB_ENABLED]
      ,[POST_REGISTRATION]
      ,[EMAIL_REGISTRATION]
      ,[MEETING_URL]
      ,[MEETING_IMAGE_NAME]
      ,[CONTACT_ID]
      ,[IS_FR_MEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[MEET_CATEGORY]
      ,[COMP_REG_REG_CLASS]
      ,[COMP_REG_CALCULATION]
      ,[SQUARE_FOOT_RULES]
      ,[TAX_BY_ADDRESS]
      ,[VAT_RULESET]
      ,[REG_CLASS_STORED_PROC]
      ,[WEB_REG_CLASS_METHOD]
      ,[REG_OTHERS]
      ,[ADD_GUESTS]
      ,[WEB_DESC]
      ,[ALLOW_REG_EDIT]
      ,[REG_EDIT_CUTOFF]
      ,[FORM_DEFINITION_ID]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[REGISTRATION_START_DATE]
      ,[REGISTRATION_END_DATE]
      ,[REGISTRATION_CLOSED_MESSAGE]
      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
      ,[TEMPLATE_STATE_CODE]
      ,[ENABLE_TIME_CONFLICTS]
      ,[ALLOW_REGISTRANT_CONFLICTS]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]

                 )
select  
                     [MEETING]
      ,[TITLE]
      ,[MEETING_TYPE]
      ,[DESCRIPTION]
      ,[BEGIN_DATE]
      ,[END_DATE]
      ,[STATUS]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[ADDRESS_3]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DIRECTIONS]
      ,[COORDINATORS]
      ,[NOTES]
      ,[ALLOW_REG_STRING]
      ,[EARLY_CUTOFF]
      ,[REG_CUTOFF]
      ,[LATE_CUTOFF]
      ,[ORG_CODE]
      ,[LOGO]
      ,[MAX_REGISTRANTS]
      ,[TOTAL_REGISTRANTS]
      ,[TOTAL_CANCELATIONS]
      ,[TOTAL_REVENUE]
      ,[HEAD_COUNT]
      ,[TAX_AUTHORITY_1]
      ,[SUPPRESS_COOR]
      ,[SUPPRESS_DIR]
      ,[SUPPRESS_NOTES]
      ,[MUF_1]
      ,[MUF_2]
      ,[MUF_3]
      ,[MUF_4]
      ,[MUF_5]
      ,[MUF_6]
      ,[MUF_7]
      ,[MUF_8]
      ,[MUF_9]
      ,[MUF_10]
      ,[INTENT_TO_EDIT]
      ,[SUPPRESS_CONFIRM]
      ,[WEB_VIEW_ONLY]
      ,[WEB_ENABLED]
      ,[POST_REGISTRATION]
      ,[EMAIL_REGISTRATION]
      ,[MEETING_URL]
      ,[MEETING_IMAGE_NAME]
      ,[CONTACT_ID]
      ,[IS_FR_MEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[MEET_CATEGORY]
      ,[COMP_REG_REG_CLASS]
      ,[COMP_REG_CALCULATION]
      ,[SQUARE_FOOT_RULES]
      ,[TAX_BY_ADDRESS]
      ,[VAT_RULESET]
      ,[REG_CLASS_STORED_PROC]
      ,[WEB_REG_CLASS_METHOD]
      ,[REG_OTHERS]
      ,[ADD_GUESTS]
      ,[WEB_DESC]
      ,[ALLOW_REG_EDIT]
      ,[REG_EDIT_CUTOFF]
      ,[FORM_DEFINITION_ID]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[PUBLISH_START_DATE]
      ,[PUBLISH_END_DATE]
      ,[REGISTRATION_START_DATE]
      ,[REGISTRATION_END_DATE]
      ,[REGISTRATION_CLOSED_MESSAGE]
      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
      ,[TEMPLATE_STATE_CODE]
      ,[ENABLE_TIME_CONFLICTS]
      ,[ALLOW_REGISTRANT_CONFLICTS]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Meet_Master
WHERE meeting IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp




/**************************************************Product_Function**************************************/
MERGE stg.imis_Product_Function AS DST
USING tmp.imis_Product_Function AS SRC
       ON DST.product_code = SRC.product_code 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.FUNCTION_TYPE, '') <> ISNULL(SRC.FUNCTION_TYPE, '')
                     OR ISNULL(DST.BEGIN_DATE_TIME, '') <> ISNULL(SRC.BEGIN_DATE_TIME, '')
                     OR ISNULL(DST.END_DATE_TIME, '') <> ISNULL(SRC.END_DATE_TIME, '')
                     OR ISNULL(DST.SEQ, '') <> ISNULL(SRC.SEQ, '')
                     OR ISNULL(DST.MINIMUM_ATTENDANCE, '') <> ISNULL(SRC.MINIMUM_ATTENDANCE, '')
                     OR ISNULL(DST.EXPECTED_ATTENDANCE, '') <> ISNULL(SRC.EXPECTED_ATTENDANCE, '')
                     OR ISNULL(DST.GUARANTEED_ATTENDANCE, '') <> ISNULL(SRC.GUARANTEED_ATTENDANCE, '')
                     OR ISNULL(DST.ACTUAL_ATTENDANCE, '') <> ISNULL(SRC.ACTUAL_ATTENDANCE, '')
                     OR ISNULL(DST.SETTINGS, '') <> ISNULL(SRC.SETTINGS, '')
					 OR ISNULL(DST.SETUP_DATE_TIME, '') <> ISNULL(SRC.SETUP_DATE_TIME, '')
					 OR ISNULL(DST.POST_DATE_TIME, '') <> ISNULL(SRC.POST_DATE_TIME, '')
					 OR ISNULL(DST.AUTO_ENROLL, '') <> ISNULL(SRC.AUTO_ENROLL, '')
					 OR ISNULL(DST.PRINT_TICKET, '') <> ISNULL(SRC.PRINT_TICKET, '')
					 OR ISNULL(DST.LAST_TICKET, '') <> ISNULL(SRC.LAST_TICKET, '')
					 OR ISNULL(DST.CEU_TYPE, '') <> ISNULL(SRC.CEU_TYPE, '')
					 OR ISNULL(DST.CEU_AMOUNT, 0) <> ISNULL(SRC.CEU_AMOUNT, 0)
					 OR ISNULL(DST.COURSE_CODE, '') <> ISNULL(SRC.COURSE_CODE, '')
					 OR ISNULL(DST.OTHER_TICKETS, '') <> ISNULL(SRC.OTHER_TICKETS, '')
					 OR ISNULL(DST.CEU_ENTERED, '') <> ISNULL(SRC.CEU_ENTERED, '')
					 OR ISNULL(DST.MAXIMUM_ATTENDANCE, '') <> ISNULL(SRC.MAXIMUM_ATTENDANCE, '')
					 OR ISNULL(DST.PARENT, '') <> ISNULL(SRC.PARENT, '')
					 OR ISNULL(DST.CONFLICT_CODE, '') <> ISNULL(SRC.CONFLICT_CODE, '')
					 OR ISNULL(DST.WEB_ENABLED, '') <> ISNULL(SRC.WEB_ENABLED, '')
					 OR ISNULL(DST.WEB_MULTI_REG, '') <> ISNULL(SRC.WEB_MULTI_REG, '')
					 OR ISNULL(DST.SQUARE_FEET, 0) <> ISNULL(SRC.SQUARE_FEET, 0)
					 OR ISNULL(DST.IS_FR_ITEM, '') <> ISNULL(SRC.IS_FR_ITEM, '')
					 OR ISNULL(DST.IS_GUEST_FUNCTION, '') <> ISNULL(SRC.IS_GUEST_FUNCTION, '')
					 OR ISNULL(DST.CREATE_DETAIL_ACTIVITY, '') <> ISNULL(SRC.CREATE_DETAIL_ACTIVITY, '')
					 OR ISNULL(DST.EVENT_TRACK, '') <> ISNULL(SRC.EVENT_TRACK, '')
					 OR ISNULL(DST.EVENT_CATEGORY, '') <> ISNULL(SRC.EVENT_CATEGORY, '')
					 OR ISNULL(DST.IS_EVENT_REGISTRATION_OPTION, '') <> ISNULL(SRC.IS_EVENT_REGISTRATION_OPTION, '')
					 OR ISNULL(DST.FORM_DEFINITION_SECTION_ID, '') <> ISNULL(SRC.FORM_DEFINITION_SECTION_ID, '')
					 OR ISNULL(DST.AVAILABLE_TO, '') <> ISNULL(SRC.AVAILABLE_TO, '')
					 OR ISNULL(DST.ENABLE_TIME_CONFLICTS, '') <> ISNULL(SRC.ENABLE_TIME_CONFLICTS, '')
					 OR ISNULL(DST.WEB_CONFLICT_CODES, '') <> ISNULL(SRC.WEB_CONFLICT_CODES, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [PRODUCT_CODE]
      ,[FUNCTION_TYPE]
      ,[BEGIN_DATE_TIME]
      ,[END_DATE_TIME]
      ,[SEQ]
      ,[MINIMUM_ATTENDANCE]
      ,[EXPECTED_ATTENDANCE]
      ,[GUARANTEED_ATTENDANCE]
      ,[ACTUAL_ATTENDANCE]
      ,[SETTINGS]
      ,[SETUP_DATE_TIME]
      ,[POST_DATE_TIME]
      ,[AUTO_ENROLL]
      ,[PRINT_TICKET]
      ,[LAST_TICKET]
      ,[CEU_TYPE]
      ,[CEU_AMOUNT]
      ,[COURSE_CODE]
      ,[OTHER_TICKETS]
      ,[CEU_ENTERED]
      ,[MAXIMUM_ATTENDANCE]
      ,[PARENT]
      ,[CONFLICT_CODE]
      ,[WEB_ENABLED]
      ,[WEB_MULTI_REG]
      ,[SQUARE_FEET]
      ,[IS_FR_ITEM]
      ,[IS_GUEST_FUNCTION]
      ,[CREATE_DETAIL_ACTIVITY]
      ,[EVENT_TRACK]
      ,[EVENT_CATEGORY]
      ,[IS_EVENT_REGISTRATION_OPTION]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[AVAILABLE_TO]
      ,[ENABLE_TIME_CONFLICTS]
      ,[WEB_CONFLICT_CODES]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     src.[PRODUCT_CODE]
      ,src.[FUNCTION_TYPE]
      ,src.[BEGIN_DATE_TIME]
      ,src.[END_DATE_TIME]
      ,src.[SEQ]
      ,src.[MINIMUM_ATTENDANCE]
      ,src.[EXPECTED_ATTENDANCE]
      ,src.[GUARANTEED_ATTENDANCE]
      ,src.[ACTUAL_ATTENDANCE]
      ,src.[SETTINGS]
      ,src.[SETUP_DATE_TIME]
      ,src.[POST_DATE_TIME]
      ,src.[AUTO_ENROLL]
      ,src.[PRINT_TICKET]
      ,src.[LAST_TICKET]
      ,src.[CEU_TYPE]
      ,src.[CEU_AMOUNT]
      ,src.[COURSE_CODE]
      ,src.[OTHER_TICKETS]
      ,src.[CEU_ENTERED]
      ,src.[MAXIMUM_ATTENDANCE]
      ,src.[PARENT]
      ,src.[CONFLICT_CODE]
      ,src.[WEB_ENABLED]
      ,src.[WEB_MULTI_REG]
      ,src.[SQUARE_FEET]
      ,src.[IS_FR_ITEM]
      ,src.[IS_GUEST_FUNCTION]
      ,src.[CREATE_DETAIL_ACTIVITY]
      ,src.[EVENT_TRACK]
      ,src.[EVENT_CATEGORY]
      ,src.[IS_EVENT_REGISTRATION_OPTION]
      ,src.[FORM_DEFINITION_SECTION_ID]
      ,src.[AVAILABLE_TO]
      ,src.[ENABLE_TIME_CONFLICTS]
      ,src.[WEB_CONFLICT_CODES]
      ,src.[TIME_STAMP]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.product_code
       ,deleted.product_code
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_Product_Function'
       ,'stg.imis_Product_Function'
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
			,(select count(*) as RowsRead
				from tmp.imis_Product_Function
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Product_Function (
[PRODUCT_CODE]
      ,[FUNCTION_TYPE]
      ,[BEGIN_DATE_TIME]
      ,[END_DATE_TIME]
      ,[SEQ]
      ,[MINIMUM_ATTENDANCE]
      ,[EXPECTED_ATTENDANCE]
      ,[GUARANTEED_ATTENDANCE]
      ,[ACTUAL_ATTENDANCE]
      ,[SETTINGS]
      ,[SETUP_DATE_TIME]
      ,[POST_DATE_TIME]
      ,[AUTO_ENROLL]
      ,[PRINT_TICKET]
      ,[LAST_TICKET]
      ,[CEU_TYPE]
      ,[CEU_AMOUNT]
      ,[COURSE_CODE]
      ,[OTHER_TICKETS]
      ,[CEU_ENTERED]
      ,[MAXIMUM_ATTENDANCE]
      ,[PARENT]
      ,[CONFLICT_CODE]
      ,[WEB_ENABLED]
      ,[WEB_MULTI_REG]
      ,[SQUARE_FEET]
      ,[IS_FR_ITEM]
      ,[IS_GUEST_FUNCTION]
      ,[CREATE_DETAIL_ACTIVITY]
      ,[EVENT_TRACK]
      ,[EVENT_CATEGORY]
      ,[IS_EVENT_REGISTRATION_OPTION]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[AVAILABLE_TO]
      ,[ENABLE_TIME_CONFLICTS]
      ,[WEB_CONFLICT_CODES]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]                 )
select  
                     [PRODUCT_CODE]
      ,[FUNCTION_TYPE]
      ,[BEGIN_DATE_TIME]
      ,[END_DATE_TIME]
      ,[SEQ]
      ,[MINIMUM_ATTENDANCE]
      ,[EXPECTED_ATTENDANCE]
      ,[GUARANTEED_ATTENDANCE]
      ,[ACTUAL_ATTENDANCE]
      ,[SETTINGS]
      ,[SETUP_DATE_TIME]
      ,[POST_DATE_TIME]
      ,[AUTO_ENROLL]
      ,[PRINT_TICKET]
      ,[LAST_TICKET]
      ,[CEU_TYPE]
      ,[CEU_AMOUNT]
      ,[COURSE_CODE]
      ,[OTHER_TICKETS]
      ,[CEU_ENTERED]
      ,[MAXIMUM_ATTENDANCE]
      ,[PARENT]
      ,[CONFLICT_CODE]
      ,[WEB_ENABLED]
      ,[WEB_MULTI_REG]
      ,[SQUARE_FEET]
      ,[IS_FR_ITEM]
      ,[IS_GUEST_FUNCTION]
      ,[CREATE_DETAIL_ACTIVITY]
      ,[EVENT_TRACK]
      ,[EVENT_CATEGORY]
      ,[IS_EVENT_REGISTRATION_OPTION]
      ,[FORM_DEFINITION_SECTION_ID]
      ,[AVAILABLE_TO]
      ,[ENABLE_TIME_CONFLICTS]
      ,[WEB_CONFLICT_CODES]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Product_Function
WHERE product_code IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

/***********************************************CUSTOM ADDRESS GEOCODE******************/
MERGE stg.imis_Custom_Address_Geocode AS DST
USING tmp.imis_Custom_Address_Geocode AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.ADDRESS_NUM, '') <> ISNULL(SRC.ADDRESS_NUM, '')
                     OR ISNULL(DST.VOTERVOICE_CHECKSUM, '') <> ISNULL(SRC.VOTERVOICE_CHECKSUM, '')
                     OR ISNULL(DST.LONGITUDE, '') <> ISNULL(SRC.LONGITUDE, '')
                     OR ISNULL(DST.LATITUDE, '') <> ISNULL(SRC.LATITUDE, '')
                     OR ISNULL(DST.WEAK_COORDINATES, '') <> ISNULL(SRC.WEAK_COORDINATES, '')
                     OR ISNULL(DST.US_CONGRESS, '') <> ISNULL(SRC.US_CONGRESS, '')
                     OR ISNULL(DST.STATE_SENATE, '') <> ISNULL(SRC.STATE_SENATE, '')
                     OR ISNULL(DST.STATE_HOUSE, '') <> ISNULL(SRC.STATE_HOUSE, '')
                     OR ISNULL(DST.CHANGED, '') <> ISNULL(SRC.CHANGED, '')
                     OR ISNULL(DST.LAST_UPDATED, '') <> ISNULL(SRC.LAST_UPDATED, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [ID]
      ,[SEQN]
      ,[ADDRESS_NUM]
      ,[VOTERVOICE_CHECKSUM]
      ,[LONGITUDE]
      ,[LATITUDE]
      ,[WEAK_COORDINATES]
      ,[US_CONGRESS]
      ,[STATE_SENATE]
      ,[STATE_HOUSE]
      ,[CHANGED]
      ,[LAST_UPDATED]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     SRC.[ID]
      ,SRC.[SEQN]
      ,SRC.[ADDRESS_NUM]
      ,SRC.[VOTERVOICE_CHECKSUM]
      ,SRC.[LONGITUDE]
      ,SRC.[LATITUDE]
      ,SRC.[WEAK_COORDINATES]
      ,SRC.[US_CONGRESS]
      ,SRC.[STATE_SENATE]
      ,SRC.[STATE_HOUSE]
      ,SRC.[CHANGED]
      ,SRC.[LAST_UPDATED]
      ,SRC.[TIME_STAMP]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
        '@PipelineName'
	   ,'tmp.imis_Custom_Address_Geocode'
       ,'stg.imis_Custom_Address_Geocode'
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
			 ,(select count(*) as RowsRead
				from tmp.imis_Custom_Address_Geocode
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Custom_Address_Geocode (
[ID]
      ,[SEQN]
      ,[ADDRESS_NUM]
      ,[VOTERVOICE_CHECKSUM]
      ,[LONGITUDE]
      ,[LATITUDE]
      ,[WEAK_COORDINATES]
      ,[US_CONGRESS]
      ,[STATE_SENATE]
      ,[STATE_HOUSE]
      ,[CHANGED]
      ,[LAST_UPDATED]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                    [ID]
      ,[SEQN]
      ,[ADDRESS_NUM]
      ,[VOTERVOICE_CHECKSUM]
      ,[LONGITUDE]
      ,[LATITUDE]
      ,[WEAK_COORDINATES]
      ,[US_CONGRESS]
      ,[STATE_SENATE]
      ,[STATE_HOUSE]
      ,[CHANGED]
      ,[LAST_UPDATED]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Custom_Address_Geocode
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

/**************************************************table**********************/
END TRY
BEGIN CATCH
SET @ErrorMessage  = ERROR_MESSAGE();
    SET @ErrorSeverity = ERROR_SEVERITY();
    SET @ErrorState    = ERROR_STATE();
    THROW;
END CATCH;
