


/*******************************************************************************************************************************
 Description:   This Stored Procedure merges data from the iMIS temp tables into the staging tables on a daily basis. These tables 
				have been identified to be incrementally merged on a daily basis. 
 				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE procedure [etl].[usp_IMIS_Daily_Tmp_To_Stage]
@PipelineName varchar(60) 
as

/*************************************************CUSTOM DEGREE********************************************************/
---add new degree records to the Custom Degree Staging Table

BEGIN

BEGIN TRY

BEGIN TRAN

DECLARE @Yesterday DATETIME = DATEADD(dd, - 1, GETDATE())
DECLARE @Today DATETIME = GETDATE()

IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
       Drop TABLE #audittemp

CREATE TABLE #audittemp (
       action NVARCHAR(20)
       ,inserted_id varchar(80)
       ,deleted_id varchar(80)
       );

DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @ErrorSeverity NVARCHAR(MAX)


DECLARE @ErrorState tinyint

MERGE stg.imis_Custom_Degree AS DST
USING tmp.imis_Custom_Degree AS SRC
       ON DST.ID = SRC.ID and
	   DST.SEQN = SRC.SEQN
WHEN MATCHED
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
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

--  Below process will delete any duplicate records that got inserted from the previous step which is possible if a Member ID has a new degree
--  and a changed degree on the same day

;with cte as
(select ID, SEQN, IS_CURRENT, count(*) as ttlcount 
from stg.imis_Custom_Degree
where IsActive = 1 and IS_CURRENT = 1
group by ID, SEQN, IS_CURRENT
having count(*) > 1
)

, cte2 as
(
select ID, SEQN, IS_CURRENT, min(Id_Identitycolumn) as identityval from stg.imis_Custom_Degree A where isactive = 1 and exists
(Select * from cte B where A.ID = B.ID and A.SEQN = B.SEQN and A.IS_CURRENT = B.IS_CURRENT
)
group by ID, SEQN, IS_CURRENT
)

delete from stg.imis_Custom_Degree  where Id_Identitycolumn in
(Select identityval from cte2)


/********************************************************Custom_AICP_Exam_Score*******************************************/
---add new AICP Exam Score records to the Custom AICP Exam Score Staging Table


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
              AND DST.IsActive = 1
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
              INSERT  (
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
---add new custom credit records to the Custom Credit Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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



/********************************************************IND_DEMOGRAPHICS**********************************************************/
---add new individual demographics records to the Ind Demographics Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
---add new mailing demographic records to the Mailing Demographics Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
---add new member records to the Name Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
select 
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
---add new organization demographics records to the Org Demographics Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
---add new PAS demographics records to the PAS Demographics Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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
---add new race origin records to the Race Origin Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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



/*************************************************************************SUBSCRIPTIONS*******************************************************/
---add new subscription records to the Subscriptions Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
      @PipelineName
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

--  Below process will delete any duplicate records that got inserted from the previous step which is possible if a Member ID has a new Subscription
--  and a changed subscription on the same day

;with cte as
(select ID, product_code, status, paid_thru, count(*) as ttlcount from stg.imis_Subscriptions   where IsActive = 1
group by ID, product_code, status, paid_thru
having count(*) > 1
)

, cte2 as
(
select ID, product_code, status, paid_thru, min(id_identity) as identityval from stg.imis_Subscriptions A where isactive = 1 and exists
(Select * from cte B where A.ID = B.ID and A.PRODUCT_CODE = B.PRODUCT_CODE and A.status = B.STATUS
and  A.PAID_THRU is null and B.PAID_THRU is null)
group by ID, product_code, status, paid_thru
union 
select ID, product_code, status, paid_thru, min(id_identity) as identityval from stg.imis_Subscriptions A where isactive = 1 and exists
(Select * from cte B where A.ID = B.ID and A.PRODUCT_CODE = B.PRODUCT_CODE and A.status = B.STATUS
and  A.PAID_THRU = B.PAID_THRU 
)
group by ID, product_code, status, paid_thru
)

delete from stg.imis_Subscriptions  where Id_Identity in
(Select identityval from cte2)




/*****************************************************************NAME_ADDRESS*************************************************************************/
---add new address records to the Name Address Staging Table


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
              AND DST.IsActive = 1
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
					 OR ISNULL(DST.NOTE, '') <> ISNULL(SRC.NOTE, '')
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
       ,inserted.address_num
       ,deleted.address_num
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
WHERE address_num IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp



/*********************************************************************GEN_TABLES*******************************************************/
---add new UD description records to the Gen Tables Staging Table


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
              AND DST.IsActive = 1
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
---add new product records to the Product Staging Table


--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Product AS DST
USING tmp.imis_Product AS SRC
          ON  DST.PRODUCT_CODE = SRC.PRODUCT_CODE
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                        ISNULL(DST.PRODUCT_MAJOR, '') <> ISNULL(SRC.PRODUCT_MAJOR, '')
                     OR ISNULL(DST.PRODUCT_MINOR, '') <> ISNULL(SRC.PRODUCT_MINOR, '')
                     OR ISNULL(DST.PROD_TYPE, '') <> ISNULL(SRC.PROD_TYPE, '')
                     OR ISNULL(DST.CATEGORY, '') <> ISNULL(SRC.CATEGORY, '')
					 OR ISNULL(DST.TITLE_KEY, '') <> ISNULL(SRC.TITLE_KEY, '')
					 OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
                     OR ISNULL(DST.[DESCRIPTION],'') <> ISNULL(SRC.[DESCRIPTION],'')
                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
                     OR ISNULL(DST.NOTE,'') <> ISNULL(SRC.NOTE,'')
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
					 OR ISNULL(DST.DECIMAL_POINTS, '') <> ISNULL(SRC.DECIMAL_POINTS, '')
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
					 OR ISNULL(DST.CATALOG_DESC, '') <> ISNULL(SRC.CATALOG_DESC, '')
					 OR ISNULL(DST.WEB_DESC, '') <> ISNULL(SRC.WEB_DESC, '')
					 OR ISNULL(DST.OTHER_DESC, '') <> ISNULL(SRC.OTHER_DESC, '')
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
      ,CONVERT(VARCHAR(MAX),SRC.[TITLE_KEY])
      ,CONVERT(VARCHAR(MAX),SRC.[TITLE])
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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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
select   distinct
       [PRODUCT_CODE]
      ,[PRODUCT_MAJOR]
      ,[PRODUCT_MINOR]
      ,[PROD_TYPE]
      ,[CATEGORY]
      ,CONVERT(VARCHAR(MAX),[TITLE_KEY])
      ,CONVERT(VARCHAR(MAX),[TITLE])
      ,CONVERT(VARCHAR(MAX),[DESCRIPTION])
      ,[STATUS]
      ,CONVERT(VARCHAR(MAX),[NOTE])
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
      ,CONVERT(VARCHAR(MAX),[CATALOG_DESC])
      ,CONVERT(VARCHAR(MAX),[WEB_DESC])
      ,CONVERT(VARCHAR(MAX),[OTHER_DESC])
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

/**************************Activity***********/

---delete duplicate records prior to loading to staging and fact table
delete from tmp.imis_Activity  where  exists
(Select * from stg.imis_Activity B where tmp.imis_Activity.ID = B.ID and tmp.imis_Activity.SEQN = B.SEQN)

---add new activity records to the Activity Staging Table
MERGE stg.imis_Activity AS DST
USING tmp.imis_Activity AS SRC
       ON DST.ID = SRC.ID 
	   AND DST.SEQN = SRC.SEQN
WHEN NOT MATCHED
THEN
INSERT (
[SEQN]
      ,[ID]
      ,[ACTIVITY_TYPE]
      ,[TRANSACTION_DATE]
      ,[EFFECTIVE_DATE]
      ,[PRODUCT_CODE]
      ,[OTHER_CODE]
      ,[DESCRIPTION]
      ,[SOURCE_SYSTEM]
      ,[SOURCE_CODE]
      ,[QUANTITY]
      ,[AMOUNT]
      ,[CATEGORY]
      ,[UNITS]
      ,[THRU_DATE]
      ,[MEMBER_TYPE]
      ,[ACTION_CODES]
      ,[PAY_METHOD]
      ,[TICKLER_DATE]
      ,[NOTE]
      ,[NOTE_2]
      ,[BATCH_NUM]
      ,[CO_ID]
      ,[OBJECT]
      ,[INTENT_TO_EDIT]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[ORIGINATING_TRANS_NUM]
      ,[ORG_CODE]
      ,[CAMPAIGN_CODE]
      ,[OTHER_ID]
      ,[SOLICITOR_ID]
      ,[TAXABLE_VALUE]
      ,[ATTACH_SEQN]
      ,[ATTACH_TOTAL]
      ,[RECURRING_REQUEST]
      ,[STATUS_CODE]
      ,[NEXT_INSTALL_DATE]
      ,[GRACE_PERIOD]
      ,[MEM_TRIB_CODE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
	  )
	  VALUES (
	  SRC.[SEQN]
      ,SRC.[ID]
      ,SRC.[ACTIVITY_TYPE]
      ,SRC.[TRANSACTION_DATE]
      ,SRC.[EFFECTIVE_DATE]
      ,SRC.[PRODUCT_CODE]
      ,SRC.[OTHER_CODE]
      ,SRC.[DESCRIPTION]
      ,SRC.[SOURCE_SYSTEM]
      ,SRC.[SOURCE_CODE]
      ,SRC.[QUANTITY]
      ,SRC.[AMOUNT]
      ,SRC.[CATEGORY]
      ,SRC.[UNITS]
      ,SRC.[THRU_DATE]
      ,SRC.[MEMBER_TYPE]
      ,SRC.[ACTION_CODES]
      ,SRC.[PAY_METHOD]
      ,SRC.[TICKLER_DATE]
      ,SRC.[NOTE]
      ,SRC.[NOTE_2]
      ,SRC.[BATCH_NUM]
      ,SRC.[CO_ID]
      ,SRC.[OBJECT]
      ,SRC.[INTENT_TO_EDIT]
      ,SRC.[UF_1]
      ,SRC.[UF_2]
      ,SRC.[UF_3]
      ,SRC.[UF_4]
      ,SRC.[UF_5]
      ,SRC.[UF_6]
      ,SRC.[UF_7]
      ,SRC.[ORIGINATING_TRANS_NUM]
      ,SRC.[ORG_CODE]
      ,SRC.[CAMPAIGN_CODE]
      ,SRC.[OTHER_ID]
      ,SRC.[SOLICITOR_ID]
      ,SRC.[TAXABLE_VALUE]
      ,SRC.[ATTACH_SEQN]
      ,SRC.[ATTACH_TOTAL]
      ,SRC.[RECURRING_REQUEST]
      ,SRC.[STATUS_CODE]
      ,SRC.[NEXT_INSTALL_DATE]
      ,SRC.[GRACE_PERIOD]
      ,SRC.[MEM_TRIB_CODE]
	  ,SRC.[TIME_STAMP] 
      ,1
      ,@Today
	  );

	  INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
	   ,'tmp.imis_Activity'
       ,'stg.imis_Activity'
       ,(
                     SELECT count(*) AS action_count
                     FROM tmp.imis_Activity
        )
       ,(
             0
					 )
		 ,(SELECT count(*) AS action_count
                     FROM tmp.imis_Activity
				) 
       ,getdate()
       )

	    Truncate TABLE #audittemp

	   /********************Meet Reg Class************/
--MERGE stg.imis_meet_reg_class AS DST
--USING tmp.imis_meet_reg_class AS SRC
--       ON DST.registrant_class = SRC.registrant_class
--WHEN MATCHED
--              AND DST.IsActive = 1
--              AND (
                   
--                     ISNULL(DST.description, '') <> ISNULL(SRC.description, '')
--                     OR ISNULL(DST.long_description, '') <> ISNULL(SRC.long_description, '')
              
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [REGISTRANT_CLASS]
--      ,[DESCRIPTION]
--      ,[LONG_DESCRIPTION]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]
          
--                     )
--              VALUES (
--                     src.[REGISTRANT_CLASS]
--      ,src.[DESCRIPTION]
--      ,src.[LONG_DESCRIPTION]
--      ,src.[TIME_STAMP]
--                 ,1
--                 ,@Today
--                     )
			

--OUTPUT $ACTION AS action
--       ,inserted.registrant_class
--       ,deleted.registrant_class
--INTO #audittemp;



--INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
--VALUES (
--        @PipelineName
--	   ,'tmp.imis_meet_reg_class'
--       ,'stg.imis_meet_reg_class'
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
--			 ,(select count(*) as RowsRead
--				from tmp.imis_meet_reg_class
--				) 
--       ,getdate()
--       )

--INSERT INTO stg.imis_meet_reg_class (
-- [REGISTRANT_CLASS]
--      ,[DESCRIPTION]
--      ,[LONG_DESCRIPTION]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]
--                 )
--select  
--                      [REGISTRANT_CLASS]
--      ,[DESCRIPTION]
--      ,[LONG_DESCRIPTION]
--           ,cast(TIME_STAMP as bigint)
--                 ,1
--                 ,@Today
           
         
--FROM 
--tmp.imis_meet_reg_class
--WHERE registrant_class IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp

/******************Orders**************/
MERGE stg.imis_orders AS DST
USING tmp.imis_orders AS SRC
       ON DST.order_number = SRC.order_number 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
ISNULL (DST.[ORG_CODE], '')	<> 	ISNULL (SRC.[ORG_CODE], '')
OR ISNULL (DST.[ORDER_TYPE_CODE], '')	<> 	ISNULL (SRC.[ORDER_TYPE_CODE], '')
OR ISNULL (DST.[STAGE], '')	<> 	ISNULL (SRC.[STAGE], '')
OR ISNULL (DST.[SOURCE_SYSTEM], '')	<> 	ISNULL (SRC.[SOURCE_SYSTEM], '')
OR ISNULL (DST.[BATCH_NUM], '')	<> 	ISNULL (SRC.[BATCH_NUM], '')
OR ISNULL (DST.[STATUS], '')	<> 	ISNULL (SRC.[STATUS], '')
OR ISNULL (DST.[HOLD_CODE], '')	<> 	ISNULL (SRC.[HOLD_CODE], '')
OR ISNULL (DST.[ORDER_DATE], '')	<> 	ISNULL (SRC.[ORDER_DATE], '')
OR ISNULL (DST.[BT_ID], '')	<> 	ISNULL (SRC.[BT_ID], '')
OR ISNULL (DST.[ST_ID], '')	<> 	ISNULL (SRC.[ST_ID], '')
OR ISNULL (DST.[ST_ADDRESS_NUM], '')	<> 	ISNULL (SRC.[ST_ADDRESS_NUM], '')
OR ISNULL (DST.[ENTERED_DATE_TIME], '')	<> 	ISNULL (SRC.[ENTERED_DATE_TIME], '')
OR ISNULL (DST.[ENTERED_BY], '')	<> 	ISNULL (SRC.[ENTERED_BY], '')
OR ISNULL (DST.[UPDATED_DATE_TIME], '')	<> 	ISNULL (SRC.[UPDATED_DATE_TIME], '')
OR ISNULL (DST.[UPDATED_BY], '')	<> 	ISNULL (SRC.[UPDATED_BY], '')
OR ISNULL (DST.[INVOICE_REFERENCE_NUM], '')	<> 	ISNULL (SRC.[INVOICE_REFERENCE_NUM], '')
OR ISNULL (DST.[INVOICE_NUMBER], '')	<> 	ISNULL (SRC.[INVOICE_NUMBER], '')
OR ISNULL (DST.[INVOICE_DATE], '')	<> 	ISNULL (SRC.[INVOICE_DATE], '')
OR ISNULL (DST.[NUMBER_LINES], '')	<> 	ISNULL (SRC.[NUMBER_LINES], '')
OR ISNULL (DST.[FULL_NAME], '')	<> 	ISNULL (SRC.[FULL_NAME], '')
OR ISNULL (DST.[TITLE], '')	<> 	ISNULL (SRC.[TITLE], '')
OR ISNULL (DST.[COMPANY], '')	<> 	ISNULL (SRC.[COMPANY], '')
OR ISNULL (DST.[FULL_ADDRESS], '')	<> 	ISNULL (SRC.[FULL_ADDRESS], '')
OR ISNULL (DST.[PREFIX], '')	<> 	ISNULL (SRC.[PREFIX], '')
OR ISNULL (DST.[FIRST_NAME], '')	<> 	ISNULL (SRC.[FIRST_NAME], '')
OR ISNULL (DST.[MIDDLE_NAME], '')	<> 	ISNULL (SRC.[MIDDLE_NAME], '')
OR ISNULL (DST.[LAST_NAME], '')	<> 	ISNULL (SRC.[LAST_NAME], '')
OR ISNULL (DST.[SUFFIX], '')	<> 	ISNULL (SRC.[SUFFIX], '')
OR ISNULL (DST.[DESIGNATION], '')	<> 	ISNULL (SRC.[DESIGNATION], '')
OR ISNULL (DST.[INFORMAL], '')	<> 	ISNULL (SRC.[INFORMAL], '')
OR ISNULL (DST.[LAST_FIRST], '')	<> 	ISNULL (SRC.[LAST_FIRST], '')
OR ISNULL (DST.[COMPANY_SORT], '')	<> 	ISNULL (SRC.[COMPANY_SORT], '')
OR ISNULL (DST.[ADDRESS_1], '')	<> 	ISNULL (SRC.[ADDRESS_1], '')
OR ISNULL (DST.[ADDRESS_2], '')	<> 	ISNULL (SRC.[ADDRESS_2], '')
OR ISNULL (DST.[CITY], '')	<> 	ISNULL (SRC.[CITY], '')
OR ISNULL (DST.[STATE_PROVINCE], '')	<> 	ISNULL (SRC.[STATE_PROVINCE], '')
OR ISNULL (DST.[ZIP], '')	<> 	ISNULL (SRC.[ZIP], '')
OR ISNULL (DST.[COUNTRY], '')	<> 	ISNULL (SRC.[COUNTRY], '')
OR ISNULL (DST.[DPB], '')	<> 	ISNULL (SRC.[DPB], '')
OR ISNULL (DST.[BAR_CODE], '')	<> 	ISNULL (SRC.[BAR_CODE], '')
OR ISNULL (DST.[ADDRESS_FORMAT], '')	<> 	ISNULL (SRC.[ADDRESS_FORMAT], '')
OR ISNULL (DST.[PHONE], '')	<> 	ISNULL (SRC.[PHONE], '')
OR ISNULL (DST.[FAX], '')	<> 	ISNULL (SRC.[FAX], '')
OR ISNULL (DST.[NOTES], '')	<> 	ISNULL (SRC.[NOTES], '')
OR ISNULL (DST.[TOTAL_CHARGES], '')	<> 	ISNULL (SRC.[TOTAL_CHARGES], '')
OR ISNULL (DST.[TOTAL_PAYMENTS], '')	<> 	ISNULL (SRC.[TOTAL_PAYMENTS], '')
OR ISNULL (DST.[BALANCE], '')	<> 	ISNULL (SRC.[BALANCE], '')
OR ISNULL (DST.[LINE_TOTAL], '')	<> 	ISNULL (SRC.[LINE_TOTAL], '')
OR ISNULL (DST.[LINE_TAXABLE], '')	<> 	ISNULL (SRC.[LINE_TAXABLE], '')
OR ISNULL (DST.[FREIGHT_1], '')	<> 	ISNULL (SRC.[FREIGHT_1], '')
OR ISNULL (DST.[FREIGHT_2], '')	<> 	ISNULL (SRC.[FREIGHT_2], '')
OR ISNULL (DST.[HANDLING_1], '')	<> 	ISNULL (SRC.[HANDLING_1], '')
OR ISNULL (DST.[HANDLING_2], '')	<> 	ISNULL (SRC.[HANDLING_2], '')
OR ISNULL (DST.[CANCELLATION_FEE], '')	<> 	ISNULL (SRC.[CANCELLATION_FEE], '')
OR ISNULL (DST.[TAX_1], '')	<> 	ISNULL (SRC.[TAX_1], '')
OR ISNULL (DST.[TAX_2], '')	<> 	ISNULL (SRC.[TAX_2], '')
OR ISNULL (DST.[TAX_3], '')	<> 	ISNULL (SRC.[TAX_3], '')
OR ISNULL (DST.[LINE_PAY], '')	<> 	ISNULL (SRC.[LINE_PAY], '')
OR ISNULL (DST.[OTHER_PAY], '')	<> 	ISNULL (SRC.[OTHER_PAY], '')
OR ISNULL (DST.[AR_PAY], '')	<> 	ISNULL (SRC.[AR_PAY], '')
OR ISNULL (DST.[TAX_AUTHOR_1], '')	<> 	ISNULL (SRC.[TAX_AUTHOR_1], '')
OR ISNULL (DST.[TAX_AUTHOR_2], '')	<> 	ISNULL (SRC.[TAX_AUTHOR_2], '')
OR ISNULL (DST.[TAX_AUTHOR_3], '')	<> 	ISNULL (SRC.[TAX_AUTHOR_3], '')
OR ISNULL (DST.[TAX_RATE_1], 0)	<> 	ISNULL (SRC.[TAX_RATE_1], 0)
OR ISNULL (DST.[TAX_RATE_2], 0)	<> 	ISNULL (SRC.[TAX_RATE_2], 0)
OR ISNULL (DST.[TAX_RATE_3], 0)	<> 	ISNULL (SRC.[TAX_RATE_3], 0)
OR ISNULL (DST.[TAX_EXEMPT], '')	<> 	ISNULL (SRC.[TAX_EXEMPT], '')
OR ISNULL (DST.[TERMS_CODE], '')	<> 	ISNULL (SRC.[TERMS_CODE], '')
OR ISNULL (DST.[SCHEDULED_DATE], '')	<> 	ISNULL (SRC.[SCHEDULED_DATE], '')
OR ISNULL (DST.[CONFIRMATION_DATE_TIME], '')	<> 	ISNULL (SRC.[CONFIRMATION_DATE_TIME], '')
OR ISNULL (DST.[SHIP_PAPERS_DATE_TIME], '')	<> 	ISNULL (SRC.[SHIP_PAPERS_DATE_TIME], '')
OR ISNULL (DST.[SHIPPED_DATE_TIME], '')	<> 	ISNULL (SRC.[SHIPPED_DATE_TIME], '')
OR ISNULL (DST.[BO_RELEASED_DATE_TIME], '')	<> 	ISNULL (SRC.[BO_RELEASED_DATE_TIME], '')
OR ISNULL (DST.[SOURCE_CODE], '')	<> 	ISNULL (SRC.[SOURCE_CODE], '')
OR ISNULL (DST.[SALESMAN], '')	<> 	ISNULL (SRC.[SALESMAN], '')
OR ISNULL (DST.[COMMISSION_RATE], 0)	<> 	ISNULL (SRC.[COMMISSION_RATE], 0)
OR ISNULL (DST.[DISCOUNT_RATE], 0)	<> 	ISNULL (SRC.[DISCOUNT_RATE], 0)
OR ISNULL (DST.[PRIORITY], '')	<> 	ISNULL (SRC.[PRIORITY], '')
OR ISNULL (DST.[HOLD_COMMENT], '')	<> 	ISNULL (SRC.[HOLD_COMMENT], '')
OR ISNULL (DST.[AFFECT_INVENTORY], '')	<> 	ISNULL (SRC.[AFFECT_INVENTORY], '')
OR ISNULL (DST.[HOLD_FLAG], '')	<> 	ISNULL (SRC.[HOLD_FLAG], '')
OR ISNULL (DST.[CUSTOMER_REFERENCE], '')	<> 	ISNULL (SRC.[CUSTOMER_REFERENCE], '')
OR ISNULL (DST.[VALUATION_BASIS], '')	<> 	ISNULL (SRC.[VALUATION_BASIS], '')
OR ISNULL (DST.[UNDISCOUNTED_TOTAL], '')	<> 	ISNULL (SRC.[UNDISCOUNTED_TOTAL], '')
OR ISNULL (DST.[AUTO_CALC_HANDLING], '')	<> 	ISNULL (SRC.[AUTO_CALC_HANDLING], '')
OR ISNULL (DST.[AUTO_CALC_RESTOCKING], '')	<> 	ISNULL (SRC.[AUTO_CALC_RESTOCKING], '')
OR ISNULL (DST.[BACKORDERS], '')	<> 	ISNULL (SRC.[BACKORDERS], '')
OR ISNULL (DST.[MEMBER_TYPE], '')	<> 	ISNULL (SRC.[MEMBER_TYPE], '')
OR ISNULL (DST.[PAY_TYPE], '')	<> 	ISNULL (SRC.[PAY_TYPE], '')
OR ISNULL (DST.[PAY_NUMBER], '')	<> 	ISNULL (SRC.[PAY_NUMBER], '')
OR ISNULL (DST.[CREDIT_CARD_EXPIRES], '')	<> 	ISNULL (SRC.[CREDIT_CARD_EXPIRES], '')
OR ISNULL (DST.[AUTHORIZE], '')	<> 	ISNULL (SRC.[AUTHORIZE], '')
OR ISNULL (DST.[CREDIT_CARD_NAME], '')	<> 	ISNULL (SRC.[CREDIT_CARD_NAME], '')
OR ISNULL (DST.[BO_STATUS], '')	<> 	ISNULL (SRC.[BO_STATUS], '')
OR ISNULL (DST.[BO_RELEASE_DATE], '')	<> 	ISNULL (SRC.[BO_RELEASE_DATE], '')
OR ISNULL (DST.[TOTAL_QUANTITY_ORDERED], 0)	<> 	ISNULL (SRC.[TOTAL_QUANTITY_ORDERED], 0)
OR ISNULL (DST.[TOTAL_QUANTITY_BACKORDERED], 0)	<> 	ISNULL (SRC.[TOTAL_QUANTITY_BACKORDERED], 0)
OR ISNULL (DST.[SHIP_METHOD], '')	<> 	ISNULL (SRC.[SHIP_METHOD], '')
OR ISNULL (DST.[TOTAL_WEIGHT], '')	<> 	ISNULL (SRC.[TOTAL_WEIGHT], '')
OR ISNULL (DST.[CASH_GL_ACCT], '')	<> 	ISNULL (SRC.[CASH_GL_ACCT], '')
OR ISNULL (DST.[LINE_PST_TAXABLE], '')	<> 	ISNULL (SRC.[LINE_PST_TAXABLE], '')
OR ISNULL (DST.[INTENT_TO_EDIT], '')	<> 	ISNULL (SRC.[INTENT_TO_EDIT], '')
OR ISNULL (DST.[PREPAID_INVOICE_REFERENCE_NUM], '')	<> 	ISNULL (SRC.[PREPAID_INVOICE_REFERENCE_NUM], '')
OR ISNULL (DST.[AUTO_CALC_FREIGHT], '')	<> 	ISNULL (SRC.[AUTO_CALC_FREIGHT], '')
OR ISNULL (DST.[CO_ID], '')	<> 	ISNULL (SRC.[CO_ID], '')
OR ISNULL (DST.[CO_MEMBER_TYPE], '')	<> 	ISNULL (SRC.[CO_MEMBER_TYPE], '')
OR ISNULL (DST.[EMAIL], '')	<> 	ISNULL (SRC.[EMAIL], '')
OR ISNULL (DST.[CRRT], '')	<> 	ISNULL (SRC.[CRRT], '')
OR ISNULL (DST.[ADDRESS_STATUS], '')	<> 	ISNULL (SRC.[ADDRESS_STATUS], '')
OR ISNULL (DST.[RECOGNIZED_CASH_AMOUNT], '')	<> 	ISNULL (SRC.[RECOGNIZED_CASH_AMOUNT], '')
OR ISNULL (DST.[IS_FR_ORDER], '')	<> 	ISNULL (SRC.[IS_FR_ORDER], '')
OR ISNULL (DST.[VAT_TAX_CODE_FH], '')	<> 	ISNULL (SRC.[VAT_TAX_CODE_FH], '')
OR ISNULL (DST.[ENCRYPT_PAY_NUMBER], '')	<> 	ISNULL (SRC.[ENCRYPT_PAY_NUMBER], '')
OR ISNULL (DST.[ENCRYPT_CREDIT_CARD_EXPIRES], '')	<> 	ISNULL (SRC.[ENCRYPT_CREDIT_CARD_EXPIRES], '')
OR ISNULL (DST.[AUTO_FREIGHT_TYPE], '')	<> 	ISNULL (SRC.[AUTO_FREIGHT_TYPE], '')
OR ISNULL (DST.[USE_MEMBER_PRICE], '')	<> 	ISNULL (SRC.[USE_MEMBER_PRICE], '')
OR ISNULL (DST.[ST_PRINT_COMPANY], '')	<> 	ISNULL (SRC.[ST_PRINT_COMPANY], '')
OR ISNULL (DST.[ST_PRINT_TITLE], '')	<> 	ISNULL (SRC.[ST_PRINT_TITLE], '')
OR ISNULL (DST.[TOLL_FREE], '')	<> 	ISNULL (SRC.[TOLL_FREE], '')
OR ISNULL (DST.[MAIL_CODE], '')	<> 	ISNULL (SRC.[MAIL_CODE], '')
OR ISNULL (DST.[ADDRESS_3], '')	<> 	ISNULL (SRC.[ADDRESS_3], '')
OR ISNULL (DST.[ENCRYPT_CSC], '')	<> 	ISNULL (SRC.[ENCRYPT_CSC], '')
OR ISNULL (DST.[ISSUE_DATE], '')	<> 	ISNULL (SRC.[ISSUE_DATE], '')
OR ISNULL (DST.[ISSUE_NUMBER], '')	<> 	ISNULL (SRC.[ISSUE_NUMBER], '')
OR ISNULL (DST.[MORE_PAYMENTS], 0)	<> 	ISNULL (SRC.[MORE_PAYMENTS], 0)
OR ISNULL (DST.[GATEWAY_REF], '')	<> 	ISNULL (SRC.[GATEWAY_REF], '')
OR ISNULL (DST.[ORIGINATING_TRANS_NUM], '')	<> 	ISNULL (SRC.[ORIGINATING_TRANS_NUM], '')
OR ISNULL (DST.[FREIGHT_TAX], '')	<> 	ISNULL (SRC.[FREIGHT_TAX], '')
OR ISNULL (DST.[HANDLING_TAX], '')	<> 	ISNULL (SRC.[HANDLING_TAX], '')
OR ISNULL (DST.[TAX_RATE_FH], 0)	<> 	ISNULL (SRC.[TAX_RATE_FH], 0)
OR ISNULL (DST.[DISCOUNT_CODE], '')	<> 	ISNULL (SRC.[DISCOUNT_CODE], '')
  
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ORDER_NUMBER]
      ,[ORG_CODE]
      ,[ORDER_TYPE_CODE]
      ,[STAGE]
      ,[SOURCE_SYSTEM]
      ,[BATCH_NUM]
      ,[STATUS]
      ,[HOLD_CODE]
      ,[ORDER_DATE]
      ,[BT_ID]
      ,[ST_ID]
      ,[ST_ADDRESS_NUM]
      ,[ENTERED_DATE_TIME]
      ,[ENTERED_BY]
      ,[UPDATED_DATE_TIME]
      ,[UPDATED_BY]
      ,[INVOICE_REFERENCE_NUM]
      ,[INVOICE_NUMBER]
      ,[INVOICE_DATE]
      ,[NUMBER_LINES]
      ,[FULL_NAME]
      ,[TITLE]
      ,[COMPANY]
      ,[FULL_ADDRESS]
      ,[PREFIX]
      ,[FIRST_NAME]
      ,[MIDDLE_NAME]
      ,[LAST_NAME]
      ,[SUFFIX]
      ,[DESIGNATION]
      ,[INFORMAL]
      ,[LAST_FIRST]
      ,[COMPANY_SORT]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DPB]
      ,[BAR_CODE]
      ,[ADDRESS_FORMAT]
      ,[PHONE]
      ,[FAX]
      ,[NOTES]
      ,[TOTAL_CHARGES]
      ,[TOTAL_PAYMENTS]
      ,[BALANCE]
      ,[LINE_TOTAL]
      ,[LINE_TAXABLE]
      ,[FREIGHT_1]
      ,[FREIGHT_2]
      ,[HANDLING_1]
      ,[HANDLING_2]
      ,[CANCELLATION_FEE]
      ,[TAX_1]
      ,[TAX_2]
      ,[TAX_3]
      ,[LINE_PAY]
      ,[OTHER_PAY]
      ,[AR_PAY]
      ,[TAX_AUTHOR_1]
      ,[TAX_AUTHOR_2]
      ,[TAX_AUTHOR_3]
      ,[TAX_RATE_1]
      ,[TAX_RATE_2]
      ,[TAX_RATE_3]
      ,[TAX_EXEMPT]
      ,[TERMS_CODE]
      ,[SCHEDULED_DATE]
      ,[CONFIRMATION_DATE_TIME]
      ,[SHIP_PAPERS_DATE_TIME]
      ,[SHIPPED_DATE_TIME]
      ,[BO_RELEASED_DATE_TIME]
      ,[SOURCE_CODE]
      ,[SALESMAN]
      ,[COMMISSION_RATE]
      ,[DISCOUNT_RATE]
      ,[PRIORITY]
      ,[HOLD_COMMENT]
      ,[AFFECT_INVENTORY]
      ,[HOLD_FLAG]
      ,[CUSTOMER_REFERENCE]
      ,[VALUATION_BASIS]
      ,[UNDISCOUNTED_TOTAL]
      ,[AUTO_CALC_HANDLING]
      ,[AUTO_CALC_RESTOCKING]
      ,[BACKORDERS]
      ,[MEMBER_TYPE]
      ,[PAY_TYPE]
      ,[PAY_NUMBER]
      ,[CREDIT_CARD_EXPIRES]
      ,[AUTHORIZE]
      ,[CREDIT_CARD_NAME]
      ,[BO_STATUS]
      ,[BO_RELEASE_DATE]
      ,[TOTAL_QUANTITY_ORDERED]
      ,[TOTAL_QUANTITY_BACKORDERED]
      ,[SHIP_METHOD]
      ,[TOTAL_WEIGHT]
      ,[CASH_GL_ACCT]
      ,[LINE_PST_TAXABLE]
      ,[INTENT_TO_EDIT]
      ,[PREPAID_INVOICE_REFERENCE_NUM]
      ,[AUTO_CALC_FREIGHT]
      ,[CO_ID]
      ,[CO_MEMBER_TYPE]
      ,[EMAIL]
      ,[CRRT]
      ,[ADDRESS_STATUS]
      ,[RECOGNIZED_CASH_AMOUNT]
      ,[IS_FR_ORDER]
      ,[VAT_TAX_CODE_FH]
      ,[ENCRYPT_PAY_NUMBER]
      ,[ENCRYPT_CREDIT_CARD_EXPIRES]
      ,[AUTO_FREIGHT_TYPE]
      ,[USE_MEMBER_PRICE]
      ,[ST_PRINT_COMPANY]
      ,[ST_PRINT_TITLE]
      ,[TOLL_FREE]
      ,[MAIL_CODE]
      ,[ADDRESS_3]
      ,[ENCRYPT_CSC]
      ,[ISSUE_DATE]
      ,[ISSUE_NUMBER]
      ,[MORE_PAYMENTS]
      ,[GATEWAY_REF]
      ,[ORIGINATING_TRANS_NUM]
      ,[FREIGHT_TAX]
      ,[HANDLING_TAX]
      ,[TAX_RATE_FH]
      ,[DISCOUNT_CODE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     SRC.[ORDER_NUMBER]
      ,SRC.[ORG_CODE]
      ,SRC.[ORDER_TYPE_CODE]
      ,SRC.[STAGE]
      ,SRC.[SOURCE_SYSTEM]
      ,SRC.[BATCH_NUM]
      ,SRC.[STATUS]
      ,SRC.[HOLD_CODE]
      ,SRC.[ORDER_DATE]
      ,SRC.[BT_ID]
      ,SRC.[ST_ID]
      ,SRC.[ST_ADDRESS_NUM]
      ,SRC.[ENTERED_DATE_TIME]
      ,SRC.[ENTERED_BY]
      ,SRC.[UPDATED_DATE_TIME]
      ,SRC.[UPDATED_BY]
      ,SRC.[INVOICE_REFERENCE_NUM]
      ,SRC.[INVOICE_NUMBER]
      ,SRC.[INVOICE_DATE]
      ,SRC.[NUMBER_LINES]
      ,SRC.[FULL_NAME]
      ,SRC.[TITLE]
      ,SRC.[COMPANY]
      ,SRC.[FULL_ADDRESS]
      ,SRC.[PREFIX]
      ,SRC.[FIRST_NAME]
      ,SRC.[MIDDLE_NAME]
      ,SRC.[LAST_NAME]
      ,SRC.[SUFFIX]
      ,SRC.[DESIGNATION]
      ,SRC.[INFORMAL]
      ,SRC.[LAST_FIRST]
      ,SRC.[COMPANY_SORT]
      ,SRC.[ADDRESS_1]
      ,SRC.[ADDRESS_2]
      ,SRC.[CITY]
      ,SRC.[STATE_PROVINCE]
      ,SRC.[ZIP]
      ,SRC.[COUNTRY]
      ,SRC.[DPB]
      ,SRC.[BAR_CODE]
      ,SRC.[ADDRESS_FORMAT]
      ,SRC.[PHONE]
      ,SRC.[FAX]
      ,SRC.[NOTES]
      ,SRC.[TOTAL_CHARGES]
      ,SRC.[TOTAL_PAYMENTS]
      ,SRC.[BALANCE]
      ,SRC.[LINE_TOTAL]
      ,SRC.[LINE_TAXABLE]
      ,SRC.[FREIGHT_1]
      ,SRC.[FREIGHT_2]
      ,SRC.[HANDLING_1]
      ,SRC.[HANDLING_2]
      ,SRC.[CANCELLATION_FEE]
      ,SRC.[TAX_1]
      ,SRC.[TAX_2]
      ,SRC.[TAX_3]
      ,SRC.[LINE_PAY]
      ,SRC.[OTHER_PAY]
      ,SRC.[AR_PAY]
      ,SRC.[TAX_AUTHOR_1]
      ,SRC.[TAX_AUTHOR_2]
      ,SRC.[TAX_AUTHOR_3]
      ,SRC.[TAX_RATE_1]
      ,SRC.[TAX_RATE_2]
      ,SRC.[TAX_RATE_3]
      ,SRC.[TAX_EXEMPT]
      ,SRC.[TERMS_CODE]
      ,SRC.[SCHEDULED_DATE]
      ,SRC.[CONFIRMATION_DATE_TIME]
      ,SRC.[SHIP_PAPERS_DATE_TIME]
      ,SRC.[SHIPPED_DATE_TIME]
      ,SRC.[BO_RELEASED_DATE_TIME]
      ,SRC.[SOURCE_CODE]
      ,SRC.[SALESMAN]
      ,SRC.[COMMISSION_RATE]
      ,SRC.[DISCOUNT_RATE]
      ,SRC.[PRIORITY]
      ,SRC.[HOLD_COMMENT]
      ,SRC.[AFFECT_INVENTORY]
      ,SRC.[HOLD_FLAG]
      ,SRC.[CUSTOMER_REFERENCE]
      ,SRC.[VALUATION_BASIS]
      ,SRC.[UNDISCOUNTED_TOTAL]
      ,SRC.[AUTO_CALC_HANDLING]
      ,SRC.[AUTO_CALC_RESTOCKING]
      ,SRC.[BACKORDERS]
      ,SRC.[MEMBER_TYPE]
      ,SRC.[PAY_TYPE]
      ,SRC.[PAY_NUMBER]
      ,SRC.[CREDIT_CARD_EXPIRES]
      ,SRC.[AUTHORIZE]
      ,SRC.[CREDIT_CARD_NAME]
      ,SRC.[BO_STATUS]
      ,SRC.[BO_RELEASE_DATE]
      ,SRC.[TOTAL_QUANTITY_ORDERED]
      ,SRC.[TOTAL_QUANTITY_BACKORDERED]
      ,SRC.[SHIP_METHOD]
      ,SRC.[TOTAL_WEIGHT]
      ,SRC.[CASH_GL_ACCT]
      ,SRC.[LINE_PST_TAXABLE]
      ,SRC.[INTENT_TO_EDIT]
      ,SRC.[PREPAID_INVOICE_REFERENCE_NUM]
      ,SRC.[AUTO_CALC_FREIGHT]
      ,SRC.[CO_ID]
      ,SRC.[CO_MEMBER_TYPE]
      ,SRC.[EMAIL]
      ,SRC.[CRRT]
      ,SRC.[ADDRESS_STATUS]
      ,SRC.[RECOGNIZED_CASH_AMOUNT]
      ,SRC.[IS_FR_ORDER]
      ,SRC.[VAT_TAX_CODE_FH]
      ,SRC.[ENCRYPT_PAY_NUMBER]
      ,SRC.[ENCRYPT_CREDIT_CARD_EXPIRES]
      ,SRC.[AUTO_FREIGHT_TYPE]
      ,SRC.[USE_MEMBER_PRICE]
      ,SRC.[ST_PRINT_COMPANY]
      ,SRC.[ST_PRINT_TITLE]
      ,SRC.[TOLL_FREE]
      ,SRC.[MAIL_CODE]
      ,SRC.[ADDRESS_3]
      ,SRC.[ENCRYPT_CSC]
      ,SRC.[ISSUE_DATE]
      ,SRC.[ISSUE_NUMBER]
      ,SRC.[MORE_PAYMENTS]
      ,SRC.[GATEWAY_REF]
      ,SRC.[ORIGINATING_TRANS_NUM]
      ,SRC.[FREIGHT_TAX]
      ,SRC.[HANDLING_TAX]
      ,SRC.[TAX_RATE_FH]
      ,SRC.[DISCOUNT_CODE]
      ,SRC.[TIME_STAMP]
                 ,1
                 ,@Today
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ORDER_NUMBER
       ,deleted.ORDER_NUMBER
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.imis_Orders'
       ,'stg.imis_Orders'
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
				from tmp.imis_Orders
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Orders (
[ORDER_NUMBER]
      ,[ORG_CODE]
      ,[ORDER_TYPE_CODE]
      ,[STAGE]
      ,[SOURCE_SYSTEM]
      ,[BATCH_NUM]
      ,[STATUS]
      ,[HOLD_CODE]
      ,[ORDER_DATE]
      ,[BT_ID]
      ,[ST_ID]
      ,[ST_ADDRESS_NUM]
      ,[ENTERED_DATE_TIME]
      ,[ENTERED_BY]
      ,[UPDATED_DATE_TIME]
      ,[UPDATED_BY]
      ,[INVOICE_REFERENCE_NUM]
      ,[INVOICE_NUMBER]
      ,[INVOICE_DATE]
      ,[NUMBER_LINES]
      ,[FULL_NAME]
      ,[TITLE]
      ,[COMPANY]
      ,[FULL_ADDRESS]
      ,[PREFIX]
      ,[FIRST_NAME]
      ,[MIDDLE_NAME]
      ,[LAST_NAME]
      ,[SUFFIX]
      ,[DESIGNATION]
      ,[INFORMAL]
      ,[LAST_FIRST]
      ,[COMPANY_SORT]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DPB]
      ,[BAR_CODE]
      ,[ADDRESS_FORMAT]
      ,[PHONE]
      ,[FAX]
      ,[NOTES]
      ,[TOTAL_CHARGES]
      ,[TOTAL_PAYMENTS]
      ,[BALANCE]
      ,[LINE_TOTAL]
      ,[LINE_TAXABLE]
      ,[FREIGHT_1]
      ,[FREIGHT_2]
      ,[HANDLING_1]
      ,[HANDLING_2]
      ,[CANCELLATION_FEE]
      ,[TAX_1]
      ,[TAX_2]
      ,[TAX_3]
      ,[LINE_PAY]
      ,[OTHER_PAY]
      ,[AR_PAY]
      ,[TAX_AUTHOR_1]
      ,[TAX_AUTHOR_2]
      ,[TAX_AUTHOR_3]
      ,[TAX_RATE_1]
      ,[TAX_RATE_2]
      ,[TAX_RATE_3]
      ,[TAX_EXEMPT]
      ,[TERMS_CODE]
      ,[SCHEDULED_DATE]
      ,[CONFIRMATION_DATE_TIME]
      ,[SHIP_PAPERS_DATE_TIME]
      ,[SHIPPED_DATE_TIME]
      ,[BO_RELEASED_DATE_TIME]
      ,[SOURCE_CODE]
      ,[SALESMAN]
      ,[COMMISSION_RATE]
      ,[DISCOUNT_RATE]
      ,[PRIORITY]
      ,[HOLD_COMMENT]
      ,[AFFECT_INVENTORY]
      ,[HOLD_FLAG]
      ,[CUSTOMER_REFERENCE]
      ,[VALUATION_BASIS]
      ,[UNDISCOUNTED_TOTAL]
      ,[AUTO_CALC_HANDLING]
      ,[AUTO_CALC_RESTOCKING]
      ,[BACKORDERS]
      ,[MEMBER_TYPE]
      ,[PAY_TYPE]
      ,[PAY_NUMBER]
      ,[CREDIT_CARD_EXPIRES]
      ,[AUTHORIZE]
      ,[CREDIT_CARD_NAME]
      ,[BO_STATUS]
      ,[BO_RELEASE_DATE]
      ,[TOTAL_QUANTITY_ORDERED]
      ,[TOTAL_QUANTITY_BACKORDERED]
      ,[SHIP_METHOD]
      ,[TOTAL_WEIGHT]
      ,[CASH_GL_ACCT]
      ,[LINE_PST_TAXABLE]
      ,[INTENT_TO_EDIT]
      ,[PREPAID_INVOICE_REFERENCE_NUM]
      ,[AUTO_CALC_FREIGHT]
      ,[CO_ID]
      ,[CO_MEMBER_TYPE]
      ,[EMAIL]
      ,[CRRT]
      ,[ADDRESS_STATUS]
      ,[RECOGNIZED_CASH_AMOUNT]
      ,[IS_FR_ORDER]
      ,[VAT_TAX_CODE_FH]
      ,[ENCRYPT_PAY_NUMBER]
      ,[ENCRYPT_CREDIT_CARD_EXPIRES]
      ,[AUTO_FREIGHT_TYPE]
      ,[USE_MEMBER_PRICE]
      ,[ST_PRINT_COMPANY]
      ,[ST_PRINT_TITLE]
      ,[TOLL_FREE]
      ,[MAIL_CODE]
      ,[ADDRESS_3]
      ,[ENCRYPT_CSC]
      ,[ISSUE_DATE]
      ,[ISSUE_NUMBER]
      ,[MORE_PAYMENTS]
      ,[GATEWAY_REF]
      ,[ORIGINATING_TRANS_NUM]
      ,[FREIGHT_TAX]
      ,[HANDLING_TAX]
      ,[TAX_RATE_FH]
      ,[DISCOUNT_CODE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                     [ORDER_NUMBER]
      ,[ORG_CODE]
      ,[ORDER_TYPE_CODE]
      ,[STAGE]
      ,[SOURCE_SYSTEM]
      ,[BATCH_NUM]
      ,[STATUS]
      ,[HOLD_CODE]
      ,[ORDER_DATE]
      ,[BT_ID]
      ,[ST_ID]
      ,[ST_ADDRESS_NUM]
      ,[ENTERED_DATE_TIME]
      ,[ENTERED_BY]
      ,[UPDATED_DATE_TIME]
      ,[UPDATED_BY]
      ,[INVOICE_REFERENCE_NUM]
      ,[INVOICE_NUMBER]
      ,[INVOICE_DATE]
      ,[NUMBER_LINES]
      ,[FULL_NAME]
      ,[TITLE]
      ,[COMPANY]
      ,[FULL_ADDRESS]
      ,[PREFIX]
      ,[FIRST_NAME]
      ,[MIDDLE_NAME]
      ,[LAST_NAME]
      ,[SUFFIX]
      ,[DESIGNATION]
      ,[INFORMAL]
      ,[LAST_FIRST]
      ,[COMPANY_SORT]
      ,[ADDRESS_1]
      ,[ADDRESS_2]
      ,[CITY]
      ,[STATE_PROVINCE]
      ,[ZIP]
      ,[COUNTRY]
      ,[DPB]
      ,[BAR_CODE]
      ,[ADDRESS_FORMAT]
      ,[PHONE]
      ,[FAX]
      ,[NOTES]
      ,[TOTAL_CHARGES]
      ,[TOTAL_PAYMENTS]
      ,[BALANCE]
      ,[LINE_TOTAL]
      ,[LINE_TAXABLE]
      ,[FREIGHT_1]
      ,[FREIGHT_2]
      ,[HANDLING_1]
      ,[HANDLING_2]
      ,[CANCELLATION_FEE]
      ,[TAX_1]
      ,[TAX_2]
      ,[TAX_3]
      ,[LINE_PAY]
      ,[OTHER_PAY]
      ,[AR_PAY]
      ,[TAX_AUTHOR_1]
      ,[TAX_AUTHOR_2]
      ,[TAX_AUTHOR_3]
      ,[TAX_RATE_1]
      ,[TAX_RATE_2]
      ,[TAX_RATE_3]
      ,[TAX_EXEMPT]
      ,[TERMS_CODE]
      ,[SCHEDULED_DATE]
      ,[CONFIRMATION_DATE_TIME]
      ,[SHIP_PAPERS_DATE_TIME]
      ,[SHIPPED_DATE_TIME]
      ,[BO_RELEASED_DATE_TIME]
      ,[SOURCE_CODE]
      ,[SALESMAN]
      ,[COMMISSION_RATE]
      ,[DISCOUNT_RATE]
      ,[PRIORITY]
      ,[HOLD_COMMENT]
      ,[AFFECT_INVENTORY]
      ,[HOLD_FLAG]
      ,[CUSTOMER_REFERENCE]
      ,[VALUATION_BASIS]
      ,[UNDISCOUNTED_TOTAL]
      ,[AUTO_CALC_HANDLING]
      ,[AUTO_CALC_RESTOCKING]
      ,[BACKORDERS]
      ,[MEMBER_TYPE]
      ,[PAY_TYPE]
      ,[PAY_NUMBER]
      ,[CREDIT_CARD_EXPIRES]
      ,[AUTHORIZE]
      ,[CREDIT_CARD_NAME]
      ,[BO_STATUS]
      ,[BO_RELEASE_DATE]
      ,[TOTAL_QUANTITY_ORDERED]
      ,[TOTAL_QUANTITY_BACKORDERED]
      ,[SHIP_METHOD]
      ,[TOTAL_WEIGHT]
      ,[CASH_GL_ACCT]
      ,[LINE_PST_TAXABLE]
      ,[INTENT_TO_EDIT]
      ,[PREPAID_INVOICE_REFERENCE_NUM]
      ,[AUTO_CALC_FREIGHT]
      ,[CO_ID]
      ,[CO_MEMBER_TYPE]
      ,[EMAIL]
      ,[CRRT]
      ,[ADDRESS_STATUS]
      ,[RECOGNIZED_CASH_AMOUNT]
      ,[IS_FR_ORDER]
      ,[VAT_TAX_CODE_FH]
      ,[ENCRYPT_PAY_NUMBER]
      ,[ENCRYPT_CREDIT_CARD_EXPIRES]
      ,[AUTO_FREIGHT_TYPE]
      ,[USE_MEMBER_PRICE]
      ,[ST_PRINT_COMPANY]
      ,[ST_PRINT_TITLE]
      ,[TOLL_FREE]
      ,[MAIL_CODE]
      ,[ADDRESS_3]
      ,[ENCRYPT_CSC]
      ,[ISSUE_DATE]
      ,[ISSUE_NUMBER]
      ,[MORE_PAYMENTS]
      ,[GATEWAY_REF]
      ,[ORIGINATING_TRANS_NUM]
      ,[FREIGHT_TAX]
      ,[HANDLING_TAX]
      ,[TAX_RATE_FH]
      ,[DISCOUNT_CODE]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Orders
WHERE order_number IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp



/****************Order_Lines *************************/
MERGE stg.imis_Order_Lines AS DST
USING tmp.imis_Order_Lines AS SRC
       ON DST.Order_Number = SRC.Order_Number and
	   DST.Line_Number = SRC.Line_Number
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
   ISNULL (DST.[PRODUCT_CODE], '')	<> 	ISNULL (SRC.[PRODUCT_CODE], '')
OR ISNULL (DST.[LOCATION], '')	<> 	ISNULL (SRC.[LOCATION], '')
OR ISNULL (DST.[LOT_SERIAL], '')	<> 	ISNULL (SRC.[LOT_SERIAL], '')
OR ISNULL (DST.[DESCRIPTION], '')	<> 	ISNULL (SRC.[DESCRIPTION], '')
OR ISNULL (DST.[QUANTITY_ORDERED], 0)	<> 	ISNULL (SRC.[QUANTITY_ORDERED], 0)
OR ISNULL (DST.[QUANTITY_SHIPPED], 0)	<> 	ISNULL (SRC.[QUANTITY_SHIPPED], 0)
OR ISNULL (DST.[QUANTITY_BACKORDERED], 0)	<> 	ISNULL (SRC.[QUANTITY_BACKORDERED], 0)
OR ISNULL (DST.[QUANTITY_RESERVED], 0)	<> 	ISNULL (SRC.[QUANTITY_RESERVED], 0)
OR ISNULL (DST.[QUANTITY_COMMITTED], 0)	<> 	ISNULL (SRC.[QUANTITY_COMMITTED], 0)
OR ISNULL (DST.[NUMBER_DAYS], '')	<> 	ISNULL (SRC.[NUMBER_DAYS], '')
OR ISNULL (DST.[TAXABLE], '')	<> 	ISNULL (SRC.[TAXABLE], '')
OR ISNULL (DST.[TAXABLE_2], '')	<> 	ISNULL (SRC.[TAXABLE_2], '')
OR ISNULL (DST.[UNIT_PRICE], '')	<> 	ISNULL (SRC.[UNIT_PRICE], '')
OR ISNULL (DST.[UNIT_COST], '')	<> 	ISNULL (SRC.[UNIT_COST], '')
OR ISNULL (DST.[UNDISCOUNTED_PRICE], '')	<> 	ISNULL (SRC.[UNDISCOUNTED_PRICE], '')
OR ISNULL (DST.[EXTENDED_AMOUNT], '')	<> 	ISNULL (SRC.[EXTENDED_AMOUNT], '')
OR ISNULL (DST.[EXTENDED_COST], '')	<> 	ISNULL (SRC.[EXTENDED_COST], '')
OR ISNULL (DST.[UNDISCOUNTED_EXTENDED_AMOUNT], '')	<> 	ISNULL (SRC.[UNDISCOUNTED_EXTENDED_AMOUNT], '')
OR ISNULL (DST.[CANCEL_CODE], '')	<> 	ISNULL (SRC.[CANCEL_CODE], '')
OR ISNULL (DST.[CANCEL_QUANTITY], 0)	<> 	ISNULL (SRC.[CANCEL_QUANTITY], 0)
OR ISNULL (DST.[COMMISSION_RATE], 0)	<> 	ISNULL (SRC.[COMMISSION_RATE], 0)
OR ISNULL (DST.[COMMISSION_AMOUNT], '')	<> 	ISNULL (SRC.[COMMISSION_AMOUNT], '')
OR ISNULL (DST.[CEU_TYPE], '')	<> 	ISNULL (SRC.[CEU_TYPE], '')
OR ISNULL (DST.[CEU_AWARDED], 0)	<> 	ISNULL (SRC.[CEU_AWARDED], 0)
OR ISNULL (DST.[PASS_FAIL], '')	<> 	ISNULL (SRC.[PASS_FAIL], '')
OR ISNULL (DST.[DATE_CONFIRMED], '')	<> 	ISNULL (SRC.[DATE_CONFIRMED], '')
OR ISNULL (DST.[TICKETS_PRINTED], '')	<> 	ISNULL (SRC.[TICKETS_PRINTED], '')
OR ISNULL (DST.[BOOTH_NUMBERS], '')	<> 	ISNULL (SRC.[BOOTH_NUMBERS], '')
OR ISNULL (DST.[NOTE], '')	<> 	ISNULL (SRC.[NOTE], '')
OR ISNULL (DST.[INCOME_ACCOUNT], '')	<> 	ISNULL (SRC.[INCOME_ACCOUNT], '')
OR ISNULL (DST.[UNIT_WEIGHT], 0)	<> 	ISNULL (SRC.[UNIT_WEIGHT], 0)
OR ISNULL (DST.[EXTENDED_WEIGHT], 0)	<> 	ISNULL (SRC.[EXTENDED_WEIGHT], 0)
OR ISNULL (DST.[PST_TAXABLE], '')	<> 	ISNULL (SRC.[PST_TAXABLE], '')
OR ISNULL (DST.[IS_GST_TAXABLE], '')	<> 	ISNULL (SRC.[IS_GST_TAXABLE], '')
OR ISNULL (DST.[ADDED_TO_WAIT_LIST], '')	<> 	ISNULL (SRC.[ADDED_TO_WAIT_LIST], '')
OR ISNULL (DST.[BIN], '')	<> 	ISNULL (SRC.[BIN], '')
OR ISNULL (DST.[TAX_AUTHORITY], '')	<> 	ISNULL (SRC.[TAX_AUTHORITY], '')
OR ISNULL (DST.[TAX_RATE], 0)	<> 	ISNULL (SRC.[TAX_RATE], 0)
OR ISNULL (DST.[TAX_1], 0)	<> 	ISNULL (SRC.[TAX_1], 0)
OR ISNULL (DST.[KIT_ITEM_TYPE], '')	<> 	ISNULL (SRC.[KIT_ITEM_TYPE], '')
OR ISNULL (DST.[DISCOUNT], 0)	<> 	ISNULL (SRC.[DISCOUNT], 0)
OR ISNULL (DST.[UF_1], '')	<> 	ISNULL (SRC.[UF_1], '')
OR ISNULL (DST.[UF_2], '')	<> 	ISNULL (SRC.[UF_2], '')
OR ISNULL (DST.[UF_3], '')	<> 	ISNULL (SRC.[UF_3], '')
OR ISNULL (DST.[UF_4], '')	<> 	ISNULL (SRC.[UF_4], '')
OR ISNULL (DST.[EXTENDED_SQUARE_FEET], 0)	<> 	ISNULL (SRC.[EXTENDED_SQUARE_FEET], 0)
OR ISNULL (DST.[SQUARE_FEET], 0)	<> 	ISNULL (SRC.[SQUARE_FEET], 0)
OR ISNULL (DST.[MEET_APPEAL], '')	<> 	ISNULL (SRC.[MEET_APPEAL], '')
OR ISNULL (DST.[MEET_CAMPAIGN], '')	<> 	ISNULL (SRC.[MEET_CAMPAIGN], '')
OR ISNULL (DST.[FAIR_MARKET_VALUE], 0)	<> 	ISNULL (SRC.[FAIR_MARKET_VALUE], 0)
OR ISNULL (DST.[ORG_CODE], '')	<> 	ISNULL (SRC.[ORG_CODE], '')
OR ISNULL (DST.[IS_FR_ITEM], '')	<> 	ISNULL (SRC.[IS_FR_ITEM], '')
OR ISNULL (DST.[MANUAL_PRICE], 0)	<> 	ISNULL (SRC.[MANUAL_PRICE], 0)
OR ISNULL (DST.[IS_MANUAL_PRICE], '')	<> 	ISNULL (SRC.[IS_MANUAL_PRICE], '')
OR ISNULL (DST.[UNIT_TAX_AMOUNT], 0)	<> 	ISNULL (SRC.[UNIT_TAX_AMOUNT], 0)
OR ISNULL (DST.[PRICE_FROM_COMPONENTS], '')	<> 	ISNULL (SRC.[PRICE_FROM_COMPONENTS], '')
OR ISNULL (DST.[QUANTITY_PER_KIT], 0)	<> 	ISNULL (SRC.[QUANTITY_PER_KIT], 0)
OR ISNULL (DST.[DISCOUNT_CODE], '')	<> 	ISNULL (SRC.[DISCOUNT_CODE], '')
  
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ORDER_NUMBER]
      ,[LINE_NUMBER]
      ,[PRODUCT_CODE]
      ,[LOCATION]
      ,[LOT_SERIAL]
      ,[DESCRIPTION]
      ,[QUANTITY_ORDERED]
      ,[QUANTITY_SHIPPED]
      ,[QUANTITY_BACKORDERED]
      ,[QUANTITY_RESERVED]
      ,[QUANTITY_COMMITTED]
      ,[NUMBER_DAYS]
      ,[TAXABLE]
      ,[TAXABLE_2]
      ,[UNIT_PRICE]
      ,[UNIT_COST]
      ,[UNDISCOUNTED_PRICE]
      ,[EXTENDED_AMOUNT]
      ,[EXTENDED_COST]
      ,[UNDISCOUNTED_EXTENDED_AMOUNT]
      ,[CANCEL_CODE]
      ,[CANCEL_QUANTITY]
      ,[COMMISSION_RATE]
      ,[COMMISSION_AMOUNT]
      ,[CEU_TYPE]
      ,[CEU_AWARDED]
      ,[PASS_FAIL]
      ,[DATE_CONFIRMED]
      ,[TICKETS_PRINTED]
      ,[BOOTH_NUMBERS]
      ,[NOTE]
      ,[INCOME_ACCOUNT]
      ,[UNIT_WEIGHT]
      ,[EXTENDED_WEIGHT]
      ,[PST_TAXABLE]
      ,[IS_GST_TAXABLE]
      ,[ADDED_TO_WAIT_LIST]
      ,[BIN]
      ,[TAX_AUTHORITY]
      ,[TAX_RATE]
      ,[TAX_1]
      ,[KIT_ITEM_TYPE]
      ,[DISCOUNT]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[EXTENDED_SQUARE_FEET]
      ,[SQUARE_FEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[FAIR_MARKET_VALUE]
      ,[ORG_CODE]
      ,[IS_FR_ITEM]
      ,[MANUAL_PRICE]
      ,[IS_MANUAL_PRICE]
      ,[UNIT_TAX_AMOUNT]
      ,[PRICE_FROM_COMPONENTS]
      ,[QUANTITY_PER_KIT]
      ,[DISCOUNT_CODE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     SRC.[ORDER_NUMBER]
      ,SRC.[LINE_NUMBER]
      ,SRC.[PRODUCT_CODE]
      ,SRC.[LOCATION]
      ,SRC.[LOT_SERIAL]
      ,SRC.[DESCRIPTION]
      ,SRC.[QUANTITY_ORDERED]
      ,SRC.[QUANTITY_SHIPPED]
      ,SRC.[QUANTITY_BACKORDERED]
      ,SRC.[QUANTITY_RESERVED]
      ,SRC.[QUANTITY_COMMITTED]
      ,SRC.[NUMBER_DAYS]
      ,SRC.[TAXABLE]
      ,SRC.[TAXABLE_2]
      ,SRC.[UNIT_PRICE]
      ,SRC.[UNIT_COST]
      ,SRC.[UNDISCOUNTED_PRICE]
      ,SRC.[EXTENDED_AMOUNT]
      ,SRC.[EXTENDED_COST]
      ,SRC.[UNDISCOUNTED_EXTENDED_AMOUNT]
      ,SRC.[CANCEL_CODE]
      ,SRC.[CANCEL_QUANTITY]
      ,SRC.[COMMISSION_RATE]
      ,SRC.[COMMISSION_AMOUNT]
      ,SRC.[CEU_TYPE]
      ,SRC.[CEU_AWARDED]
      ,SRC.[PASS_FAIL]
      ,SRC.[DATE_CONFIRMED]
      ,SRC.[TICKETS_PRINTED]
      ,SRC.[BOOTH_NUMBERS]
      ,SRC.[NOTE]
      ,SRC.[INCOME_ACCOUNT]
      ,SRC.[UNIT_WEIGHT]
      ,SRC.[EXTENDED_WEIGHT]
      ,SRC.[PST_TAXABLE]
      ,SRC.[IS_GST_TAXABLE]
      ,SRC.[ADDED_TO_WAIT_LIST]
      ,SRC.[BIN]
      ,SRC.[TAX_AUTHORITY]
      ,SRC.[TAX_RATE]
      ,SRC.[TAX_1]
      ,SRC.[KIT_ITEM_TYPE]
      ,SRC.[DISCOUNT]
      ,SRC.[UF_1]
      ,SRC.[UF_2]
      ,SRC.[UF_3]
      ,SRC.[UF_4]
      ,SRC.[EXTENDED_SQUARE_FEET]
      ,SRC.[SQUARE_FEET]
      ,SRC.[MEET_APPEAL]
      ,SRC.[MEET_CAMPAIGN]
      ,SRC.[FAIR_MARKET_VALUE]
      ,SRC.[ORG_CODE]
      ,SRC.[IS_FR_ITEM]
      ,SRC.[MANUAL_PRICE]
      ,SRC.[IS_MANUAL_PRICE]
      ,SRC.[UNIT_TAX_AMOUNT]
      ,SRC.[PRICE_FROM_COMPONENTS]
      ,SRC.[QUANTITY_PER_KIT]
      ,SRC.[DISCOUNT_CODE]
      ,SRC.[TIME_STAMP]
                 ,1
                 ,@Today
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ORDER_NUMBER
       ,deleted.ORDER_NUMBER
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.imis_Order_Lines'
       ,'stg.imis_Order_Lines'
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
				from tmp.imis_Order_Lines
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Order_Lines (
[ORDER_NUMBER]
      ,[LINE_NUMBER]
      ,[PRODUCT_CODE]
      ,[LOCATION]
      ,[LOT_SERIAL]
      ,[DESCRIPTION]
      ,[QUANTITY_ORDERED]
      ,[QUANTITY_SHIPPED]
      ,[QUANTITY_BACKORDERED]
      ,[QUANTITY_RESERVED]
      ,[QUANTITY_COMMITTED]
      ,[NUMBER_DAYS]
      ,[TAXABLE]
      ,[TAXABLE_2]
      ,[UNIT_PRICE]
      ,[UNIT_COST]
      ,[UNDISCOUNTED_PRICE]
      ,[EXTENDED_AMOUNT]
      ,[EXTENDED_COST]
      ,[UNDISCOUNTED_EXTENDED_AMOUNT]
      ,[CANCEL_CODE]
      ,[CANCEL_QUANTITY]
      ,[COMMISSION_RATE]
      ,[COMMISSION_AMOUNT]
      ,[CEU_TYPE]
      ,[CEU_AWARDED]
      ,[PASS_FAIL]
      ,[DATE_CONFIRMED]
      ,[TICKETS_PRINTED]
      ,[BOOTH_NUMBERS]
      ,[NOTE]
      ,[INCOME_ACCOUNT]
      ,[UNIT_WEIGHT]
      ,[EXTENDED_WEIGHT]
      ,[PST_TAXABLE]
      ,[IS_GST_TAXABLE]
      ,[ADDED_TO_WAIT_LIST]
      ,[BIN]
      ,[TAX_AUTHORITY]
      ,[TAX_RATE]
      ,[TAX_1]
      ,[KIT_ITEM_TYPE]
      ,[DISCOUNT]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[EXTENDED_SQUARE_FEET]
      ,[SQUARE_FEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[FAIR_MARKET_VALUE]
      ,[ORG_CODE]
      ,[IS_FR_ITEM]
      ,[MANUAL_PRICE]
      ,[IS_MANUAL_PRICE]
      ,[UNIT_TAX_AMOUNT]
      ,[PRICE_FROM_COMPONENTS]
      ,[QUANTITY_PER_KIT]
      ,[DISCOUNT_CODE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                    [ORDER_NUMBER]
      ,[LINE_NUMBER]
      ,[PRODUCT_CODE]
      ,[LOCATION]
      ,[LOT_SERIAL]
      ,[DESCRIPTION]
      ,[QUANTITY_ORDERED]
      ,[QUANTITY_SHIPPED]
      ,[QUANTITY_BACKORDERED]
      ,[QUANTITY_RESERVED]
      ,[QUANTITY_COMMITTED]
      ,[NUMBER_DAYS]
      ,[TAXABLE]
      ,[TAXABLE_2]
      ,[UNIT_PRICE]
      ,[UNIT_COST]
      ,[UNDISCOUNTED_PRICE]
      ,[EXTENDED_AMOUNT]
      ,[EXTENDED_COST]
      ,[UNDISCOUNTED_EXTENDED_AMOUNT]
      ,[CANCEL_CODE]
      ,[CANCEL_QUANTITY]
      ,[COMMISSION_RATE]
      ,[COMMISSION_AMOUNT]
      ,[CEU_TYPE]
      ,[CEU_AWARDED]
      ,[PASS_FAIL]
      ,[DATE_CONFIRMED]
      ,[TICKETS_PRINTED]
      ,[BOOTH_NUMBERS]
      ,[NOTE]
      ,[INCOME_ACCOUNT]
      ,[UNIT_WEIGHT]
      ,[EXTENDED_WEIGHT]
      ,[PST_TAXABLE]
      ,[IS_GST_TAXABLE]
      ,[ADDED_TO_WAIT_LIST]
      ,[BIN]
      ,[TAX_AUTHORITY]
      ,[TAX_RATE]
      ,[TAX_1]
      ,[KIT_ITEM_TYPE]
      ,[DISCOUNT]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[EXTENDED_SQUARE_FEET]
      ,[SQUARE_FEET]
      ,[MEET_APPEAL]
      ,[MEET_CAMPAIGN]
      ,[FAIR_MARKET_VALUE]
      ,[ORG_CODE]
      ,[IS_FR_ITEM]
      ,[MANUAL_PRICE]
      ,[IS_MANUAL_PRICE]
      ,[UNIT_TAX_AMOUNT]
      ,[PRICE_FROM_COMPONENTS]
      ,[QUANTITY_PER_KIT]
      ,[DISCOUNT_CODE]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_order_lines
WHERE order_number IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

--  Below process will delete any duplicate records that got inserted from the previous step which is possible if a Member ID has a new order
--  and a changed order on the same day
;with cte as
(select ORDER_NUMBER, LINE_NUMBER, quantity_ordered, count(*) as ttlcount 
from stg.imis_Order_Lines
where IsActive = 1 
group by ORDER_NUMBER, LINE_NUMBER, quantity_ordered
having count(*) > 1
)

, cte2 as
(
select ORDER_NUMBER, LINE_NUMBER, quantity_ordered, min(Id_Identitycolumn) as identityval from stg.imis_Order_Lines A where isactive = 1 and exists
(Select * from cte B where A.ORDER_NUMBER = B.ORDER_NUMBER and A.LINE_NUMBER = B.LINE_NUMBER and A.QUANTITY_ORDERED = B.QUANTITY_ORDERED
)
group by ORDER_NUMBER, LINE_NUMBER, quantity_ordered
)

delete from stg.imis_Order_Lines where Id_Identitycolumn in
(Select identityval from cte2)

/****************Order_Meet *************************/
MERGE stg.imis_Order_Meet AS DST
USING tmp.imis_Order_Meet AS SRC
       ON DST.Order_Number = SRC.Order_Number
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
   ISNULL (DST.MEETING, '')	<> 	ISNULL (SRC.MEETING, '')
OR ISNULL (DST.REGISTRANT_CLASS, '')	<> 	ISNULL (SRC.REGISTRANT_CLASS, '')
OR ISNULL (DST.ARRIVAL, '')	<> 	ISNULL (SRC.ARRIVAL, '')
OR ISNULL (DST.DEPARTURE, '')	<> 	ISNULL (SRC.DEPARTURE, '')
OR ISNULL (DST.HOTEL, '')	<> 	ISNULL (SRC.HOTEL, '')
OR ISNULL (DST.LODGING_INSTRUCTIONS, '')	<> 	ISNULL (SRC.LODGING_INSTRUCTIONS, '')
OR ISNULL (DST.BOOTH, '')	<> 	ISNULL (SRC.BOOTH, '')
OR ISNULL (DST.GUEST_FIRST, '')	<> 	ISNULL (SRC.GUEST_FIRST, '')
OR ISNULL (DST.GUEST_MIDDLE, '')	<> 	ISNULL (SRC.GUEST_MIDDLE, '')
OR ISNULL (DST.GUEST_LAST, '')	<> 	ISNULL (SRC.GUEST_LAST, '')
OR ISNULL (DST.GUEST_IS_SPOUSE, '')	<> 	ISNULL (SRC.GUEST_IS_SPOUSE, '')
OR ISNULL (DST.ADDITIONAL_BADGES, '')	<> 	ISNULL (SRC.ADDITIONAL_BADGES, '')
OR ISNULL (DST.DELEGATE, '')	<> 	ISNULL (SRC.DELEGATE, '')
OR ISNULL (DST.UF_1, '')	<> 	ISNULL (SRC.UF_1, '')
OR ISNULL (DST.UF_2, '')	<> 	ISNULL (SRC.UF_2, '')
OR ISNULL (DST.UF_3, '')	<> 	ISNULL (SRC.UF_3, '')
OR ISNULL (DST.UF_4, '')	<> 	ISNULL (SRC.UF_4, '')
OR ISNULL (DST.UF_5, '')	<> 	ISNULL (SRC.UF_5, '')
OR ISNULL (DST.UF_6, '')	<> 	ISNULL (SRC.UF_6, '')
OR ISNULL (DST.UF_7, '')	<> 	ISNULL (SRC.UF_7, '')
OR ISNULL (DST.UF_8, '')	<> 	ISNULL (SRC.UF_8, '')
OR ISNULL (DST.SHARE_STATUS, '')	<> 	ISNULL (SRC.SHARE_STATUS, '')
OR ISNULL (DST.SHARE_ORDER_NUMBER, 0)	<> 	ISNULL (SRC.SHARE_ORDER_NUMBER, 0)
OR ISNULL (DST.ROOM_TYPE, '')	<> 	ISNULL (SRC.ROOM_TYPE, '')
OR ISNULL (DST.ROOM_QUANTITY, '')	<> 	ISNULL (SRC.ROOM_QUANTITY, '')
OR ISNULL (DST.ROOM_CONFIRM, '')	<> 	ISNULL (SRC.ROOM_CONFIRM, '')
OR ISNULL (DST.UF_9, '')	<> 	ISNULL (SRC.UF_9, '')
OR ISNULL (DST.UF_10, '')	<> 	ISNULL (SRC.UF_10, '')
OR ISNULL (DST.ARRIVAL_TIME, '')	<> 	ISNULL (SRC.ARRIVAL_TIME, '')
OR ISNULL (DST.DEPARTURE_TIME, '')	<> 	ISNULL (SRC.DEPARTURE_TIME, '')
OR ISNULL (DST.COMP_REGISTRATIONS, '')	<> 	ISNULL (SRC.COMP_REGISTRATIONS, '')
OR ISNULL (DST.COMP_REG_SOURCE, 0)	<> 	ISNULL (SRC.COMP_REG_SOURCE, 0)
OR ISNULL (DST.TOTAL_SQUARE_FEET, 0)	<> 	ISNULL (SRC.TOTAL_SQUARE_FEET, 0)
OR ISNULL (DST.COMP_REGISTRATIONS_USED, '')	<> 	ISNULL (SRC.COMP_REGISTRATIONS_USED, '')
OR ISNULL (DST.PARENT_ORDER_NUMBER, 0)	<> 	ISNULL (SRC.PARENT_ORDER_NUMBER, 0)
OR ISNULL (DST.REGISTERED_BY_ID, '')	<> 	ISNULL (SRC.REGISTERED_BY_ID, '')

  
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     SRC.[ORDER_NUMBER]
      ,SRC.[MEETING]
      ,SRC.[REGISTRANT_CLASS]
      ,SRC.[ARRIVAL]
      ,SRC.[DEPARTURE]
      ,SRC.[HOTEL]
      ,SRC.[LODGING_INSTRUCTIONS]
      ,SRC.[BOOTH]
      ,SRC.[GUEST_FIRST]
      ,SRC.[GUEST_MIDDLE]
      ,SRC.[GUEST_LAST]
      ,SRC.[GUEST_IS_SPOUSE]
      ,SRC.[ADDITIONAL_BADGES]
      ,SRC.[DELEGATE]
      ,SRC.[UF_1]
      ,SRC.[UF_2]
      ,SRC.[UF_3]
      ,SRC.[UF_4]
      ,SRC.[UF_5]
      ,SRC.[UF_6]
      ,SRC.[UF_7]
      ,SRC.[UF_8]
      ,SRC.[SHARE_STATUS]
      ,SRC.[SHARE_ORDER_NUMBER]
      ,SRC.[ROOM_TYPE]
      ,SRC.[ROOM_QUANTITY]
      ,SRC.[ROOM_CONFIRM]
      ,SRC.[UF_9]
      ,SRC.[UF_10]
      ,SRC.[ARRIVAL_TIME]
      ,SRC.[DEPARTURE_TIME]
      ,SRC.[COMP_REGISTRATIONS]
      ,SRC.[COMP_REG_SOURCE]
      ,SRC.[TOTAL_SQUARE_FEET]
      ,SRC.[COMP_REGISTRATIONS_USED]
      ,SRC.[PARENT_ORDER_NUMBER]
      ,SRC.[REGISTERED_BY_ID]
      ,SRC.[TIME_STAMP]
                 ,1
                 ,@Today
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ORDER_NUMBER
       ,deleted.ORDER_NUMBER
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.imis_Order_Meet'
       ,'stg.imis_Order_Meet'
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
				from tmp.imis_Order_Meet
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Order_Meet (
[ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                    [ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_order_meet
WHERE order_number IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp


--			  /**********************************************CUSTOM_EVENT_REGISTRATION*********************************************/
-----add new event registration records to the Custom Event Registration Staging Table


----IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
----       Truncate TABLE #audittemp

----CREATE TABLE #audittemp (
----       action NVARCHAR(20)
----       ,inserted_id varchar(30)
----       ,deleted_id varchar(30)
----       );

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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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

--  Below process will delete any duplicate records that got inserted from the previous step which is possible if a Member ID has a new registration
--  and a changed registration on the same day
;with cte as
(select ID, SEQN, MEETING, count(*) as ttlcount 
from stg.imis_Custom_Event_Registration
where IsActive = 1 
group by ID, SEQN, MEETING
having count(*) > 1
)


, cte2 as
(
select ID, SEQN, MEETING, min(Id_Identitycolumn) as identityval from stg.imis_Custom_Event_Registration A where isactive = 1 and exists
(Select * from cte B where A.ID = B.ID and A.SEQN = B.SEQN and A.MEETING = B.MEETING
)
group by ID, SEQN, MEETING

)

delete from stg.imis_Custom_Event_Registration  where Id_Identitycolumn in
(Select identityval from cte2)

/**********************************************CUSTOM_EVENT_SCHEDULE********************************************/
---add new event schedule records to the Custom Event Schedule Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
       @PipelineName
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

--  Below process will delete any duplicate records that got inserted from the previous step which is possible if a Member ID has a new event
--  and a changed event on the same day
;with cte as
(select ID, SEQN, PRODUCT_CODE, count(*) as ttlcount 
from stg.imis_Custom_Event_Schedule
where IsActive = 1 
group by id, SEQN, PRODUCT_CODE
having count(*) > 1
)

, cte2 as
(
select ID, SEQN, PRODUCT_CODE, min(Id_Identitycolumn) as identityval from stg.imis_Custom_Event_Schedule A where isactive = 1 and exists
(Select * from cte B where A.ID = B.ID and A.SEQN = B.SEQN and A.PRODUCT_CODE = B.PRODUCT_CODE
)
group by ID, SEQN, PRODUCT_CODE

)

delete from stg.imis_Custom_Event_Schedule  where Id_Identitycolumn in
(Select identityval from cte2)


/****************************************Trans********************/
---add new Transaction records to the Transaction Staging Table
--MERGE stg.imis_Trans AS DST
--USING tmp.imis_Trans AS SRC
--       ON DST.TRANS_NUMBER = SRC.TRANS_NUMBER 
--	   AND DST.LINE_NUMBER = SRC.LINE_NUMBER
--	   AND DST.SUB_LINE_NUMBER = SRC.SUB_LINE_NUMBER
--WHEN NOT MATCHED
--THEN
--INSERT (
--[TRANS_NUMBER]
--      ,[LINE_NUMBER]
--      ,[BATCH_NUM]
--      ,[OWNER_ORG_CODE]
--      ,[SOURCE_SYSTEM]
--      ,[JOURNAL_TYPE]
--      ,[TRANSACTION_TYPE]
--      ,[TRANSACTION_DATE]
--      ,[BT_ID]
--      ,[ST_ID]
--      ,[INVOICE_REFERENCE_NUM]
--      ,[DESCRIPTION]
--      ,[CUSTOMER_NAME]
--      ,[CUSTOMER_REFERENCE]
--      ,[REFERENCE_1]
--      ,[SOURCE_CODE]
--      ,[PRODUCT_CODE]
--      ,[EFFECTIVE_DATE]
--      ,[PAID_THRU]
--      ,[MONTHS_PAID]
--      ,[FISCAL_PERIOD]
--      ,[DEFERRAL_MONTHS]
--      ,[AMOUNT]
--      ,[ADJUSTMENT_AMOUNT]
--      ,[PSEUDO_ACCOUNT]
--      ,[GL_ACCT_ORG_CODE]
--      ,[GL_ACCOUNT]
--      ,[DEFERRED_GL_ACCOUNT]
--      ,[INVOICE_CHARGES]
--      ,[INVOICE_CREDITS]
--      ,[QUANTITY]
--      ,[UNIT_PRICE]
--      ,[PAYMENT_TYPE]
--      ,[CHECK_NUMBER]
--      ,[CC_NUMBER]
--      ,[CC_EXPIRE]
--      ,[CC_AUTHORIZE]
--      ,[CC_NAME]
--      ,[TERMS_CODE]
--      ,[ACTIVITY_SEQN]
--      ,[POSTED]
--      ,[PROD_TYPE]
--      ,[ACTIVITY_TYPE]
--      ,[ACTION_CODES]
--      ,[TICKLER_DATE]
--      ,[DATE_ENTERED]
--      ,[ENTERED_BY]
--      ,[SUB_LINE_NUMBER]
--      ,[INSTALL_BILL_DATE]
--      ,[TAXABLE_VALUE]
--      ,[SOLICITOR_ID]
--      ,[INVOICE_ADJUSTMENTS]
--      ,[INVOICE_LINE_NUM]
--      ,[MERGE_CODE]
--      ,[SALUTATION_CODE]
--      ,[SENDER_CODE]
--      ,[IS_MATCH_GIFT]
--      ,[MATCH_GIFT_TRANS_NUM]
--      ,[MATCH_ACTIVITY_SEQN]
--      ,[MEM_TRIB_ID]
--      ,[RECEIPT_ID]
--      ,[DO_NOT_RECEIPT]
--      ,[CC_STATUS]
--      ,[ENCRYPT_CC_NUMBER]
--      ,[ENCRYPT_CC_EXPIRE]
--      ,[FR_ACTIVITY]
--      ,[FR_ACTIVITY_SEQN]
--      ,[MEM_TRIB_NAME_TEXT]
--      ,[CAMPAIGN_CODE]
--      ,[IS_FR_ITEM]
--      ,[ENCRYPT_CSC]
--      ,[ISSUE_DATE]
--      ,[ISSUE_NUMBER]
--      ,[GL_EXPORT_DATE]
--      ,[FR_CHECKBOX]
--      ,[GATEWAY_REF]
--      ,[TAX_AUTHORITY]
--      ,[TAX_RATE]
--      ,[TAX_1]
--      ,[PRICE_ADJ]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]
--	  )
--VALUES (
--SRC.[TRANS_NUMBER]
--      ,SRC.[LINE_NUMBER]
--      ,SRC.[BATCH_NUM]
--      ,SRC.[OWNER_ORG_CODE]
--      ,SRC.[SOURCE_SYSTEM]
--      ,SRC.[JOURNAL_TYPE]
--      ,SRC.[TRANSACTION_TYPE]
--      ,SRC.[TRANSACTION_DATE]
--      ,SRC.[BT_ID]
--      ,SRC.[ST_ID]
--      ,SRC.[INVOICE_REFERENCE_NUM]
--      ,SRC.[DESCRIPTION]
--      ,SRC.[CUSTOMER_NAME]
--      ,SRC.[CUSTOMER_REFERENCE]
--      ,SRC.[REFERENCE_1]
--      ,SRC.[SOURCE_CODE]
--      ,SRC.[PRODUCT_CODE]
--      ,SRC.[EFFECTIVE_DATE]
--      ,SRC.[PAID_THRU]
--      ,SRC.[MONTHS_PAID]
--      ,SRC.[FISCAL_PERIOD]
--      ,SRC.[DEFERRAL_MONTHS]
--      ,SRC.[AMOUNT]
--      ,SRC.[ADJUSTMENT_AMOUNT]
--      ,SRC.[PSEUDO_ACCOUNT]
--      ,SRC.[GL_ACCT_ORG_CODE]
--      ,SRC.[GL_ACCOUNT]
--      ,SRC.[DEFERRED_GL_ACCOUNT]
--      ,SRC.[INVOICE_CHARGES]
--      ,SRC.[INVOICE_CREDITS]
--      ,SRC.[QUANTITY]
--      ,SRC.[UNIT_PRICE]
--      ,SRC.[PAYMENT_TYPE]
--      ,SRC.[CHECK_NUMBER]
--      ,SRC.[CC_NUMBER]
--      ,SRC.[CC_EXPIRE]
--      ,SRC.[CC_AUTHORIZE]
--      ,SRC.[CC_NAME]
--      ,SRC.[TERMS_CODE]
--      ,SRC.[ACTIVITY_SEQN]
--      ,SRC.[POSTED]
--      ,SRC.[PROD_TYPE]
--      ,SRC.[ACTIVITY_TYPE]
--      ,SRC.[ACTION_CODES]
--      ,SRC.[TICKLER_DATE]
--      ,SRC.[DATE_ENTERED]
--      ,SRC.[ENTERED_BY]
--      ,SRC.[SUB_LINE_NUMBER]
--      ,SRC.[INSTALL_BILL_DATE]
--      ,SRC.[TAXABLE_VALUE]
--      ,SRC.[SOLICITOR_ID]
--      ,SRC.[INVOICE_ADJUSTMENTS]
--      ,SRC.[INVOICE_LINE_NUM]
--      ,SRC.[MERGE_CODE]
--      ,SRC.[SALUTATION_CODE]
--      ,SRC.[SENDER_CODE]
--      ,SRC.[IS_MATCH_GIFT]
--      ,SRC.[MATCH_GIFT_TRANS_NUM]
--      ,SRC.[MATCH_ACTIVITY_SEQN]
--      ,SRC.[MEM_TRIB_ID]
--      ,SRC.[RECEIPT_ID]
--      ,SRC.[DO_NOT_RECEIPT]
--      ,SRC.[CC_STATUS]
--      ,SRC.[ENCRYPT_CC_NUMBER]
--      ,SRC.[ENCRYPT_CC_EXPIRE]
--      ,SRC.[FR_ACTIVITY]
--      ,SRC.[FR_ACTIVITY_SEQN]
--      ,SRC.[MEM_TRIB_NAME_TEXT]
--      ,SRC.[CAMPAIGN_CODE]
--      ,SRC.[IS_FR_ITEM]
--      ,SRC.[ENCRYPT_CSC]
--      ,SRC.[ISSUE_DATE]
--      ,SRC.[ISSUE_NUMBER]
--      ,SRC.[GL_EXPORT_DATE]
--      ,SRC.[FR_CHECKBOX]
--      ,SRC.[GATEWAY_REF]
--      ,SRC.[TAX_AUTHORITY]
--      ,SRC.[TAX_RATE]
--      ,SRC.[TAX_1]
--      ,SRC.[PRICE_ADJ]
--      ,SRC.[TIME_STAMP]
--      ,1
--      ,@Today
--	  );

--	  	  INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
--VALUES (
--       '@PipelineName'
--	   ,'tmp.imis_Trans'
--       ,'stg.imis_Trans'
--       ,(
--                     SELECT count(*) AS action_count
--                     FROM tmp.imis_Trans
--        )
--       ,(
--             0
--					 )
--		 ,(SELECT count(*) AS action_count
--                     FROM tmp.imis_Trans
--				) 
--       ,getdate()
--       )



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
