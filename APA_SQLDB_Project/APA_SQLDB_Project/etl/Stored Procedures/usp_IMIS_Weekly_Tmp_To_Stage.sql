


/*******************************************************************************************************************************
 Description:   This Stored Procedure merges data from the temp tables into the staging tables on a weekly basis. These tables 
				have been identified to be incrementally merged on a weekly basis. 
 				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE procedure [etl].[usp_IMIS_Weekly_Tmp_To_Stage]
@PipelineName varchar(60) = 'ssms'
as



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




/********************************************************ACAD_Program_Details********************************************************************/
---add new ACAD program records to the ACAD Program Details Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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
---add new advocacy records to the Advocacy Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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


/**********************************************CUSTOM_EVENT_REGISTRATION*********************************************/
---add new event registration records to the Custom Event Registration Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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
---add new school accreditation records to the Custom School Accredited Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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



/***********************************************************************REALMAGNET************************************************************/
---add new real magnet records to the Real Magnet Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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



/*************************************************************************VERIFICATION***********************************************************/
---add new verification records to the Verification Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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



/*******************************************************UD_TABLE***********************************************/
---add new UD table records to the UD Table Staging Table


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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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




/**************************************************Member_Types**************************************/
---add new member types records to the Member Types Staging Table

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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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






/*************************************************Meet_Master***********************************/
-----add new meet master records to the Meet Master Staging Table

--MERGE stg.imis_Meet_Master AS DST
--USING tmp.imis_Meet_Master AS SRC
--       ON DST.meeting = SRC.meeting
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
--                     OR ISNULL(DST.MEETING_TYPE, '') <> ISNULL(SRC.MEETING_TYPE, '')
--                     OR ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
--                     OR ISNULL(DST.BEGIN_DATE, '') <> ISNULL(SRC.BEGIN_DATE, '')
--                     OR ISNULL(DST.END_DATE, '') <> ISNULL(SRC.END_DATE, '')
--                     OR ISNULL(DST.[STATUS], '') <> ISNULL(SRC.[STATUS], '')
--                     OR ISNULL(DST.ADDRESS_1, '') <> ISNULL(SRC.ADDRESS_1, '')
--                     OR ISNULL(DST.ADDRESS_2, '') <> ISNULL(SRC.ADDRESS_2, '')
--                     OR ISNULL(DST.ADDRESS_3, '') <> ISNULL(SRC.ADDRESS_3, '')
--					 OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
--					 OR ISNULL(DST.STATE_PROVINCE, '') <> ISNULL(SRC.STATE_PROVINCE, '')
--					 OR ISNULL(DST.ZIP, '') <> ISNULL(SRC.ZIP, '')
--					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
--					 OR ISNULL(DST.DIRECTIONS, '') <> ISNULL(SRC.DIRECTIONS, '')
--					 OR ISNULL(DST.COORDINATORS, '') <> ISNULL(SRC.COORDINATORS, '')
--					 OR ISNULL(DST.NOTES, '') <> ISNULL(SRC.NOTES, '')
--					 OR ISNULL(DST.ALLOW_REG_STRING, '') <> ISNULL(SRC.ALLOW_REG_STRING, '')
--					 OR ISNULL(DST.EARLY_CUTOFF, '') <> ISNULL(SRC.EARLY_CUTOFF, '')
--					 OR ISNULL(DST.REG_CUTOFF, '') <> ISNULL(SRC.REG_CUTOFF, '')
--					 OR ISNULL(DST.LATE_CUTOFF, '') <> ISNULL(SRC.LATE_CUTOFF, '')
--					 OR ISNULL(DST.ORG_CODE, '') <> ISNULL(SRC.ORG_CODE, '')
--					-- OR ISNULL(DST.LOGO, '') <> ISNULL(SRC.LOGO, '')
--					 OR ISNULL(DST.MAX_REGISTRANTS, '') <> ISNULL(SRC.MAX_REGISTRANTS, '')
--					 OR ISNULL(DST.TOTAL_REGISTRANTS, '') <> ISNULL(SRC.TOTAL_REGISTRANTS, '')
--					 OR ISNULL(DST.TOTAL_CANCELATIONS, '') <> ISNULL(SRC.TOTAL_CANCELATIONS, '')
--					 OR ISNULL(DST.TOTAL_REVENUE, 0) <> ISNULL(SRC.TOTAL_REVENUE, 0)
--					 OR ISNULL(DST.HEAD_COUNT, '') <> ISNULL(SRC.HEAD_COUNT, '')
--					 OR ISNULL(DST.TAX_AUTHORITY_1, '') <> ISNULL(SRC.TAX_AUTHORITY_1, '')
--					 OR ISNULL(DST.[SUPPRESS_COOR], '') <> ISNULL(SRC.[SUPPRESS_COOR], '')
--					 OR ISNULL(DST.SUPPRESS_DIR, '') <> ISNULL(SRC.SUPPRESS_DIR, '')
--					 OR ISNULL(DST.SUPPRESS_NOTES, '') <> ISNULL(SRC.SUPPRESS_NOTES, '')
--					 OR ISNULL(DST.MUF_1, '') <> ISNULL(SRC.MUF_1, '')
--					 OR ISNULL(DST.MUF_2, '') <> ISNULL(SRC.MUF_2, '')
--					 OR ISNULL(DST.MUF_3, '') <> ISNULL(SRC.MUF_3, '')
--					 OR ISNULL(DST.MUF_4, '') <> ISNULL(SRC.MUF_4, '')
--					 OR ISNULL(DST.MUF_5, '') <> ISNULL(SRC.MUF_5, '')
--					 OR ISNULL(DST.MUF_6, '') <> ISNULL(SRC.MUF_6, '')
--					 OR ISNULL(DST.MUF_7, '') <> ISNULL(SRC.MUF_7, '')
--					 OR ISNULL(DST.MUF_8, '') <> ISNULL(SRC.MUF_8, '')
--					 OR ISNULL(DST.MUF_9, '') <> ISNULL(SRC.MUF_9, '')
--					 OR ISNULL(DST.MUF_10, '') <> ISNULL(SRC.MUF_10, '')
--					 OR ISNULL(DST.INTENT_TO_EDIT, '') <> ISNULL(SRC.INTENT_TO_EDIT, '')
--					 OR ISNULL(DST.SUPPRESS_CONFIRM, '') <> ISNULL(SRC.SUPPRESS_CONFIRM, '')
--					 OR ISNULL(DST.WEB_VIEW_ONLY, '') <> ISNULL(SRC.WEB_VIEW_ONLY, '')
--					 OR ISNULL(DST.WEB_ENABLED, '') <> ISNULL(SRC.WEB_ENABLED, '')
--					 OR ISNULL(DST.POST_REGISTRATION, '') <> ISNULL(SRC.POST_REGISTRATION, '')
--					 OR ISNULL(DST.EMAIL_REGISTRATION, '') <> ISNULL(SRC.EMAIL_REGISTRATION, '')
--					 OR ISNULL(DST.MEETING_URL, '') <> ISNULL(SRC.MEETING_URL, '')
--					 OR ISNULL(DST.MEETING_IMAGE_NAME, '') <> ISNULL(SRC.MEETING_IMAGE_NAME, '')
--					 OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
--					 OR ISNULL(DST.IS_FR_MEET, '') <> ISNULL(SRC.IS_FR_MEET, '')
--					 OR ISNULL(DST.MEET_APPEAL, '') <> ISNULL(SRC.MEET_APPEAL, '')
--					 OR ISNULL(DST.MEET_CAMPAIGN, '') <> ISNULL(SRC.MEET_CAMPAIGN, '')
--					 OR ISNULL(DST.MEET_CATEGORY, '') <> ISNULL(SRC.MEET_CATEGORY, '')
--					 OR ISNULL(DST.COMP_REG_REG_CLASS, '') <> ISNULL(SRC.COMP_REG_REG_CLASS, '')
--					 OR ISNULL(DST.COMP_REG_CALCULATION, '') <> ISNULL(SRC.COMP_REG_CALCULATION, '')
--					 OR ISNULL(DST.SQUARE_FOOT_RULES, '') <> ISNULL(SRC.SQUARE_FOOT_RULES, '')
--					 OR ISNULL(DST.TAX_BY_ADDRESS, '') <> ISNULL(SRC.TAX_BY_ADDRESS, '')
--					 OR ISNULL(DST.VAT_RULESET, '') <> ISNULL(SRC.VAT_RULESET, '')
--					 OR ISNULL(DST.REG_CLASS_STORED_PROC, '') <> ISNULL(SRC.REG_CLASS_STORED_PROC, '')
--					 OR ISNULL(DST.WEB_REG_CLASS_METHOD, '') <> ISNULL(SRC.WEB_REG_CLASS_METHOD, '')
--					 OR ISNULL(DST.REG_OTHERS, '') <> ISNULL(SRC.REG_OTHERS, '')
--					 OR ISNULL(DST.ADD_GUESTS, '') <> ISNULL(SRC.ADD_GUESTS, '')
--					 OR ISNULL(DST.WEB_DESC, '') <> ISNULL(SRC.WEB_DESC, '')
--					 OR ISNULL(DST.ALLOW_REG_EDIT, '') <> ISNULL(SRC.ALLOW_REG_EDIT, '')
--					 OR ISNULL(DST.REG_EDIT_CUTOFF, '') <> ISNULL(SRC.REG_EDIT_CUTOFF, '')
--					 OR ISNULL(DST.FORM_DEFINITION_ID, '') <> ISNULL(SRC.FORM_DEFINITION_ID, '')
--					 OR ISNULL(DST.FORM_DEFINITION_SECTION_ID, '') <> ISNULL(SRC.FORM_DEFINITION_SECTION_ID, '')
--					 OR ISNULL(DST.PUBLISH_START_DATE, '') <> ISNULL(SRC.PUBLISH_START_DATE, '')
--					 OR ISNULL(DST.PUBLISH_END_DATE, '') <> ISNULL(SRC.PUBLISH_END_DATE, '')
--					 OR ISNULL(DST.REGISTRATION_START_DATE, '') <> ISNULL(SRC.REGISTRATION_START_DATE, '')
--					 OR ISNULL(DST.REGISTRATION_END_DATE, '') <> ISNULL(SRC.REGISTRATION_END_DATE, '')
--					 OR ISNULL(DST.REGISTRATION_CLOSED_MESSAGE, '') <> ISNULL(SRC.REGISTRATION_CLOSED_MESSAGE, '')
--					 OR ISNULL(DST.DEFAULT_PROGRAMITEM_DISPLAYMODE, '') <> ISNULL(SRC.DEFAULT_PROGRAMITEM_DISPLAYMODE, '')
--					 OR ISNULL(DST.TEMPLATE_STATE_CODE, '') <> ISNULL(SRC.TEMPLATE_STATE_CODE, '')
--					 OR ISNULL(DST.ENABLE_TIME_CONFLICTS, '') <> ISNULL(SRC.ENABLE_TIME_CONFLICTS, '')
--					 OR ISNULL(DST.ALLOW_REGISTRANT_CONFLICTS, '') <> ISNULL(SRC.ALLOW_REGISTRANT_CONFLICTS, '')
                     
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                    [MEETING]
--      ,[TITLE]
--      ,[MEETING_TYPE]
--      ,[DESCRIPTION]
--      ,[BEGIN_DATE]
--      ,[END_DATE]
--      ,[STATUS]
--      ,[ADDRESS_1]
--      ,[ADDRESS_2]
--      ,[ADDRESS_3]
--      ,[CITY]
--      ,[STATE_PROVINCE]
--      ,[ZIP]
--      ,[COUNTRY]
--      ,[DIRECTIONS]
--      ,[COORDINATORS]
--      ,[NOTES]
--      ,[ALLOW_REG_STRING]
--      ,[EARLY_CUTOFF]
--      ,[REG_CUTOFF]
--      ,[LATE_CUTOFF]
--      ,[ORG_CODE]
--      ,[LOGO]
--      ,[MAX_REGISTRANTS]
--      ,[TOTAL_REGISTRANTS]
--      ,[TOTAL_CANCELATIONS]
--      ,[TOTAL_REVENUE]
--      ,[HEAD_COUNT]
--      ,[TAX_AUTHORITY_1]
--      ,[SUPPRESS_COOR]
--      ,[SUPPRESS_DIR]
--      ,[SUPPRESS_NOTES]
--      ,[MUF_1]
--      ,[MUF_2]
--      ,[MUF_3]
--      ,[MUF_4]
--      ,[MUF_5]
--      ,[MUF_6]
--      ,[MUF_7]
--      ,[MUF_8]
--      ,[MUF_9]
--      ,[MUF_10]
--      ,[INTENT_TO_EDIT]
--      ,[SUPPRESS_CONFIRM]
--      ,[WEB_VIEW_ONLY]
--      ,[WEB_ENABLED]
--      ,[POST_REGISTRATION]
--      ,[EMAIL_REGISTRATION]
--      ,[MEETING_URL]
--      ,[MEETING_IMAGE_NAME]
--      ,[CONTACT_ID]
--      ,[IS_FR_MEET]
--      ,[MEET_APPEAL]
--      ,[MEET_CAMPAIGN]
--      ,[MEET_CATEGORY]
--      ,[COMP_REG_REG_CLASS]
--      ,[COMP_REG_CALCULATION]
--      ,[SQUARE_FOOT_RULES]
--      ,[TAX_BY_ADDRESS]
--      ,[VAT_RULESET]
--      ,[REG_CLASS_STORED_PROC]
--      ,[WEB_REG_CLASS_METHOD]
--      ,[REG_OTHERS]
--      ,[ADD_GUESTS]
--      ,[WEB_DESC]
--      ,[ALLOW_REG_EDIT]
--      ,[REG_EDIT_CUTOFF]
--      ,[FORM_DEFINITION_ID]
--      ,[FORM_DEFINITION_SECTION_ID]
--      ,[PUBLISH_START_DATE]
--      ,[PUBLISH_END_DATE]
--      ,[REGISTRATION_START_DATE]
--      ,[REGISTRATION_END_DATE]
--      ,[REGISTRATION_CLOSED_MESSAGE]
--      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
--      ,[TEMPLATE_STATE_CODE]
--      ,[ENABLE_TIME_CONFLICTS]
--      ,[ALLOW_REGISTRANT_CONFLICTS]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]

          
--                     )
--              VALUES (
--                    src.[MEETING]
--      ,src.[TITLE]
--      ,src.[MEETING_TYPE]
--      ,src.[DESCRIPTION]
--      ,src.[BEGIN_DATE]
--      ,src.[END_DATE]
--      ,src.[STATUS]
--      ,src.[ADDRESS_1]
--      ,src.[ADDRESS_2]
--      ,src.[ADDRESS_3]
--      ,src.[CITY]
--      ,src.[STATE_PROVINCE]
--      ,src.[ZIP]
--      ,src.[COUNTRY]
--      ,src.[DIRECTIONS]
--      ,src.[COORDINATORS]
--      ,src.[NOTES]
--      ,src.[ALLOW_REG_STRING]
--      ,src.[EARLY_CUTOFF]
--      ,src.[REG_CUTOFF]
--      ,src.[LATE_CUTOFF]
--      ,src.[ORG_CODE]
--      ,src.[LOGO]
--      ,src.[MAX_REGISTRANTS]
--      ,src.[TOTAL_REGISTRANTS]
--      ,src.[TOTAL_CANCELATIONS]
--      ,src.[TOTAL_REVENUE]
--      ,src.[HEAD_COUNT]
--      ,src.[TAX_AUTHORITY_1]
--      ,src.[SUPPRESS_COOR]
--      ,src.[SUPPRESS_DIR]
--      ,src.[SUPPRESS_NOTES]
--      ,src.[MUF_1]
--      ,src.[MUF_2]
--      ,src.[MUF_3]
--      ,src.[MUF_4]
--      ,src.[MUF_5]
--      ,src.[MUF_6]
--      ,src.[MUF_7]
--      ,src.[MUF_8]
--      ,src.[MUF_9]
--      ,src.[MUF_10]
--      ,src.[INTENT_TO_EDIT]
--      ,src.[SUPPRESS_CONFIRM]
--      ,src.[WEB_VIEW_ONLY]
--      ,src.[WEB_ENABLED]
--      ,src.[POST_REGISTRATION]
--      ,src.[EMAIL_REGISTRATION]
--      ,src.[MEETING_URL]
--      ,src.[MEETING_IMAGE_NAME]
--      ,src.[CONTACT_ID]
--      ,src.[IS_FR_MEET]
--      ,src.[MEET_APPEAL]
--      ,src.[MEET_CAMPAIGN]
--      ,src.[MEET_CATEGORY]
--      ,src.[COMP_REG_REG_CLASS]
--      ,src.[COMP_REG_CALCULATION]
--      ,src.[SQUARE_FOOT_RULES]
--      ,src.[TAX_BY_ADDRESS]
--      ,src.[VAT_RULESET]
--      ,src.[REG_CLASS_STORED_PROC]
--      ,src.[WEB_REG_CLASS_METHOD]
--      ,src.[REG_OTHERS]
--      ,src.[ADD_GUESTS]
--      ,src.[WEB_DESC]
--      ,src.[ALLOW_REG_EDIT]
--      ,src.[REG_EDIT_CUTOFF]
--      ,src.[FORM_DEFINITION_ID]
--      ,src.[FORM_DEFINITION_SECTION_ID]
--      ,src.[PUBLISH_START_DATE]
--      ,src.[PUBLISH_END_DATE]
--      ,src.[REGISTRATION_START_DATE]
--      ,src.[REGISTRATION_END_DATE]
--      ,src.[REGISTRATION_CLOSED_MESSAGE]
--      ,src.[DEFAULT_PROGRAMITEM_DISPLAYMODE]
--      ,src.[TEMPLATE_STATE_CODE]
--      ,src.[ENABLE_TIME_CONFLICTS]
--      ,src.[ALLOW_REGISTRANT_CONFLICTS]
--      ,src.[TIME_STAMP]
--                 ,1
--                 ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.meeting
--       ,deleted.meeting
--INTO #audittemp;



--INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
--VALUES (
--		'@PipelineName'
--	   ,'tmp.imis_Meet_Master'
--       ,'stg.imis_Meet_Master'
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
--				from tmp.imis_Meet_Master
--				)
--       ,getdate()
--       )

--INSERT INTO stg.imis_Meet_Master (
--[MEETING]
--      ,[TITLE]
--      ,[MEETING_TYPE]
--      ,[DESCRIPTION]
--      ,[BEGIN_DATE]
--      ,[END_DATE]
--      ,[STATUS]
--      ,[ADDRESS_1]
--      ,[ADDRESS_2]
--      ,[ADDRESS_3]
--      ,[CITY]
--      ,[STATE_PROVINCE]
--      ,[ZIP]
--      ,[COUNTRY]
--      ,[DIRECTIONS]
--      ,[COORDINATORS]
--      ,[NOTES]
--      ,[ALLOW_REG_STRING]
--      ,[EARLY_CUTOFF]
--      ,[REG_CUTOFF]
--      ,[LATE_CUTOFF]
--      ,[ORG_CODE]
--      ,[LOGO]
--      ,[MAX_REGISTRANTS]
--      ,[TOTAL_REGISTRANTS]
--      ,[TOTAL_CANCELATIONS]
--      ,[TOTAL_REVENUE]
--      ,[HEAD_COUNT]
--      ,[TAX_AUTHORITY_1]
--      ,[SUPPRESS_COOR]
--      ,[SUPPRESS_DIR]
--      ,[SUPPRESS_NOTES]
--      ,[MUF_1]
--      ,[MUF_2]
--      ,[MUF_3]
--      ,[MUF_4]
--      ,[MUF_5]
--      ,[MUF_6]
--      ,[MUF_7]
--      ,[MUF_8]
--      ,[MUF_9]
--      ,[MUF_10]
--      ,[INTENT_TO_EDIT]
--      ,[SUPPRESS_CONFIRM]
--      ,[WEB_VIEW_ONLY]
--      ,[WEB_ENABLED]
--      ,[POST_REGISTRATION]
--      ,[EMAIL_REGISTRATION]
--      ,[MEETING_URL]
--      ,[MEETING_IMAGE_NAME]
--      ,[CONTACT_ID]
--      ,[IS_FR_MEET]
--      ,[MEET_APPEAL]
--      ,[MEET_CAMPAIGN]
--      ,[MEET_CATEGORY]
--      ,[COMP_REG_REG_CLASS]
--      ,[COMP_REG_CALCULATION]
--      ,[SQUARE_FOOT_RULES]
--      ,[TAX_BY_ADDRESS]
--      ,[VAT_RULESET]
--      ,[REG_CLASS_STORED_PROC]
--      ,[WEB_REG_CLASS_METHOD]
--      ,[REG_OTHERS]
--      ,[ADD_GUESTS]
--      ,[WEB_DESC]
--      ,[ALLOW_REG_EDIT]
--      ,[REG_EDIT_CUTOFF]
--      ,[FORM_DEFINITION_ID]
--      ,[FORM_DEFINITION_SECTION_ID]
--      ,[PUBLISH_START_DATE]
--      ,[PUBLISH_END_DATE]
--      ,[REGISTRATION_START_DATE]
--      ,[REGISTRATION_END_DATE]
--      ,[REGISTRATION_CLOSED_MESSAGE]
--      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
--      ,[TEMPLATE_STATE_CODE]
--      ,[ENABLE_TIME_CONFLICTS]
--      ,[ALLOW_REGISTRANT_CONFLICTS]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]

--                 )
--select  
--                     [MEETING]
--      ,[TITLE]
--      ,[MEETING_TYPE]
--      ,[DESCRIPTION]
--      ,[BEGIN_DATE]
--      ,[END_DATE]
--      ,[STATUS]
--      ,[ADDRESS_1]
--      ,[ADDRESS_2]
--      ,[ADDRESS_3]
--      ,[CITY]
--      ,[STATE_PROVINCE]
--      ,[ZIP]
--      ,[COUNTRY]
--      ,[DIRECTIONS]
--      ,[COORDINATORS]
--      ,[NOTES]
--      ,[ALLOW_REG_STRING]
--      ,[EARLY_CUTOFF]
--      ,[REG_CUTOFF]
--      ,[LATE_CUTOFF]
--      ,[ORG_CODE]
--      ,[LOGO]
--      ,[MAX_REGISTRANTS]
--      ,[TOTAL_REGISTRANTS]
--      ,[TOTAL_CANCELATIONS]
--      ,[TOTAL_REVENUE]
--      ,[HEAD_COUNT]
--      ,[TAX_AUTHORITY_1]
--      ,[SUPPRESS_COOR]
--      ,[SUPPRESS_DIR]
--      ,[SUPPRESS_NOTES]
--      ,[MUF_1]
--      ,[MUF_2]
--      ,[MUF_3]
--      ,[MUF_4]
--      ,[MUF_5]
--      ,[MUF_6]
--      ,[MUF_7]
--      ,[MUF_8]
--      ,[MUF_9]
--      ,[MUF_10]
--      ,[INTENT_TO_EDIT]
--      ,[SUPPRESS_CONFIRM]
--      ,[WEB_VIEW_ONLY]
--      ,[WEB_ENABLED]
--      ,[POST_REGISTRATION]
--      ,[EMAIL_REGISTRATION]
--      ,[MEETING_URL]
--      ,[MEETING_IMAGE_NAME]
--      ,[CONTACT_ID]
--      ,[IS_FR_MEET]
--      ,[MEET_APPEAL]
--      ,[MEET_CAMPAIGN]
--      ,[MEET_CATEGORY]
--      ,[COMP_REG_REG_CLASS]
--      ,[COMP_REG_CALCULATION]
--      ,[SQUARE_FOOT_RULES]
--      ,[TAX_BY_ADDRESS]
--      ,[VAT_RULESET]
--      ,[REG_CLASS_STORED_PROC]
--      ,[WEB_REG_CLASS_METHOD]
--      ,[REG_OTHERS]
--      ,[ADD_GUESTS]
--      ,[WEB_DESC]
--      ,[ALLOW_REG_EDIT]
--      ,[REG_EDIT_CUTOFF]
--      ,[FORM_DEFINITION_ID]
--      ,[FORM_DEFINITION_SECTION_ID]
--      ,[PUBLISH_START_DATE]
--      ,[PUBLISH_END_DATE]
--      ,[REGISTRATION_START_DATE]
--      ,[REGISTRATION_END_DATE]
--      ,[REGISTRATION_CLOSED_MESSAGE]
--      ,[DEFAULT_PROGRAMITEM_DISPLAYMODE]
--      ,[TEMPLATE_STATE_CODE]
--      ,[ENABLE_TIME_CONFLICTS]
--      ,[ALLOW_REGISTRANT_CONFLICTS]
--           ,cast(TIME_STAMP as bigint)
--                 ,1
--                 ,@Today
           
         
--FROM 
--tmp.imis_Meet_Master
--WHERE meeting IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp




/**************************************************Product_Function**************************************/
---add new product function records to the Product Function Staging Table

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



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
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
---add new address geocode records to the Custom Address Geocode Staging Table

--MERGE stg.imis_Custom_Address_Geocode AS DST
--USING tmp.imis_Custom_Address_Geocode AS SRC
--       ON DST.ID = SRC.ID and
--	   DST.SEQN = SRC.SEQN
--WHEN MATCHED
--              AND IsActive = 1
--              AND (
                   
--                     ISNULL(DST.ADDRESS_NUM, '') <> ISNULL(SRC.ADDRESS_NUM, '')
--                     OR ISNULL(DST.VOTERVOICE_CHECKSUM, '') <> ISNULL(SRC.VOTERVOICE_CHECKSUM, '')
--                     OR ISNULL(DST.LONGITUDE, '') <> ISNULL(SRC.LONGITUDE, '')
--                     OR ISNULL(DST.LATITUDE, '') <> ISNULL(SRC.LATITUDE, '')
--                     OR ISNULL(DST.WEAK_COORDINATES, '') <> ISNULL(SRC.WEAK_COORDINATES, '')
--                     OR ISNULL(DST.US_CONGRESS, '') <> ISNULL(SRC.US_CONGRESS, '')
--                     OR ISNULL(DST.STATE_SENATE, '') <> ISNULL(SRC.STATE_SENATE, '')
--                     OR ISNULL(DST.STATE_HOUSE, '') <> ISNULL(SRC.STATE_HOUSE, '')
--                     OR ISNULL(DST.CHANGED, '') <> ISNULL(SRC.CHANGED, '')
--                     OR ISNULL(DST.LAST_UPDATED, '') <> ISNULL(SRC.LAST_UPDATED, '')
--                     )
--              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
--              THEN
--                     UPDATE
--                     SET DST.isActive = 0
--                           ,DST.EndDate = @Yesterday
--WHEN NOT MATCHED
--       THEN
--              INSERT (
--                     [ID]
--      ,[SEQN]
--      ,[ADDRESS_NUM]
--      ,[VOTERVOICE_CHECKSUM]
--      ,[LONGITUDE]
--      ,[LATITUDE]
--      ,[WEAK_COORDINATES]
--      ,[US_CONGRESS]
--      ,[STATE_SENATE]
--      ,[STATE_HOUSE]
--      ,[CHANGED]
--      ,[LAST_UPDATED]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]
          
--                     )
--              VALUES (
--                     SRC.[ID]
--      ,SRC.[SEQN]
--      ,SRC.[ADDRESS_NUM]
--      ,SRC.[VOTERVOICE_CHECKSUM]
--      ,SRC.[LONGITUDE]
--      ,SRC.[LATITUDE]
--      ,SRC.[WEAK_COORDINATES]
--      ,SRC.[US_CONGRESS]
--      ,SRC.[STATE_SENATE]
--      ,SRC.[STATE_HOUSE]
--      ,SRC.[CHANGED]
--      ,SRC.[LAST_UPDATED]
--      ,SRC.[TIME_STAMP]
--                 ,1
--                 ,@Today
--                     ) 

--OUTPUT $ACTION AS action
--       ,inserted.ID
--       ,deleted.ID
--INTO #audittemp;



--INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
--VALUES (
--        '@PipelineName'
--	   ,'tmp.imis_Custom_Address_Geocode'
--       ,'stg.imis_Custom_Address_Geocode'
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
--				from tmp.imis_Custom_Address_Geocode
--				) 
--       ,getdate()
--       )

--INSERT INTO stg.imis_Custom_Address_Geocode (
--[ID]
--      ,[SEQN]
--      ,[ADDRESS_NUM]
--      ,[VOTERVOICE_CHECKSUM]
--      ,[LONGITUDE]
--      ,[LATITUDE]
--      ,[WEAK_COORDINATES]
--      ,[US_CONGRESS]
--      ,[STATE_SENATE]
--      ,[STATE_HOUSE]
--      ,[CHANGED]
--      ,[LAST_UPDATED]
--      ,[TIME_STAMP]
--      ,[IsActive]
--      ,[StartDate]
--                 )
--select  
--                    [ID]
--      ,[SEQN]
--      ,[ADDRESS_NUM]
--      ,[VOTERVOICE_CHECKSUM]
--      ,[LONGITUDE]
--      ,[LATITUDE]
--      ,[WEAK_COORDINATES]
--      ,[US_CONGRESS]
--      ,[STATE_SENATE]
--      ,[STATE_HOUSE]
--      ,[CHANGED]
--      ,[LAST_UPDATED]
--           ,cast(TIME_STAMP as bigint)
--                 ,1
--                 ,@Today
           
         
--FROM 
--tmp.imis_Custom_Address_Geocode
--WHERE ID IN (
--              SELECT inserted_ID
--              FROM #audittemp
--              WHERE action = 'UPDATE'
--              )


--			  Truncate TABLE #audittemp



/***********************************product_type**************/
---add new product type records to the Product Type Staging Table

MERGE stg.imis_Product_Type AS DST
USING tmp.imis_Product_Type AS SRC
       ON DST.prod_type = SRC.prod_type 
WHEN MATCHED
              AND IsActive = 1
              AND (
                   
                     ISNULL(DST.[DESCRIPTION], '') <> ISNULL(SRC.[DESCRIPTION], '')
                     OR ISNULL(DST.SALES_ITEM, '') <> ISNULL(SRC.SALES_ITEM, '')
                     OR ISNULL(DST.DUES_SUB_ITEM, '') <> ISNULL(SRC.DUES_SUB_ITEM, '')
                     OR ISNULL(DST.PROFILE_SORT, '') <> ISNULL(SRC.PROFILE_SORT, '')
                     OR ISNULL(DST.SUBTYPE_PROMPT, '') <> ISNULL(SRC.SUBTYPE_PROMPT, '')
                     OR ISNULL(DST.DESCRIPTION_PROMPT, '') <> ISNULL(SRC.DESCRIPTION_PROMPT, '')
                     OR ISNULL(DST.EFF_DATE_PROMPT, '') <> ISNULL(SRC.EFF_DATE_PROMPT, '')
                     OR ISNULL(DST.THRU_DATE_PROMPT, '') <> ISNULL(SRC.THRU_DATE_PROMPT, '')
                     OR ISNULL(DST.SOURCE_PROMPT, '') <> ISNULL(SRC.SOURCE_PROMPT, '')
					 OR ISNULL(DST.AMOUNT_PROMPT, '') <> ISNULL(SRC.AMOUNT_PROMPT, '')
					 OR ISNULL(DST.TICKLER_PROMPT, '') <> ISNULL(SRC.TICKLER_PROMPT, '')
					 OR ISNULL(DST.ACTION_PROMPT, '') <> ISNULL(SRC.ACTION_PROMPT, '')
					 OR ISNULL(DST.CEU_TYPE_PROMPT, '') <> ISNULL(SRC.CEU_TYPE_PROMPT, '')
					 OR ISNULL(DST.UNITS_PROMPT, '') <> ISNULL(SRC.UNITS_PROMPT, '')
					 OR ISNULL(DST.USER_EDIT, '') <> ISNULL(SRC.USER_EDIT, '')
					 OR ISNULL(DST.RETAIN_MONTHS, '') <> ISNULL(SRC.RETAIN_MONTHS, '')
					 OR ISNULL(DST.FOLLOW_UP_PROMPT, '') <> ISNULL(SRC.FOLLOW_UP_PROMPT, '')
					 OR ISNULL(DST.NOTE_PROMPT, '') <> ISNULL(SRC.NOTE_PROMPT, '')
					 OR ISNULL(DST.EXTENDED_DEMO, '') <> ISNULL(SRC.EXTENDED_DEMO, '')
					 OR ISNULL(DST.UF_1_PROMPT, '') <> ISNULL(SRC.UF_1_PROMPT, '')
					 OR ISNULL(DST.UF_2_PROMPT, '') <> ISNULL(SRC.UF_2_PROMPT, '')
					 OR ISNULL(DST.UF_3_PROMPT, '') <> ISNULL(SRC.UF_3_PROMPT, '')
					 OR ISNULL(DST.UF_4_PROMPT, '') <> ISNULL(SRC.UF_4_PROMPT, '')
					 OR ISNULL(DST.UF_5_PROMPT, '') <> ISNULL(SRC.UF_5_PROMPT, '')
					 OR ISNULL(DST.UF_6_PROMPT, '') <> ISNULL(SRC.UF_6_PROMPT, '')
					 OR ISNULL(DST.UF_7_PROMPT, '') <> ISNULL(SRC.UF_7_PROMPT, '')
					 OR ISNULL(DST.CO_ID_PROMPT, '') <> ISNULL(SRC.CO_ID_PROMPT, '')
					 OR ISNULL(DST.OTHER_CODE_PROMPT, '') <> ISNULL(SRC.OTHER_CODE_PROMPT, '')
					 OR ISNULL(DST.ACCESS_KEYWORDS, '') <> ISNULL(SRC.ACCESS_KEYWORDS, '')
					 OR ISNULL(DST.CREATE_HISTORY_TAB, '') <> ISNULL(SRC.CREATE_HISTORY_TAB, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [PROD_TYPE]
      ,[DESCRIPTION]
      ,[SALES_ITEM]
      ,[DUES_SUB_ITEM]
      ,[PROFILE_SORT]
      ,[SUBTYPE_PROMPT]
      ,[DESCRIPTION_PROMPT]
      ,[EFF_DATE_PROMPT]
      ,[THRU_DATE_PROMPT]
      ,[SOURCE_PROMPT]
      ,[AMOUNT_PROMPT]
      ,[TICKLER_PROMPT]
      ,[ACTION_PROMPT]
      ,[CEU_TYPE_PROMPT]
      ,[UNITS_PROMPT]
      ,[USER_EDIT]
      ,[RETAIN_MONTHS]
      ,[FOLLOW_UP_PROMPT]
      ,[NOTE_PROMPT]
      ,[EXTENDED_DEMO]
      ,[UF_1_PROMPT]
      ,[UF_2_PROMPT]
      ,[UF_3_PROMPT]
      ,[UF_4_PROMPT]
      ,[UF_5_PROMPT]
      ,[UF_6_PROMPT]
      ,[UF_7_PROMPT]
      ,[CO_ID_PROMPT]
      ,[OTHER_CODE_PROMPT]
      ,[ACCESS_KEYWORDS]
      ,[CREATE_HISTORY_TAB]
      ,[HISTORY_TAB_TITLE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     SRC.[PROD_TYPE]
      ,SRC.[DESCRIPTION]
      ,SRC.[SALES_ITEM]
      ,SRC.[DUES_SUB_ITEM]
      ,SRC.[PROFILE_SORT]
      ,SRC.[SUBTYPE_PROMPT]
      ,SRC.[DESCRIPTION_PROMPT]
      ,SRC.[EFF_DATE_PROMPT]
      ,SRC.[THRU_DATE_PROMPT]
      ,SRC.[SOURCE_PROMPT]
      ,SRC.[AMOUNT_PROMPT]
      ,SRC.[TICKLER_PROMPT]
      ,SRC.[ACTION_PROMPT]
      ,SRC.[CEU_TYPE_PROMPT]
      ,SRC.[UNITS_PROMPT]
      ,SRC.[USER_EDIT]
      ,SRC.[RETAIN_MONTHS]
      ,SRC.[FOLLOW_UP_PROMPT]
      ,SRC.[NOTE_PROMPT]
      ,SRC.[EXTENDED_DEMO]
      ,SRC.[UF_1_PROMPT]
      ,SRC.[UF_2_PROMPT]
      ,SRC.[UF_3_PROMPT]
      ,SRC.[UF_4_PROMPT]
      ,SRC.[UF_5_PROMPT]
      ,SRC.[UF_6_PROMPT]
      ,SRC.[UF_7_PROMPT]
      ,SRC.[CO_ID_PROMPT]
      ,SRC.[OTHER_CODE_PROMPT]
      ,SRC.[ACCESS_KEYWORDS]
      ,SRC.[CREATE_HISTORY_TAB]
      ,SRC.[HISTORY_TAB_TITLE]
      ,SRC.[TIME_STAMP]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.prod_type
       ,deleted.prod_type
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
		'@PipelineName'
	   ,'tmp.imis_Product_Type'
       ,'stg.imis_Product_Type'
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
				from tmp.imis_Product_Type
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Product_Type (
 [PROD_TYPE]
      ,[DESCRIPTION]
      ,[SALES_ITEM]
      ,[DUES_SUB_ITEM]
      ,[PROFILE_SORT]
      ,[SUBTYPE_PROMPT]
      ,[DESCRIPTION_PROMPT]
      ,[EFF_DATE_PROMPT]
      ,[THRU_DATE_PROMPT]
      ,[SOURCE_PROMPT]
      ,[AMOUNT_PROMPT]
      ,[TICKLER_PROMPT]
      ,[ACTION_PROMPT]
      ,[CEU_TYPE_PROMPT]
      ,[UNITS_PROMPT]
      ,[USER_EDIT]
      ,[RETAIN_MONTHS]
      ,[FOLLOW_UP_PROMPT]
      ,[NOTE_PROMPT]
      ,[EXTENDED_DEMO]
      ,[UF_1_PROMPT]
      ,[UF_2_PROMPT]
      ,[UF_3_PROMPT]
      ,[UF_4_PROMPT]
      ,[UF_5_PROMPT]
      ,[UF_6_PROMPT]
      ,[UF_7_PROMPT]
      ,[CO_ID_PROMPT]
      ,[OTHER_CODE_PROMPT]
      ,[ACCESS_KEYWORDS]
      ,[CREATE_HISTORY_TAB]
      ,[HISTORY_TAB_TITLE]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]               )
select  
                     [PROD_TYPE]
      ,[DESCRIPTION]
      ,[SALES_ITEM]
      ,[DUES_SUB_ITEM]
      ,[PROFILE_SORT]
      ,[SUBTYPE_PROMPT]
      ,[DESCRIPTION_PROMPT]
      ,[EFF_DATE_PROMPT]
      ,[THRU_DATE_PROMPT]
      ,[SOURCE_PROMPT]
      ,[AMOUNT_PROMPT]
      ,[TICKLER_PROMPT]
      ,[ACTION_PROMPT]
      ,[CEU_TYPE_PROMPT]
      ,[UNITS_PROMPT]
      ,[USER_EDIT]
      ,[RETAIN_MONTHS]
      ,[FOLLOW_UP_PROMPT]
      ,[NOTE_PROMPT]
      ,[EXTENDED_DEMO]
      ,[UF_1_PROMPT]
      ,[UF_2_PROMPT]
      ,[UF_3_PROMPT]
      ,[UF_4_PROMPT]
      ,[UF_5_PROMPT]
      ,[UF_6_PROMPT]
      ,[UF_7_PROMPT]
      ,[CO_ID_PROMPT]
      ,[OTHER_CODE_PROMPT]
      ,[ACCESS_KEYWORDS]
      ,[CREATE_HISTORY_TAB]
      ,[HISTORY_TAB_TITLE]
           ,cast(TIME_STAMP as bigint)
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Product_Type
WHERE prod_type IN (
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
