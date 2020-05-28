CREATE Procedure [etl].[usp_DJ_Daily_Tmp_to_Stage]
@PipelineName varchar(60) = 'ssms'
as

/*******************************************************************************************************************************
 Description:   This Stored Procedure merges data from the Django temp tables into the staging tables on a daily basis. These tables 
				have been identified to be incrementally merged on a daily basis. 
 				

Added By:		Morgan Diestler on 4/30/2020				
*******************************************************************************************************************************/


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

/****************DJ CM Claim*******************/
MERGE stg.dj_cm_claim AS DST
USING tmp.dj_cm_claim AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.VERIFIED, '') <> ISNULL(SRC.VERIFIED, '')
                     OR ISNULL(DST.CREDITS, 0) <> ISNULL(SRC.CREDITS, 0)
                     OR ISNULL(DST.LAW_CREDITS, 0) <> ISNULL(SRC.LAW_CREDITS, 0)
                     OR ISNULL(DST.ETHICS_CREDITS, 0) <> ISNULL(SRC.ETHICS_CREDITS, 0)
                     OR ISNULL(DST.IS_SPEAKER, '') <> ISNULL(SRC.IS_SPEAKER, '')
                     OR ISNULL(DST.IS_AUTHOR, '') <> ISNULL(SRC.IS_AUTHOR, '')
                     OR ISNULL(DST.SELF_REPORTED, '') <> ISNULL(SRC.SELF_REPORTED, '')
                     OR ISNULL(DST.COMMENT_ID, '') <> ISNULL(SRC.COMMENT_ID, '')
					 OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
					 OR ISNULL(DST.EVENT_ID, '') <> ISNULL(SRC.EVENT_ID, '')
					 OR ISNULL(DST.LOG_ID, '') <> ISNULL(SRC.LOG_ID, '')
					 OR ISNULL(DST.SUBMITTED_TIME, '') <> ISNULL(SRC.SUBMITTED_TIME, '')
					 OR DST.BEGIN_TIME <> try_convert(datetime,left(SRC.BEGIN_TIME,19)) 
					 OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
					 OR ISNULL(DST.DESCRIPTION, '') <> ISNULL(SRC.DESCRIPTION, '')
					 OR DST.END_TIME <> try_convert(datetime,left(SRC.END_TIME,19)) 
					 OR ISNULL(DST.PROVIDER_NAME, '') <> ISNULL(SRC.PROVIDER_NAME, '')
					 OR ISNULL(DST.STATE, '') <> ISNULL(SRC.STATE, '')
					 OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
					 OR ISNULL(DST.IS_CARRYOVER, '') <> ISNULL(SRC.IS_CARRYOVER, '')
					 OR ISNULL(DST.IS_PRO_BONO, '') <> ISNULL(SRC.IS_PRO_BONO, '')
					 OR ISNULL(DST.LEARNING_OBJECTIVES, '') <> ISNULL(SRC.LEARNING_OBJECTIVES, '')
					 OR ISNULL(DST.TIMEZONE, '') <> ISNULL(SRC.TIMEZONE, '')
                     OR ISNULL(DST.AUTHOR_TYPE, '') <> ISNULL(SRC.AUTHOR_TYPE, '')
					 OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
					 OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.VERIFIED = ISNULL(SRC.VERIFIED, '')
                     ,DST.CREDITS = ISNULL(SRC.CREDITS, 0)
                     ,DST.LAW_CREDITS = ISNULL(SRC.LAW_CREDITS, 0)
                     ,DST.ETHICS_CREDITS = ISNULL(SRC.ETHICS_CREDITS, 0)
                     ,DST.IS_SPEAKER = ISNULL(SRC.IS_SPEAKER, '')
                     ,DST.IS_AUTHOR = ISNULL(SRC.IS_AUTHOR, '')
                     ,DST.SELF_REPORTED = ISNULL(SRC.SELF_REPORTED, '')
                     ,DST.COMMENT_ID = ISNULL(SRC.COMMENT_ID, '')
					 ,DST.CONTACT_ID = ISNULL(SRC.CONTACT_ID, '')
					 ,DST.EVENT_ID = ISNULL(SRC.EVENT_ID, '')
					 ,DST.LOG_ID = ISNULL(SRC.LOG_ID, '')
					 ,DST.SUBMITTED_TIME = ISNULL(SRC.SUBMITTED_TIME, '')
					 ,DST.BEGIN_TIME = try_convert(datetime,left(SRC.BEGIN_TIME, 19))
					 ,DST.CITY = ISNULL(SRC.CITY, '')
					 ,DST.COUNTRY = ISNULL(SRC.COUNTRY, '')
					 ,DST.DESCRIPTION = ISNULL(SRC.DESCRIPTION, '')
					 ,DST.END_TIME = try_convert(datetime,left(SRC.END_TIME, 19))
					 ,DST.PROVIDER_NAME = ISNULL(SRC.PROVIDER_NAME, '')
					 ,DST.STATE = ISNULL(SRC.STATE, '')
					 ,DST.TITLE = ISNULL(SRC.TITLE, '')
					 ,DST.IS_CARRYOVER = ISNULL(SRC.IS_CARRYOVER, '')
					 ,DST.IS_PRO_BONO = ISNULL(SRC.IS_PRO_BONO, '')
					 ,DST.LEARNING_OBJECTIVES = ISNULL(SRC.LEARNING_OBJECTIVES, '')
					 ,DST.TIMEZONE = ISNULL(SRC.TIMEZONE, '')
                     ,DST.AUTHOR_TYPE = ISNULL(SRC.AUTHOR_TYPE, '')
					 ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
					 ,DST.CREATED_TIME = ISNULL(SRC.CREATED_TIME, '')
WHEN NOT MATCHED
       THEN
              INSERT (
     [id]
      ,[verified]
      ,[credits]
      ,[law_credits]
      ,[ethics_credits]
      ,[is_speaker]
      ,[is_author]
      ,[self_reported]
      ,[comment_id]
      ,[contact_id]
      ,[event_id]
      ,[log_id]
      ,[submitted_time]
      ,[begin_time]
      ,[city]
      ,[country]
      ,[description]
      ,[end_time]
      ,[provider_name]
      ,[state]
      ,[title]
      ,[is_carryover]
      ,[is_pro_bono]
      ,[learning_objectives]
      ,[timezone]
      ,[author_type]
      ,[updated_time]
      ,[created_time]
          
                     )
              VALUES (
                    src.[id]
      ,src.[verified]
      ,src.[credits]
      ,src.[law_credits]
      ,src.[ethics_credits]
      ,src.[is_speaker]
      ,src.[is_author]
      ,src.[self_reported]
      ,src.[comment_id]
      ,src.[contact_id]
      ,src.[event_id]
      ,src.[log_id]
      ,src.[submitted_time]
      ,try_convert(datetime, left(src.[begin_time],19))
      ,src.[city]
      ,src.[country]
      ,src.[description]
      ,try_convert(datetime, left(src.[end_time],19))
      ,src.[provider_name]
      ,src.[state]
      ,src.[title]
      ,src.[is_carryover]
      ,src.[is_pro_bono]
      ,src.[learning_objectives]
      ,src.[timezone]
      ,src.[author_type]
      ,src.[updated_time]
      ,src.[created_time]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_cm_claim'
       ,'stg.dj_cm_claim'
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
				from tmp.dj_cm_claim
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp


/************ DJ_CM_LOG ************/
MERGE stg.dj_cm_log AS DST
USING tmp.dj_cm_log AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
			  AND DST.ISACTIVE = 1	
              AND (
                   
                     ISNULL(DST.STATUS, '') <> ISNULL(SRC.STATUS, '')
                     OR ISNULL(DST.IS_CURRENT, '') <> ISNULL(SRC.IS_CURRENT, '')
                     OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
                     OR ISNULL(DST.PERIOD_ID, '') <> ISNULL(SRC.PERIOD_ID, '')
                     OR ISNULL(DST.CREDITS_REQUIRED, 0) <> ISNULL(SRC.CREDITS_REQUIRED, 0)
                     OR ISNULL(DST.ETHICS_CREDITS_REQUIRED, 0) <> ISNULL(SRC.ETHICS_CREDITS_REQUIRED, 0)
                     OR ISNULL(DST.LAW_CREDITS_REQUIRED, 0) <> ISNULL(SRC.LAW_CREDITS_REQUIRED, 0)
                     OR ISNULL(DST.BEGIN_TIME, '') <> ISNULL(SRC.BEGIN_TIME, '')
					 OR ISNULL(DST.END_TIME, '') <> ISNULL(SRC.END_TIME, '')
					 OR ISNULL(DST.REINSTATEMENT_END_TIME, '') <> ISNULL(SRC.REINSTATEMENT_END_TIME, '')
					 OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
					 OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
					 
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
					 
WHEN NOT MATCHED
       THEN
              INSERT (
    [id]
      ,[status]
      ,[is_current]
      ,[contact_id]
      ,[period_id]
      ,[credits_required]
      ,[ethics_credits_required]
      ,[law_credits_required]
      ,[begin_time]
      ,[end_time]
      ,[reinstatement_end_time]
      ,[updated_time]
      ,[created_time]
	  ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                    SRC.[id]
      ,SRC.[status]
      ,SRC.[is_current]
      ,SRC.[contact_id]
      ,SRC.[period_id]
      ,SRC.[credits_required]
      ,SRC.[ethics_credits_required]
      ,SRC.[law_credits_required]
      ,SRC.[begin_time]
      ,SRC.[end_time]
      ,SRC.[reinstatement_end_time]
      ,SRC.[updated_time]
      ,SRC.[created_time]
	  ,1
      ,@TODAY
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_cm_log'
       ,'stg.dj_cm_log'
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
				from tmp.dj_cm_log
				) 
       ,getdate()
       )

INSERT INTO stg.dj_cm_log (
[id]
      ,[status]
      ,[is_current]
      ,[contact_id]
      ,[period_id]
      ,[credits_required]
      ,[ethics_credits_required]
      ,[law_credits_required]
      ,[begin_time]
      ,[end_time]
      ,[reinstatement_end_time]
      ,[updated_time]
      ,[created_time]
	  ,[IsActive]
      ,[StartDate]
                 )
select  
                    [id]
      ,[status]
      ,[is_current]
      ,[contact_id]
      ,[period_id]
      ,[credits_required]
      ,[ethics_credits_required]
      ,[law_credits_required]
      ,[begin_time]
      ,[end_time]
      ,[reinstatement_end_time]
      ,[updated_time]
      ,[created_time]
                 ,1
                 ,@Today
           
         
FROM 
tmp.dj_cm_log
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp


/************ DJ_CM_PERIOD ************/
MERGE stg.dj_cm_period AS DST
USING tmp.dj_cm_period AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.CODE, '') <> ISNULL(SRC.CODE, '')
                     OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
                     OR ISNULL(DST.STATUS, '') <> ISNULL(SRC.STATUS, '')
                     OR ISNULL(DST.DESCRIPTION, '') <> ISNULL(SRC.DESCRIPTION, '')
                     OR ISNULL(DST.SLUG, '') <> ISNULL(SRC.SLUG, '')
                     OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
                     OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
                     OR ISNULL(DST.BEGIN_TIME, '') <> ISNULL(SRC.BEGIN_TIME, '')
					 OR ISNULL(DST.END_TIME, '') <> ISNULL(SRC.END_TIME, '')
					 OR ISNULL(DST.GRACE_END_TIME, '') <> ISNULL(SRC.GRACE_END_TIME, '')
					 OR ISNULL(DST.CREATED_BY_ID, '') <> ISNULL(SRC.CREATED_BY_ID, '')
					 OR ISNULL(DST.ROLLOVER_FROM_ID, '') <> ISNULL(SRC.ROLLOVER_FROM_ID, '')
					 OR ISNULL(DST.UPDATED_BY_ID, '') <> ISNULL(SRC.UPDATED_BY_ID, '')
					
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.CODE = ISNULL(SRC.CODE, '')
                     ,DST.TITLE = ISNULL(SRC.TITLE, '')
                     ,DST.STATUS = ISNULL(SRC.STATUS, '')
                     ,DST.DESCRIPTION = ISNULL(SRC.DESCRIPTION, '')
                     ,DST.SLUG = ISNULL(SRC.SLUG, '')
                     ,DST.CREATED_TIME = ISNULL(SRC.CREATED_TIME, '')
                     ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
                     ,DST.BEGIN_TIME = ISNULL(SRC.BEGIN_TIME, '')
					 ,DST.END_TIME = ISNULL(SRC.END_TIME, '')
					 ,DST.GRACE_END_TIME = ISNULL(SRC.GRACE_END_TIME, '')
					 ,DST.CREATED_BY_ID = ISNULL(SRC.CREATED_BY_ID, '')
					 ,DST.ROLLOVER_FROM_ID = ISNULL(SRC.ROLLOVER_FROM_ID, '')
					 ,DST.UPDATED_BY_ID = ISNULL(SRC.UPDATED_BY_ID, '')
					 
WHEN NOT MATCHED
       THEN
              INSERT (
     [id]
      ,[code]
      ,[title]
      ,[status]
      ,[description]
      ,[slug]
      ,[created_time]
      ,[updated_time]
      ,[begin_time]
      ,[end_time]
      ,[grace_end_time]
      ,[created_by_id]
      ,[rollover_from_id]
      ,[updated_by_id]
          
                     )
              VALUES (
                   SRC.[id]
      ,SRC.[code]
      ,SRC.[title]
      ,SRC.[status]
      ,SRC.[description]
      ,SRC.[slug]
      ,SRC.[created_time]
      ,SRC.[updated_time]
      ,SRC.[begin_time]
      ,SRC.[end_time]
      ,SRC.[grace_end_time]
      ,SRC.[created_by_id]
      ,SRC.[rollover_from_id]
      ,SRC.[updated_by_id]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_cm_period'
       ,'stg.dj_cm_period'
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
				from tmp.dj_cm_period
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp
			  /***************** DJ_Store_Product ****************/
MERGE stg.dj_store_product AS DST
USING tmp.dj_store_product AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.CODE, '') <> ISNULL(SRC.CODE, '')
                     OR ISNULL(DST.TITLE, '') <> ISNULL(SRC.TITLE, '')
                     OR ISNULL(DST.STATUS, '') <> ISNULL(SRC.STATUS, '')
                     OR ISNULL(DST.DESCRIPTION, '') <> ISNULL(SRC.DESCRIPTION, '')
                     OR ISNULL(DST.SLUG, '') <> ISNULL(SRC.SLUG, '')
                     OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
                     OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
                     OR ISNULL(DST.PRODUCT_TYPE, '') <> ISNULL(SRC.PRODUCT_TYPE, '')
                     OR ISNULL(DST.IMIS_CODE, '') <> ISNULL(SRC.IMIS_CODE, '')
					 OR ISNULL(DST.MAX_QUANTITY, 0) <> ISNULL(SRC.MAX_QUANTITY, 0)
					 OR ISNULL(DST.MAX_QUANTITY_PER_PERSON, 0) <> ISNULL(SRC.MAX_QUANTITY_PER_PERSON, 0)
					 OR ISNULL(DST.MAX_QUANTITY_STANDBY, 0) <> ISNULL(SRC.MAX_QUANTITY_STANDBY, 0)
					 OR ISNULL(DST.GL_ACCOUNT, '') <> ISNULL(SRC.GL_ACCOUNT, '')
					 OR ISNULL(DST.CONTENT_ID, '') <> ISNULL(SRC.CONTENT_ID, '')
                     OR ISNULL(DST.CREATED_BY_ID, '') <> ISNULL(SRC.CREATED_BY_ID, '')
					 OR ISNULL(DST.UPDATED_BY_ID, '') <> ISNULL(SRC.UPDATED_BY_ID, '')
					 OR ISNULL(DST.AGREEMENT_STATEMENT_1, '') <> ISNULL(SRC.AGREEMENT_STATEMENT_1, '')
					 OR ISNULL(DST.AGREEMENT_STATEMENT_2, '') <> ISNULL(SRC.AGREEMENT_STATEMENT_2, '')
					 OR ISNULL(DST.AGREEMENT_STATEMENT_3, '') <> ISNULL(SRC.AGREEMENT_STATEMENT_3, '')
					 OR ISNULL(DST.QUESTION_1, '') <> ISNULL(SRC.QUESTION_1, '')
					 OR ISNULL(DST.QUESTION_2, '') <> ISNULL(SRC.QUESTION_2, '')
					 OR ISNULL(DST.QUESTION_3, '') <> ISNULL(SRC.QUESTION_3, '')
					 OR ISNULL(DST.CURRENT_QUANTITY_TAKEN, 0) <> ISNULL(SRC.CURRENT_QUANTITY_TAKEN, 0)
					 OR ISNULL(DST.EMAIL_TEMPLATE_ID, '') <> ISNULL(SRC.EMAIL_TEMPLATE_ID, '')
					 OR ISNULL(DST.REFUND_CUTOFF_TIME, '') <> ISNULL(SRC.REFUND_CUTOFF_TIME, '')
					 OR ISNULL(DST.PUBLISH_STATUS, '') <> ISNULL(SRC.PUBLISH_STATUS, '')
					 OR ISNULL(DST.PUBLISH_TIME, '') <> ISNULL(SRC.PUBLISH_TIME, '')
					 OR ISNULL(DST.PUBLISHED_BY_ID, '') <> ISNULL(SRC.PUBLISHED_BY_ID, '')
					 OR ISNULL(DST.PUBLISHED_TIME, '') <> ISNULL(SRC.PUBLISHED_TIME, '')
					 OR ISNULL(DST.PUBLISH_UUID, '') <> ISNULL(SRC.PUBLISH_UUID, '')
					 OR ISNULL(DST.INDIVIDUALS_CAN_PURCHASE, '') <> ISNULL(SRC.INDIVIDUALS_CAN_PURCHASE, '')
					 OR ISNULL(DST.ORGANIZATIONS_CAN_PURCHASE, '') <> ISNULL(SRC.ORGANIZATIONS_CAN_PURCHASE, '')
					 OR ISNULL(DST.SHIPPABLE, '') <> ISNULL(SRC.SHIPPABLE, '')
					 OR ISNULL(DST.REVIEWS, '') <> ISNULL(SRC.REVIEWS, '')
					 OR ISNULL(DST.CONFIRMATION_TEXT, '') <> ISNULL(SRC.CONFIRMATION_TEXT, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.CODE = ISNULL(SRC.CODE, '')
                     ,DST.TITLE = ISNULL(SRC.TITLE, '')
                     ,DST.STATUS = ISNULL(SRC.STATUS, '')
                     ,DST.DESCRIPTION = ISNULL(SRC.DESCRIPTION, '')
                     ,DST.SLUG = ISNULL(SRC.SLUG, '')
                     ,DST.CREATED_TIME = ISNULL(SRC.CREATED_TIME, '')
                     ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
                     ,DST.PRODUCT_TYPE = ISNULL(SRC.PRODUCT_TYPE, '')
                     ,DST.IMIS_CODE = ISNULL(SRC.IMIS_CODE, '')
					 ,DST.MAX_QUANTITY = ISNULL(SRC.MAX_QUANTITY, 0)
					 ,DST.MAX_QUANTITY_PER_PERSON = ISNULL(SRC.MAX_QUANTITY_PER_PERSON, 0)
					 ,DST.MAX_QUANTITY_STANDBY = ISNULL(SRC.MAX_QUANTITY_STANDBY, 0)
					 ,DST.GL_ACCOUNT = ISNULL(SRC.GL_ACCOUNT, '')
					 ,DST.CONTENT_ID = ISNULL(SRC.CONTENT_ID, '')
                     ,DST.CREATED_BY_ID = ISNULL(SRC.CREATED_BY_ID, '')
					 ,DST.UPDATED_BY_ID = ISNULL(SRC.UPDATED_BY_ID, '')
					 ,DST.AGREEMENT_STATEMENT_1 = ISNULL(SRC.AGREEMENT_STATEMENT_1, '')
					 ,DST.AGREEMENT_STATEMENT_2 = ISNULL(SRC.AGREEMENT_STATEMENT_2, '')
					 ,DST.AGREEMENT_STATEMENT_3 = ISNULL(SRC.AGREEMENT_STATEMENT_3, '')
					 ,DST.QUESTION_1 = ISNULL(SRC.QUESTION_1, '')
					 ,DST.QUESTION_2 = ISNULL(SRC.QUESTION_2, '')
					 ,DST.QUESTION_3 = ISNULL(SRC.QUESTION_3, '')
					 ,DST.CURRENT_QUANTITY_TAKEN = ISNULL(SRC.CURRENT_QUANTITY_TAKEN, 0)
					 ,DST.EMAIL_TEMPLATE_ID = ISNULL(SRC.EMAIL_TEMPLATE_ID, '')
					 ,DST.REFUND_CUTOFF_TIME = ISNULL(SRC.REFUND_CUTOFF_TIME, '')
					 ,DST.PUBLISH_STATUS = ISNULL(SRC.PUBLISH_STATUS, '')
					 ,DST.PUBLISH_TIME = ISNULL(SRC.PUBLISH_TIME, '')
					 ,DST.PUBLISHED_BY_ID = ISNULL(SRC.PUBLISHED_BY_ID, '')
					 ,DST.PUBLISHED_TIME = ISNULL(SRC.PUBLISHED_TIME, '')
					 ,DST.PUBLISH_UUID = ISNULL(SRC.PUBLISH_UUID, '')
					 ,DST.INDIVIDUALS_CAN_PURCHASE = ISNULL(SRC.INDIVIDUALS_CAN_PURCHASE, '')
					 ,DST.ORGANIZATIONS_CAN_PURCHASE = ISNULL(SRC.ORGANIZATIONS_CAN_PURCHASE, '')
					 ,DST.SHIPPABLE = ISNULL(SRC.SHIPPABLE, '')
					 ,DST.REVIEWS = ISNULL(SRC.REVIEWS, '')
					 ,DST.CONFIRMATION_TEXT = ISNULL(SRC.CONFIRMATION_TEXT, '')
WHEN NOT MATCHED
       THEN
              INSERT (
       [id]
      ,[code]
      ,[title]
      ,[status]
      ,[description]
      ,[slug]
      ,[created_time]
      ,[updated_time]
      ,[product_type]
      ,[imis_code]
      ,[max_quantity]
      ,[max_quantity_per_person]
      ,[max_quantity_standby]
      ,[gl_account]
      ,[content_id]
      ,[created_by_id]
      ,[updated_by_id]
      ,[agreement_statement_1]
      ,[agreement_statement_2]
      ,[agreement_statement_3]
      ,[question_1]
      ,[question_2]
      ,[question_3]
      ,[current_quantity_taken]
      ,[email_template_id]
      ,[refund_cutoff_time]
      ,[publish_status]
      ,[publish_time]
      ,[published_by_id]
      ,[published_time]
      ,[publish_uuid]
      ,[individuals_can_purchase]
      ,[organizations_can_purchase]
      ,[shippable]
      ,[reviews]
      ,[confirmation_text]
          
                     )
              VALUES (
                     SRC.[id]
      ,SRC.[code]
      ,SRC.[title]
      ,SRC.[status]
      ,SRC.[description]
      ,SRC.[slug]
      ,SRC.[created_time]
      ,SRC.[updated_time]
      ,SRC.[product_type]
      ,SRC.[imis_code]
      ,SRC.[max_quantity]
      ,SRC.[max_quantity_per_person]
      ,SRC.[max_quantity_standby]
      ,SRC.[gl_account]
      ,SRC.[content_id]
      ,SRC.[created_by_id]
      ,SRC.[updated_by_id]
      ,SRC.[agreement_statement_1]
      ,SRC.[agreement_statement_2]
      ,SRC.[agreement_statement_3]
      ,SRC.[question_1]
      ,SRC.[question_2]
      ,SRC.[question_3]
      ,SRC.[current_quantity_taken]
      ,SRC.[email_template_id]
      ,SRC.[refund_cutoff_time]
      ,SRC.[publish_status]
      ,SRC.[publish_time]
      ,SRC.[published_by_id]
      ,SRC.[published_time]
      ,SRC.[publish_uuid]
      ,SRC.[individuals_can_purchase]
      ,SRC.[organizations_can_purchase]
      ,SRC.[shippable]
      ,SRC.[reviews]
      ,SRC.[confirmation_text]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_store_product'
       ,'stg.dj_store_product'
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
				from tmp.dj_store_product
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp

			  /*********************Auth User **********************/
MERGE stg.dj_auth_user AS DST
USING tmp.dj_auth_user AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.username, '') <> ISNULL(SRC.username, '')
                     OR ISNULL(DST.first_name, '') <> ISNULL(SRC.first_name, '')
                     OR ISNULL(DST.last_name, '') <> ISNULL(SRC.last_name, '')
                    -- OR ISNULL(DST.date_joined, '') <> ISNULL(SRC.date_joined, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.username = ISNULL(SRC.username, '')
					 ,DST.first_name = ISNULL(SRC.first_name, '')
					 ,DST.last_name = ISNULL(SRC.last_name, '')
					-- ,DST.date_joined = ISNULL(SRC.date_joined, '')
					 
WHEN NOT MATCHED
       THEN
              INSERT (
      [id]
      ,[username]
      ,[first_name]
      ,[last_name]
     -- ,[date_joined]
          
                     )
              VALUES (
                    src.[id]
      ,src.[username]
      ,src.[first_name]
      ,src.[last_name]
     -- ,cast(SRC.date_joined as bigint)
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_auth_user'
       ,'stg.dj_auth_user'
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
				from tmp.dj_auth_user
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp

			  /******* content_content *******/
     MERGE stg.dj_content_content AS DST
     USING tmp.dj_content_content AS SRC
                 ON DST.ID = SRC.ID
     WHEN MATCHED

                AND (
                    ISNULL(DST.make_public_time, '') <> ISNULL(SRC.make_public_time, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    DST.archive_time <> convert(datetime,left(SRC.archive_time,19)) OR
                    ISNULL(DST.rating_average, 0) <> ISNULL(SRC.rating_average, 0) OR
                    CONVERT(VARCHAR, ISNULL(DST.og_description, '')) <> CONVERT(VARCHAR, ISNULL(SRC.og_description, '')) OR
                    ISNULL(DST.parent_landing_master_id, '') <> ISNULL(SRC.parent_landing_master_id, '') OR
                    ISNULL(DST.resource_published_date, '') <> ISNULL(SRC.resource_published_date, '') OR
                    ISNULL(DST.resource_url, '') <> ISNULL(SRC.resource_url, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.abstract, '')) <> CONVERT(VARCHAR, ISNULL(SRC.abstract, '')) OR
                    ISNULL(DST.content_area, '') <> ISNULL(SRC.content_area, '') OR
                    ISNULL(DST.show_content_without_groups, '') <> ISNULL(SRC.show_content_without_groups, '') OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.volume_number, '') <> ISNULL(SRC.volume_number, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.master_id, '') <> ISNULL(SRC.master_id, '') OR
                    ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '') OR
                    ISNULL(DST.submission_approved_time, '') <> ISNULL(SRC.submission_approved_time, '') OR
                    ISNULL(DST.og_url, '') <> ISNULL(SRC.og_url, '') OR
                    ISNULL(DST.submission_period_id, '') <> ISNULL(SRC.submission_period_id, '') OR
                    ISNULL(DST.featured_image_id, '') <> ISNULL(SRC.featured_image_id, '') OR
                    ISNULL(DST.serial_pub_id, '') <> ISNULL(SRC.serial_pub_id, '') OR
                    ISNULL(DST.og_title, '') <> ISNULL(SRC.og_title, '') OR
                    ISNULL(DST.checkin_username, '') <> ISNULL(SRC.checkin_username, '') OR
                    ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '') OR
                    ISNULL(DST.is_apa, '') <> ISNULL(SRC.is_apa, '') OR
                    ISNULL(DST.submission_category_id, '') <> ISNULL(SRC.submission_category_id, '') OR
                    ISNULL(DST.has_xhtml, '') <> ISNULL(SRC.has_xhtml, '') OR
                    ISNULL(DST.thumbnail, '') <> ISNULL(SRC.thumbnail, '') OR
                    ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '') OR
                    ISNULL(DST.og_type, '') <> ISNULL(SRC.og_type, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.keywords, '')) <> CONVERT(VARCHAR,ISNULL(SRC.keywords, '')) OR
                    ISNULL(DST.copyright_statement, '') <> ISNULL(SRC.copyright_statement, '') OR
                    ISNULL(DST.subtitle, '') <> ISNULL(SRC.subtitle, '') OR
                    ISNULL(DST.language, '') <> ISNULL(SRC.language, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.overline, '') <> ISNULL(SRC.overline, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.copyright_date, '') <> ISNULL(SRC.copyright_date, '') OR
                    ISNULL(DST.parent_id, '') <> ISNULL(SRC.parent_id, '') OR
                    ISNULL(DST.workflow_status, '') <> ISNULL(SRC.workflow_status, '') OR
                    ISNULL(DST.checkin_time, '') <> ISNULL(SRC.checkin_time, '') OR
                    ISNULL(DST.submission_time, '') <> ISNULL(SRC.submission_time, '') OR
                    ISNULL(DST.isbn, '') <> ISNULL(SRC.isbn, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.description, '')) <> CONVERT(VARCHAR,ISNULL(SRC.description, '')) OR
                    ISNULL(DST.content_type, '') <> ISNULL(SRC.content_type, '') OR
                    ISNULL(DST.editorial_comments, '') <> ISNULL(SRC.editorial_comments, '') OR
                    ISNULL(DST.submission_verified, '') <> ISNULL(SRC.submission_verified, '') OR
                    ISNULL(DST.template, '') <> ISNULL(SRC.template, '') OR
                    ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '') OR
                    ISNULL(DST.resource_type, '') <> ISNULL(SRC.resource_type, '') OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.rating_count, '') <> ISNULL(SRC.rating_count, '') OR
                    ISNULL(DST.og_image_id, '') <> ISNULL(SRC.og_image_id, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.text, '')) <> CONVERT(VARCHAR,ISNULL(SRC.text, '')) OR
                    ISNULL(DST.issue_number, '') <> ISNULL(SRC.issue_number, '') OR
                    ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '') OR
                    ISNULL(DST.url, '') <> ISNULL(SRC.url, '') OR
                    ISNULL(DST.file_path, '') <> ISNULL(SRC.file_path, '') OR
                    ISNULL(DST.taxo_xml, '') <> ISNULL(SRC.taxo_xml, '') OR 
                    ISNULL(DST.make_inactive_time, '') <> ISNULL(SRC.make_inactive_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.make_public_time = ISNULL(SRC.make_public_time, ''),
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.archive_time = try_convert(datetime,left(SRC.archive_time, 19)),
                    DST.rating_average = ISNULL(SRC.rating_average, 0),
                    DST.og_description = ISNULL(SRC.og_description, ''),
                    DST.parent_landing_master_id = ISNULL(SRC.parent_landing_master_id, ''),
                    DST.resource_published_date = ISNULL(SRC.resource_published_date, ''),
                    DST.resource_url = ISNULL(SRC.resource_url, ''),
                    DST.abstract = ISNULL(SRC.abstract, ''),
                    DST.content_area = ISNULL(SRC.content_area, ''),
                    DST.show_content_without_groups = ISNULL(SRC.show_content_without_groups, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.volume_number = ISNULL(SRC.volume_number, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.master_id = ISNULL(SRC.master_id, ''),
                    DST.published_time = ISNULL(SRC.published_time, ''),
                    DST.submission_approved_time = ISNULL(SRC.submission_approved_time, ''),
                    DST.og_url = ISNULL(SRC.og_url, ''),
                    DST.submission_period_id = ISNULL(SRC.submission_period_id, ''),
                    DST.featured_image_id = ISNULL(SRC.featured_image_id, ''),
                    DST.serial_pub_id = ISNULL(SRC.serial_pub_id, ''),
                    DST.og_title = ISNULL(SRC.og_title, ''),
                    DST.checkin_username = ISNULL(SRC.checkin_username, ''),
                    DST.publish_status = ISNULL(SRC.publish_status, ''),
                    DST.is_apa = ISNULL(SRC.is_apa, ''),
                    DST.submission_category_id = ISNULL(SRC.submission_category_id, ''),
                    DST.has_xhtml = ISNULL(SRC.has_xhtml, ''),
                    DST.thumbnail = ISNULL(SRC.thumbnail, ''),
                    DST.published_by_id = ISNULL(SRC.published_by_id, ''),
                    DST.og_type = ISNULL(SRC.og_type, ''),
                    DST.keywords = ISNULL(SRC.keywords, ''),
                    DST.copyright_statement = ISNULL(SRC.copyright_statement, ''),
                    DST.subtitle = ISNULL(SRC.subtitle, ''),
                    DST.language = ISNULL(SRC.language, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.overline = ISNULL(SRC.overline, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.copyright_date = ISNULL(SRC.copyright_date, ''),
                    DST.parent_id = ISNULL(SRC.parent_id, ''),
                    DST.workflow_status = ISNULL(SRC.workflow_status, ''),
                    DST.checkin_time = ISNULL(SRC.checkin_time, ''),
                    DST.submission_time = ISNULL(SRC.submission_time, ''),
                    DST.isbn = ISNULL(SRC.isbn, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.code = ISNULL(SRC.code, ''),
                    DST.description = ISNULL(SRC.description, ''),
                    DST.content_type = ISNULL(SRC.content_type, ''),
                    DST.editorial_comments = ISNULL(SRC.editorial_comments, ''),
                    DST.submission_verified = ISNULL(SRC.submission_verified, ''),
                    DST.template = ISNULL(SRC.template, ''),
                    DST.publish_time = ISNULL(SRC.publish_time, ''),
                    DST.resource_type = ISNULL(SRC.resource_type, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.rating_count = ISNULL(SRC.rating_count, ''),
                    DST.og_image_id = ISNULL(SRC.og_image_id, ''),
                    DST.text = ISNULL(SRC.text, ''),
                    DST.issue_number = ISNULL(SRC.issue_number, ''),
                    DST.publish_uuid = ISNULL(SRC.publish_uuid, ''),
                    DST.url = ISNULL(SRC.url, ''),
                    DST.file_path = ISNULL(SRC.file_path, ''),
                    DST.taxo_xml = ISNULL(SRC.taxo_xml, ''),
                    DST.make_inactive_time = ISNULL(SRC.make_inactive_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [make_public_time],
                            [created_by_id],
                            [archive_time],
                            [rating_average],
                            [og_description],
                            [parent_landing_master_id],
                            [resource_published_date],
                            [resource_url],
                            [abstract],
                            [content_area],
                            [show_content_without_groups],
                            [updated_by_id],
                            [volume_number],
                            [updated_time],
                            [master_id],
                            [published_time],
                            [submission_approved_time],
                            [og_url],
                            [submission_period_id],
                            [featured_image_id],
                            [serial_pub_id],
                            [og_title],
                            [checkin_username],
                            [publish_status],
                            [is_apa],
                            [submission_category_id],
                            [has_xhtml],
                            [thumbnail],
                            [published_by_id],
                            [og_type],
                            [keywords],
                            [copyright_statement],
                            [subtitle],
                            [language],
                            [title],
                            [overline],
                            [created_time],
                            [copyright_date],
                            [parent_id],
                            [workflow_status],
                            [checkin_time],
                            [submission_time],
                            [isbn],
                            [status],
                            [code],
                            [description],
                            [content_type],
                            [editorial_comments],
                            [submission_verified],
                            [template],
                            [publish_time],
                            [resource_type],
                            [slug],
                            [rating_count],
                            [og_image_id],
                            [text],
                            [issue_number],
                            [publish_uuid],
                            [url],
                            [file_path],
                            [make_inactive_time],
                            [taxo_xml],
                            [id]
                            )
                            VALUES (
                            SRC.make_public_time,
                            SRC.created_by_id,
                            try_convert(datetime, left(SRC.archive_time,19)),
                            SRC.rating_average,
                            SRC.og_description,
                            SRC.parent_landing_master_id,
                            SRC.resource_published_date,
                            SRC.resource_url,
                            SRC.abstract,
                            SRC.content_area,
                            SRC.show_content_without_groups,
                            SRC.updated_by_id,
                            SRC.volume_number,
                            SRC.updated_time,
                            SRC.master_id,
                            SRC.published_time,
                            SRC.submission_approved_time,
                            SRC.og_url,
                            SRC.submission_period_id,
                            SRC.featured_image_id,
                            SRC.serial_pub_id,
                            SRC.og_title,
                            SRC.checkin_username,
                            SRC.publish_status,
                            SRC.is_apa,
                            SRC.submission_category_id,
                            SRC.has_xhtml,
                            SRC.thumbnail,
                            SRC.published_by_id,
                            SRC.og_type,
                            SRC.keywords,
                            SRC.copyright_statement,
                            SRC.subtitle,
                            SRC.language,
                            SRC.title,
                            SRC.overline,
                            SRC.created_time,
                            SRC.copyright_date,
                            SRC.parent_id,
                            SRC.workflow_status,
                            SRC.checkin_time,
                            SRC.submission_time,
                            SRC.isbn,
                            SRC.status,
                            SRC.code,
                            SRC.description,
                            SRC.content_type,
                            SRC.editorial_comments,
                            SRC.submission_verified,
                            SRC.template,
                            SRC.publish_time,
                            SRC.resource_type,
                            SRC.slug,
                            SRC.rating_count,
                            SRC.og_image_id,
                            SRC.text,
                            SRC.issue_number,
                            SRC.publish_uuid,
                            SRC.url,
                            SRC.file_path,
                            SRC.make_inactive_time,
                            SRC.taxo_xml,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @pipelinename
            ,'tmp.dj_content_content'
            ,'stg.dj_content_content'
            ,(
                SELECT action_Count
                FROM (
                    SELECT action
                    ,count(*) AS action_count
                    FROM #audittemp
                    WHERE action = 'INSERT'
                    GROUP BY action ) X
                )
            ,(
                SELECT action_Count
                FROM (
                        SELECT action
                        ,count(*) as action_count
                        FROM #audittemp
                        WHERE action = 'UPDATE'
                        GROUP BY action
                        ) X
            )

            ,(
                SELECT COUNT(*) AS RowsRead
                FROM tmp.dj_content_content
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* events_event *******/
     MERGE stg.dj_events_event AS DST
     USING tmp.dj_events_event AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.longitude, 0) <> ISNULL(SRC.longitude, 0) OR
                    ISNULL(DST.cm_approved, 0) <> ISNULL(SRC.cm_approved, 0) OR
                    ISNULL(DST.cm_law_requested, 0) <> ISNULL(SRC.cm_law_requested, 0) OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.price_late_cutoff_time, '') <> ISNULL(SRC.price_late_cutoff_time, '') OR
                    ISNULL(DST.digital_product_url, '') <> ISNULL(SRC.digital_product_url, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.more_details, '')) <> CONVERT(VARCHAR,ISNULL(SRC.more_details, '')) OR
                    ISNULL(DST.cm_ethics_approved, 0) <> ISNULL(SRC.cm_ethics_approved, 0) OR
                    DST.begin_time <> convert(datetime,left(SRC.begin_time,19)) OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    ISNULL(DST.always_on_schedule, '') <> ISNULL(SRC.always_on_schedule, '') OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.cm_law_approved, 0) <> ISNULL(SRC.cm_law_approved, 0) OR
                    ISNULL(DST.price_regular_cutoff_time, '') <> ISNULL(SRC.price_regular_cutoff_time, '') OR
                    ISNULL(DST.timezone, '') <> ISNULL(SRC.timezone, '') OR
                    ISNULL(DST.price_early_cutoff_time, '') <> ISNULL(SRC.price_early_cutoff_time, '') OR
                    ISNULL(DST.event_type, '') <> ISNULL(SRC.event_type, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.learning_objectives, '')) <> CONVERT(VARCHAR,ISNULL(SRC.learning_objectives, '')) OR
                    ISNULL(DST.cm_status, '') <> ISNULL(SRC.cm_status, '') OR
                    ISNULL(DST.mail_badge, '') <> ISNULL(SRC.mail_badge, '') OR
                    ISNULL(DST.length_in_minutes, '') <> ISNULL(SRC.length_in_minutes, '') OR
                    ISNULL(DST.outside_vendor, '') <> ISNULL(SRC.outside_vendor, '') OR
                    ISNULL(DST.is_free, '') <> ISNULL(SRC.is_free, '') OR
                    ISNULL(DST.user_address_num, '') <> ISNULL(SRC.user_address_num, '') OR
                    ISNULL(DST.latitude, 0) <> ISNULL(SRC.latitude, 0) OR
                    ISNULL(DST.cm_requested, 0) <> ISNULL(SRC.cm_requested, 0) OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.price_default, 0) <> ISNULL(SRC.price_default, 0) OR
                    ISNULL(DST.is_online, '') <> ISNULL(SRC.is_online, '') OR
                    ISNULL(DST.cm_ethics_requested, 0) <> ISNULL(SRC.cm_ethics_requested, 0) OR
                    CONVERT(VARCHAR,ISNULL(DST.location, '')) <> CONVERT(VARCHAR,ISNULL(SRC.location, '')) OR
                    ISNULL(DST.external_key, '') <> ISNULL(SRC.external_key, '') OR
                    ISNULL(DST.ticket_template, '') <> ISNULL(SRC.ticket_template, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    DST.end_time <> convert(datetime,left(SRC.end_time,19)) OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.longitude = ISNULL(SRC.longitude, 0),
                    DST.cm_approved = ISNULL(SRC.cm_approved, 0),
                    DST.cm_law_requested = ISNULL(SRC.cm_law_requested, 0),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.price_late_cutoff_time = ISNULL(SRC.price_late_cutoff_time, ''),
                    DST.digital_product_url = ISNULL(SRC.digital_product_url, ''),
                    DST.more_details = ISNULL(SRC.more_details, ''),
                    DST.cm_ethics_approved = ISNULL(SRC.cm_ethics_approved, 0),
                    DST.begin_time = try_convert(datetime,left(SRC.begin_time, 19)),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.always_on_schedule = ISNULL(SRC.always_on_schedule, ''),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.cm_law_approved = ISNULL(SRC.cm_law_approved, 0),
                    DST.price_regular_cutoff_time = ISNULL(SRC.price_regular_cutoff_time, ''),
                    DST.timezone = ISNULL(SRC.timezone, ''),
                    DST.price_early_cutoff_time = ISNULL(SRC.price_early_cutoff_time, ''),
                    DST.event_type = ISNULL(SRC.event_type, ''),
                    DST.learning_objectives = ISNULL(SRC.learning_objectives, ''),
                    DST.cm_status = ISNULL(SRC.cm_status, ''),
                    DST.mail_badge = ISNULL(SRC.mail_badge, ''),
                    DST.length_in_minutes = ISNULL(SRC.length_in_minutes, ''),
                    DST.outside_vendor = ISNULL(SRC.outside_vendor, ''),
                    DST.is_free = ISNULL(SRC.is_free, ''),
                    DST.user_address_num = ISNULL(SRC.user_address_num, ''),
                    DST.latitude = ISNULL(SRC.latitude, 0),
                    DST.cm_requested = ISNULL(SRC.cm_requested, 0),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.price_default = ISNULL(SRC.price_default, 0),
                    DST.is_online = ISNULL(SRC.is_online, ''),
                    DST.cm_ethics_requested = ISNULL(SRC.cm_ethics_requested, 0),
                    DST.location = ISNULL(SRC.location, ''),
                    DST.external_key = ISNULL(SRC.external_key, ''),
                    DST.ticket_template = ISNULL(SRC.ticket_template, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.end_time = try_convert(datetime,left(SRC.end_time, 19)),
                    DST.address1 = ISNULL(SRC.address1, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [longitude],
                            [cm_approved],
                            [cm_law_requested],
                            [zip_code_extension],
                            [country],
                            [price_late_cutoff_time],
                            [digital_product_url],
                            [more_details],
                            [cm_ethics_approved],
                            [begin_time],
                            [zip_code],
                            [voter_voice_checksum],
                            [always_on_schedule],
                            [state],
                            [cm_law_approved],
                            [price_regular_cutoff_time],
                            [timezone],
                            [price_early_cutoff_time],
                            [event_type],
                            [learning_objectives],
                            [cm_status],
                            [mail_badge],
                            [length_in_minutes],
                            [outside_vendor],
                            [is_free],
                            [user_address_num],
                            [latitude],
                            [cm_requested],
                            [address2],
                            [price_default],
                            [is_online],
                            [cm_ethics_requested],
                            [location],
                            [external_key],
                            [ticket_template],
                            [city],
                            [end_time],
                            [address1],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.longitude,
                            SRC.cm_approved,
                            SRC.cm_law_requested,
                            SRC.zip_code_extension,
                            SRC.country,
                            SRC.price_late_cutoff_time,
                            SRC.digital_product_url,
                            SRC.more_details,
                            SRC.cm_ethics_approved,
                            try_convert(datetime,left(SRC.begin_time,19)),
                            SRC.zip_code,
                            SRC.voter_voice_checksum,
                            SRC.always_on_schedule,
                            SRC.state,
                            SRC.cm_law_approved,
                            SRC.price_regular_cutoff_time,
                            SRC.timezone,
                            SRC.price_early_cutoff_time,
                            SRC.event_type,
                            SRC.learning_objectives,
                            SRC.cm_status,
                            SRC.mail_badge,
                            SRC.length_in_minutes,
                            SRC.outside_vendor,
                            SRC.is_free,
                            SRC.user_address_num,
                            SRC.latitude,
                            SRC.cm_requested,
                            SRC.address2,
                            SRC.price_default,
                            SRC.is_online,
                            SRC.cm_ethics_requested,
                            SRC.location,
                            SRC.external_key,
                            SRC.ticket_template,
                            SRC.city,
                            try_convert(datetime, left(SRC.end_time,19)),
                            SRC.address1,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @pipelinename
            ,'tmp.dj_events_event'
            ,'stg.dj_events_event'
            ,(
                SELECT action_Count
                FROM (
                    SELECT action
                    ,count(*) AS action_count
                    FROM #audittemp
                    WHERE action = 'INSERT'
                    GROUP BY action ) X
                )
            ,(
                SELECT action_Count
                FROM (
                        SELECT action
                        ,count(*) as action_count
                        FROM #audittemp
                        WHERE action = 'UPDATE'
                        GROUP BY action
                        ) X
            )

            ,(
                SELECT COUNT(*) AS RowsRead
                FROM tmp.dj_events_event
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp




--			 /******* jobs_job *******/
     MERGE stg.dj_jobs_job AS DST
     USING tmp.dj_jobs_job AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.legacy_id, '') <> ISNULL(SRC.legacy_id, '') OR
                    ISNULL(DST.longitude, 0) <> ISNULL(SRC.longitude, 0) OR
                    ISNULL(DST.salary_range, '') <> ISNULL(SRC.salary_range, '') OR
                    ISNULL(DST.contact_us_address2, '') <> ISNULL(SRC.contact_us_address2, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    ISNULL(DST.company, '') <> ISNULL(SRC.company, '') OR
                    ISNULL(DST.contact_us_email, '') <> ISNULL(SRC.contact_us_email, '') OR
                    ISNULL(DST.contact_us_phone, '') <> ISNULL(SRC.contact_us_phone, '') OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.user_address_num, '') <> ISNULL(SRC.user_address_num, '') OR
                    ISNULL(DST.job_type, '') <> ISNULL(SRC.job_type, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.latitude, 0) <> ISNULL(SRC.latitude, 0) OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.contact_us_user_address_num, '') <> ISNULL(SRC.contact_us_user_address_num, '') OR
                    ISNULL(DST.contact_us_zip_code, '') <> ISNULL(SRC.contact_us_zip_code, '') OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.contact_us_state, '') <> ISNULL(SRC.contact_us_state, '') OR
                    ISNULL(DST.contact_us_city, '') <> ISNULL(SRC.contact_us_city, '') OR
                    ISNULL(DST.contact_us_address1, '') <> ISNULL(SRC.contact_us_address1, '') OR
                    ISNULL(DST.contact_us_first_name, '') <> ISNULL(SRC.contact_us_first_name, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.contact_us_last_name, '') <> ISNULL(SRC.contact_us_last_name, '') OR
                    ISNULL(DST.display_contact_info, '') <> ISNULL(SRC.display_contact_info, '') OR
                    ISNULL(DST.post_time, '') <> ISNULL(SRC.post_time, '') OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '') OR
                    ISNULL(DST.contact_us_country, '') <> ISNULL(SRC.contact_us_country, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.legacy_id = ISNULL(SRC.legacy_id, ''),
                    DST.longitude = ISNULL(SRC.longitude, 0),
                    DST.salary_range = ISNULL(SRC.salary_range, ''),
                    DST.contact_us_address2 = ISNULL(SRC.contact_us_address2, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.company = ISNULL(SRC.company, ''),
                    DST.contact_us_email = ISNULL(SRC.contact_us_email, ''),
                    DST.contact_us_phone = ISNULL(SRC.contact_us_phone, ''),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.user_address_num = ISNULL(SRC.user_address_num, ''),
                    DST.job_type = ISNULL(SRC.job_type, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.latitude = ISNULL(SRC.latitude, 0),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.contact_us_user_address_num = ISNULL(SRC.contact_us_user_address_num, ''),
                    DST.contact_us_zip_code = ISNULL(SRC.contact_us_zip_code, ''),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.contact_us_state = ISNULL(SRC.contact_us_state, ''),
                    DST.contact_us_city = ISNULL(SRC.contact_us_city, ''),
                    DST.contact_us_address1 = ISNULL(SRC.contact_us_address1, ''),
                    DST.contact_us_first_name = ISNULL(SRC.contact_us_first_name, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.contact_us_last_name = ISNULL(SRC.contact_us_last_name, ''),
                    DST.display_contact_info = ISNULL(SRC.display_contact_info, ''),
                    DST.post_time = ISNULL(SRC.post_time, ''),
                    DST.address1 = ISNULL(SRC.address1, ''),
                    DST.contact_us_country = ISNULL(SRC.contact_us_country, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [legacy_id],
                            [longitude],
                            [salary_range],
                            [contact_us_address2],
                            [voter_voice_checksum],
                            [company],
                            [contact_us_email],
                            [contact_us_phone],
                            [zip_code_extension],
                            [country],
                            [user_address_num],
                            [job_type],
                            [zip_code],
                            [latitude],
                            [state],
                            [contact_us_user_address_num],
                            [contact_us_zip_code],
                            [address2],
                            [contact_us_state],
                            [contact_us_city],
                            [contact_us_address1],
                            [contact_us_first_name],
                            [city],
                            [contact_us_last_name],
                            [display_contact_info],
                            [post_time],
                            [address1],
                            [contact_us_country],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.legacy_id,
                            SRC.longitude,
                            SRC.salary_range,
                            SRC.contact_us_address2,
                            SRC.voter_voice_checksum,
                            SRC.company,
                            SRC.contact_us_email,
                            SRC.contact_us_phone,
                            SRC.zip_code_extension,
                            SRC.country,
                            SRC.user_address_num,
                            SRC.job_type,
                            SRC.zip_code,
                            SRC.latitude,
                            SRC.state,
                            SRC.contact_us_user_address_num,
                            SRC.contact_us_zip_code,
                            SRC.address2,
                            SRC.contact_us_state,
                            SRC.contact_us_city,
                            SRC.contact_us_address1,
                            SRC.contact_us_first_name,
                            SRC.city,
                            SRC.contact_us_last_name,
                            SRC.display_contact_info,
                            SRC.post_time,
                            SRC.address1,
                            SRC.contact_us_country,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @pipelinename
            ,'tmp.dj_jobs_job'
            ,'stg.dj_jobs_job'
            ,(
                SELECT action_Count
                FROM (
                    SELECT action
                    ,count(*) AS action_count
                    FROM #audittemp
                    WHERE action = 'INSERT'
                    GROUP BY action ) X
                )
            ,(
                SELECT action_Count
                FROM (
                        SELECT action
                        ,count(*) as action_count
                        FROM #audittemp
                        WHERE action = 'UPDATE'
                        GROUP BY action
                        ) X
            )

            ,(
                SELECT COUNT(*) AS RowsRead
                FROM tmp.dj_jobs_job
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* myapa_contact *******/
     MERGE stg.dj_myapa_contact AS DST
     USING tmp.dj_myapa_contact AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.suffix_name, '') <> ISNULL(SRC.suffix_name, '') OR
                    ISNULL(DST.linkedin_url, '') <> ISNULL(SRC.linkedin_url, '') OR
                    ISNULL(DST.organization_type, '') <> ISNULL(SRC.organization_type, '') OR
                    ISNULL(DST.phone, '') <> ISNULL(SRC.phone, '') OR
                    ISNULL(DST.longitude, 0) <> ISNULL(SRC.longitude, 0) OR
                    ISNULL(DST.salary_range, '') <> ISNULL(SRC.salary_range, '') OR
                    ISNULL(DST.user_id, '') <> ISNULL(SRC.user_id, '') OR
                    ISNULL(DST.email, '') <> ISNULL(SRC.email, '') OR
                    ISNULL(DST.company_is_apa, '') <> ISNULL(SRC.company_is_apa, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.staff_teams, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.staff_teams, '')) OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.last_name, '') <> ISNULL(SRC.last_name, '') OR
                    ISNULL(DST.secondary_email, '') <> ISNULL(SRC.secondary_email, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.personal_url, '') <> ISNULL(SRC.personal_url, '') OR
                    ISNULL(DST.prefix_name, '') <> ISNULL(SRC.prefix_name, '') OR
                    ISNULL(DST.parent_organization_id, '') <> ISNULL(SRC.parent_organization_id, '') OR
                    ISNULL(DST.member_type, '') <> ISNULL(SRC.member_type, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.bio, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.bio, '')) OR
                    ISNULL(DST.facebook_url, '') <> ISNULL(SRC.facebook_url, '') OR
                    ISNULL(DST.designation, '') <> ISNULL(SRC.designation, '') OR
                    ISNULL(DST.company, '') <> ISNULL(SRC.company, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.about_me, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.about_me, '')) OR
                    ISNULL(DST.chapter, '') <> ISNULL(SRC.chapter, '') OR
                    ISNULL(DST.pas_type, '') <> ISNULL(SRC.pas_type, '') OR
                    ISNULL(DST.secondary_phone, '') <> ISNULL(SRC.secondary_phone, '') OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.address_type, '') <> ISNULL(SRC.address_type, '') OR
                    ISNULL(DST.instagram_url, '') <> ISNULL(SRC.instagram_url, '') OR
                    ISNULL(DST.twitter_url, '') <> ISNULL(SRC.twitter_url, '') OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.birth_date, '') <> ISNULL(SRC.birth_date, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.cell_phone, '') <> ISNULL(SRC.cell_phone, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    ISNULL(DST.external_id, '') <> ISNULL(SRC.external_id, '') OR
                    ISNULL(DST.user_address_num, '') <> ISNULL(SRC.user_address_num, '') OR
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.description, '')) <> CONVERT(NVARCHAR,ISNULL(SRC.description, '')) OR
                    ISNULL(DST.job_title, '') <> ISNULL(SRC.job_title, '') OR
                    ISNULL(DST.contact_type, '') <> ISNULL(SRC.contact_type, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.ein_number, '') <> ISNULL(SRC.ein_number, '') OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '') OR
                    ISNULL(DST.latitude, 0) <> ISNULL(SRC.latitude, 0) OR
                    ISNULL(DST.middle_name, '') <> ISNULL(SRC.middle_name, '') OR
                    ISNULL(DST.company_fk_id, '') <> ISNULL(SRC.company_fk_id, '') OR
                    ISNULL(DST.first_name, '') <> ISNULL(SRC.first_name, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.suffix_name = ISNULL(SRC.suffix_name, ''),
                    DST.linkedin_url = ISNULL(SRC.linkedin_url, ''),
                    DST.organization_type = ISNULL(SRC.organization_type, ''),
                    DST.phone = ISNULL(SRC.phone, ''),
                    DST.longitude = ISNULL(SRC.longitude, 0),
                    DST.salary_range = ISNULL(SRC.salary_range, ''),
                    DST.user_id = ISNULL(SRC.user_id, ''),
                    DST.email = ISNULL(SRC.email, ''),
                    DST.company_is_apa = ISNULL(SRC.company_is_apa, ''),
                    DST.staff_teams = ISNULL(SRC.staff_teams, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.last_name = ISNULL(SRC.last_name, ''),
                    DST.secondary_email = ISNULL(SRC.secondary_email, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.personal_url = ISNULL(SRC.personal_url, ''),
                    DST.prefix_name = ISNULL(SRC.prefix_name, ''),
                    DST.parent_organization_id = ISNULL(SRC.parent_organization_id, ''),
                    DST.member_type = ISNULL(SRC.member_type, ''),
                    DST.bio = ISNULL(SRC.bio, ''),
                    DST.facebook_url = ISNULL(SRC.facebook_url, ''),
                    DST.designation = ISNULL(SRC.designation, ''),
                    DST.company = ISNULL(SRC.company, ''),
                    DST.about_me = ISNULL(SRC.about_me, ''),
                    DST.chapter = ISNULL(SRC.chapter, ''),
                    DST.pas_type = ISNULL(SRC.pas_type, ''),
                    DST.secondary_phone = ISNULL(SRC.secondary_phone, ''),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.address_type = ISNULL(SRC.address_type, ''),
                    DST.instagram_url = ISNULL(SRC.instagram_url, ''),
                    DST.twitter_url = ISNULL(SRC.twitter_url, ''),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.birth_date = ISNULL(SRC.birth_date, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.cell_phone = ISNULL(SRC.cell_phone, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.external_id = ISNULL(SRC.external_id, ''),
                    DST.user_address_num = ISNULL(SRC.user_address_num, ''),
                    DST.code = ISNULL(SRC.code, ''),
                    DST.description = ISNULL(SRC.description, ''),
                    DST.job_title = ISNULL(SRC.job_title, ''),
                    DST.contact_type = ISNULL(SRC.contact_type, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.ein_number = ISNULL(SRC.ein_number, ''),
                    DST.address1 = ISNULL(SRC.address1, ''),
                    DST.latitude = ISNULL(SRC.latitude, 0),
                    DST.middle_name = ISNULL(SRC.middle_name, ''),
                    DST.company_fk_id = ISNULL(SRC.company_fk_id, ''),
                    DST.first_name = ISNULL(SRC.first_name, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [created_by_id],
                            [suffix_name],
                            [linkedin_url],
                            [organization_type],
                            [phone],
                            [longitude],
                            [salary_range],
                            [user_id],
                            [email],
                            [company_is_apa],
                            [staff_teams],
                            [updated_by_id],
                            [last_name],
                            [secondary_email],
                            [updated_time],
                            [address2],
                            [personal_url],
                            [prefix_name],
                            [parent_organization_id],
                            [member_type],
                            [bio],
                            [facebook_url],
                            [designation],
                            [company],
                            [about_me],
                            [chapter],
                            [pas_type],
                            [secondary_phone],
                            [state],
                            [address_type],
                            [instagram_url],
                            [twitter_url],
                            [zip_code_extension],
                            [title],
                            [birth_date],
                            [created_time],
                            [country],
                            [cell_phone],
                            [city],
                            [slug],
                            [status],
                            [external_id],
                            [user_address_num],
                            [code],
                            [description],
                            [job_title],
                            [contact_type],
                            [voter_voice_checksum],
                            [zip_code],
                            [ein_number],
                            [address1],
                            [latitude],
                            [middle_name],
                            [company_fk_id],
                            [first_name],
                            [id]
                            )
                            VALUES (
                            SRC.created_by_id,
                            SRC.suffix_name,
                            SRC.linkedin_url,
                            SRC.organization_type,
                            SRC.phone,
                            SRC.longitude,
                            SRC.salary_range,
                            SRC.user_id,
                            SRC.email,
                            SRC.company_is_apa,
                            SRC.staff_teams,
                            SRC.updated_by_id,
                            SRC.last_name,
                            SRC.secondary_email,
                            SRC.updated_time,
                            SRC.address2,
                            SRC.personal_url,
                            SRC.prefix_name,
                            SRC.parent_organization_id,
                            SRC.member_type,
                            SRC.bio,
                            SRC.facebook_url,
                            SRC.designation,
                            SRC.company,
                            SRC.about_me,
                            SRC.chapter,
                            SRC.pas_type,
                            SRC.secondary_phone,
                            SRC.state,
                            SRC.address_type,
                            SRC.instagram_url,
                            SRC.twitter_url,
                            SRC.zip_code_extension,
                            SRC.title,
                            SRC.birth_date,
                            SRC.created_time,
                            SRC.country,
                            SRC.cell_phone,
                            SRC.city,
                            SRC.slug,
                            SRC.status,
                            SRC.external_id,
                            SRC.user_address_num,
                            SRC.code,
                            SRC.description,
                            SRC.job_title,
                            SRC.contact_type,
                            SRC.voter_voice_checksum,
                            SRC.zip_code,
                            SRC.ein_number,
                            SRC.address1,
                            SRC.latitude,
                            SRC.middle_name,
                            SRC.company_fk_id,
                            SRC.first_name,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @pipelinename
            ,'tmp.dj_myapa_contact'
            ,'stg.dj_myapa_contact'
            ,(
                SELECT action_Count
                FROM (
                    SELECT action
                    ,count(*) AS action_count
                    FROM #audittemp
                    WHERE action = 'INSERT'
                    GROUP BY action ) X
                )
            ,(
                SELECT action_Count
                FROM (
                        SELECT action
                        ,count(*) as action_count
                        FROM #audittemp
                        WHERE action = 'UPDATE'
                        GROUP BY action
                        ) X
            )

            ,(
                SELECT COUNT(*) AS RowsRead
                FROM tmp.dj_myapa_contact
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

/********************dj_myapa_contact_role****************/
MERGE stg.dj_myapa_contactrole AS DST
USING tmp.dj_myapa_contactrole AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.ROLE_TYPE, '') <> ISNULL(SRC.ROLE_TYPE, '')
                     OR ISNULL(DST.SORT_NUMBER, '') <> ISNULL(SRC.SORT_NUMBER, '')
                     OR ISNULL(DST.CONFIRMED, '') <> ISNULL(SRC.CONFIRMED, '')
                     OR ISNULL(DST.INVITATION_SENT, '') <> ISNULL(SRC.INVITATION_SENT, '')
                     OR ISNULL(DST.CONTENT_RATING, '') <> ISNULL(SRC.CONTENT_RATING, '')
                     OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
                     OR ISNULL(DST.CONTENT_ID, '') <> ISNULL(SRC.CONTENT_ID, '')
                     OR ISNULL(DST.PERMISSION_AV, '') <> ISNULL(SRC.PERMISSION_AV, '')
					 OR ISNULL(DST.PERMISSION_CONTENT, '') <> ISNULL(SRC.PERMISSION_CONTENT, '')
					 OR ISNULL(DST.SPECIAL_STATUS, '') <> ISNULL(SRC.SPECIAL_STATUS, '')
					 OR ISNULL(DST.PUBLISH_STATUS, '') <> ISNULL(SRC.PUBLISH_STATUS, '')
					 OR ISNULL(DST.PUBLISH_TIME, '') <> ISNULL(SRC.PUBLISH_TIME, '')
					 OR ISNULL(DST.PUBLISHED_BY_ID, '') <> ISNULL(SRC.PUBLISHED_BY_ID, '') 
					 OR ISNULL(DST.PUBLISHED_TIME, '') <> ISNULL(SRC.PUBLISHED_TIME, '')
					 OR ISNULL(DST.BIO, '') <> ISNULL(SRC.BIO, '')
					 OR ISNULL(DST.FIRST_NAME, '') <> ISNULL(SRC.FIRST_NAME, '')
					 OR ISNULL(DST.LAST_NAME, '') <> ISNULL(SRC.LAST_NAME, '') 
					 OR ISNULL(DST.MIDDLE_NAME, '') <> ISNULL(SRC.MIDDLE_NAME, '')
					 OR ISNULL(DST.ADDRESS1, '') <> ISNULL(SRC.ADDRESS1, '')
					 OR ISNULL(DST.ADDRESS2, '') <> ISNULL(SRC.ADDRESS2, '')
					 OR ISNULL(DST.CELL_PHONE, '') <> ISNULL(SRC.CELL_PHONE, '')
					 OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
					 OR ISNULL(DST.COMPANY, '') <> ISNULL(SRC.COMPANY, '')
					 OR ISNULL(DST.COUNTRY, '') <> ISNULL(SRC.COUNTRY, '')
                     OR ISNULL(DST.EMAIL, '') <> ISNULL(SRC.EMAIL, '')
					 OR ISNULL(DST.PHONE, '') <> ISNULL(SRC.PHONE, '')
					 OR ISNULL(DST.STATE, '') <> ISNULL(SRC.STATE, '')
					 OR ISNULL(DST.USER_ADDRESS_NUM, '') <> ISNULL(SRC.USER_ADDRESS_NUM, '')
					 OR ISNULL(DST.ZIP_CODE, '') <> ISNULL(SRC.ZIP_CODE, '')
					 OR ISNULL(DST.PUBLISH_UUID, '') <> ISNULL(SRC.PUBLISH_UUID, '')
					 OR ISNULL(DST.EXTERNAL_BIO_URL, '') <> ISNULL(SRC.EXTERNAL_BIO_URL, '')
					 OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
					 OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
					 OR ISNULL(DST.VOTER_VOICE_CHECKSUM, '') <> ISNULL(SRC.VOTER_VOICE_CHECKSUM, '')
					 OR ISNULL(DST.LONGITUDE, 0) <> ISNULL(SRC.LONGITUDE, 0)
					 OR ISNULL(DST.LATITUDE, 0) <> ISNULL(SRC.LATITUDE, 0)
					 OR ISNULL(DST.ZIP_CODE_EXTENSION, '') <> ISNULL(SRC.ZIP_CODE_EXTENSION, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.ROLE_TYPE = ISNULL(SRC.ROLE_TYPE, '')
                     ,DST.SORT_NUMBER =ISNULL(SRC.SORT_NUMBER, '')
                     ,DST.CONFIRMED = ISNULL(SRC.CONFIRMED, '')
                     ,DST.INVITATION_SENT = ISNULL(SRC.INVITATION_SENT, '')
                     ,DST.CONTENT_RATING = ISNULL(SRC.CONTENT_RATING, '')
                     ,DST.CONTACT_ID = ISNULL(SRC.CONTACT_ID, '')
                     ,DST.CONTENT_ID = ISNULL(SRC.CONTENT_ID, '')
                     ,DST.PERMISSION_AV = ISNULL(SRC.PERMISSION_AV, '')
					 ,DST.PERMISSION_CONTENT = ISNULL(SRC.PERMISSION_CONTENT, '')
					 ,DST.SPECIAL_STATUS = ISNULL(SRC.SPECIAL_STATUS, '')
					 ,DST.PUBLISH_STATUS = ISNULL(SRC.PUBLISH_STATUS, '')
					 ,DST.PUBLISH_TIME = ISNULL(SRC.PUBLISH_TIME, '')
					 ,DST.PUBLISHED_BY_ID = ISNULL(SRC.PUBLISHED_BY_ID, '') 
					 ,DST.PUBLISHED_TIME = ISNULL(SRC.PUBLISHED_TIME, '')
					 ,DST.BIO = ISNULL(SRC.BIO, '')
					 ,DST.FIRST_NAME = ISNULL(SRC.FIRST_NAME, '')
					 ,DST.LAST_NAME = ISNULL(SRC.LAST_NAME, '') 
					 ,DST.MIDDLE_NAME = ISNULL(SRC.MIDDLE_NAME, '')
					 ,DST.ADDRESS1 = ISNULL(SRC.ADDRESS1, '')
					 ,DST.ADDRESS2 = ISNULL(SRC.ADDRESS2, '')
					 ,DST.CELL_PHONE = ISNULL(SRC.CELL_PHONE, '')
					 ,DST.CITY = ISNULL(SRC.CITY, '')
					 ,DST.COMPANY = ISNULL(SRC.COMPANY, '')
					 ,DST.COUNTRY = ISNULL(SRC.COUNTRY, '')
                     ,DST.EMAIL = ISNULL(SRC.EMAIL, '')
					 ,DST.PHONE = ISNULL(SRC.PHONE, '')
					 ,DST.STATE = ISNULL(SRC.STATE, '')
					 ,DST.USER_ADDRESS_NUM = ISNULL(SRC.USER_ADDRESS_NUM, '')
					 ,DST.ZIP_CODE = ISNULL(SRC.ZIP_CODE, '')
					 ,DST.PUBLISH_UUID = ISNULL(SRC.PUBLISH_UUID, '')
					 ,DST.EXTERNAL_BIO_URL = ISNULL(SRC.EXTERNAL_BIO_URL, '')
					 ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
					 ,DST.CREATED_TIME = ISNULL(SRC.CREATED_TIME, '')
					 ,DST.VOTER_VOICE_CHECKSUM = ISNULL(SRC.VOTER_VOICE_CHECKSUM, '')
					 ,DST.LONGITUDE = ISNULL(SRC.LONGITUDE, 0)
					 ,DST.LATITUDE = ISNULL(SRC.LATITUDE, 0)
					 ,DST.ZIP_CODE_EXTENSION = ISNULL(SRC.ZIP_CODE_EXTENSION, '')
WHEN NOT MATCHED
       THEN
              INSERT (
     [id]
      ,[role_type]
      ,[sort_number]
      ,[confirmed]
      ,[invitation_sent]
      ,[content_rating]
      ,[contact_id]
      ,[content_id]
      ,[permission_av]
      ,[permission_content]
      ,[special_status]
      ,[publish_status]
      ,[publish_time]
      ,[published_by_id]
      ,[published_time]
      ,[bio]
      ,[first_name]
      ,[last_name]
      ,[middle_name]
      ,[address1]
      ,[address2]
      ,[cell_phone]
      ,[city]
      ,[company]
      ,[country]
      ,[email]
      ,[phone]
      ,[state]
      ,[user_address_num]
      ,[zip_code]
      ,[publish_uuid]
      ,[external_bio_url]
      ,[updated_time]
      ,[created_time]
      ,[voter_voice_checksum]
      ,[latitude]
      ,[longitude]
      ,[zip_code_extension]
          
                     )
              VALUES (
                    SRC.[id]
      ,SRC.[role_type]
      ,SRC.[sort_number]
      ,SRC.[confirmed]
      ,SRC.[invitation_sent]
      ,SRC.[content_rating]
      ,SRC.[contact_id]
      ,SRC.[content_id]
      ,SRC.[permission_av]
      ,SRC.[permission_content]
      ,SRC.[special_status]
      ,SRC.[publish_status]
      ,SRC.[publish_time]
      ,SRC.[published_by_id]
      ,SRC.[published_time]
      ,SRC.[bio]
      ,SRC.[first_name]
      ,SRC.[last_name]
      ,SRC.[middle_name]
      ,SRC.[address1]
      ,SRC.[address2]
      ,SRC.[cell_phone]
      ,SRC.[city]
      ,SRC.[company]
      ,SRC.[country]
      ,SRC.[email]
      ,SRC.[phone]
      ,SRC.[state]
      ,SRC.[user_address_num]
      ,SRC.[zip_code]
      ,SRC.[publish_uuid]
      ,SRC.[external_bio_url]
      ,SRC.[updated_time]
      ,SRC.[created_time]
      ,SRC.[voter_voice_checksum]
      ,SRC.[latitude]
      ,SRC.[longitude]
      ,SRC.[zip_code_extension]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @pipelinename
	   ,'tmp.dj_myapa_contactrole'
       ,'stg.dj_myapa_contactrole'
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
				from tmp.dj_myapa_contactrole
				) 
       ,getdate()
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