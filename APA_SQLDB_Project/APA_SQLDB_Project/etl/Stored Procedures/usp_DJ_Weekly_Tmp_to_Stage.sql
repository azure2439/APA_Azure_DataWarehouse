
/*******************************************************************************************************************************
 Description:   This Stored Procedure merges data from the Postgres temp tables into the staging tables on a weekly basis.
                These tables have been identified to be incrementally merged on a weekly basis. 
 				

Added By:		Morgan Diestler on 5/11/2020				
*******************************************************************************************************************************/

CREATE Procedure [etl].[usp_DJ_Weekly_Tmp_to_Stage]
@PipelineName varchar(60) = 'ssms'
as

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

      /******* exam_exam *******/
     MERGE stg.dj_exam_exam AS DST
     USING tmp.dj_exam_exam AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.application_early_end_time, '') <> ISNULL(SRC.application_early_end_time, '') OR
                    ISNULL(DST.registration_start_time, '') <> ISNULL(SRC.registration_start_time, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.registration_end_time, '') <> ISNULL(SRC.registration_end_time, '') OR
                    ISNULL(DST.start_time, '') <> ISNULL(SRC.start_time, '') OR
                    ISNULL(DST.application_start_time, '') <> ISNULL(SRC.application_start_time, '') OR
                    ISNULL(DST.end_time, '') <> ISNULL(SRC.end_time, '') OR
                    ISNULL(DST.product_id, '') <> ISNULL(SRC.product_id, '') OR
                    ISNULL(DST.is_advanced, '') <> ISNULL(SRC.is_advanced, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.application_end_time, '') <> ISNULL(SRC.application_end_time, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.description, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.description, ''))
                )
                THEN
                    UPDATE
                    SET
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.application_early_end_time = ISNULL(SRC.application_early_end_time, ''),
                    DST.registration_start_time = ISNULL(SRC.registration_start_time, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.code = ISNULL(SRC.code, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.registration_end_time = ISNULL(SRC.registration_end_time, ''),
                    DST.start_time = ISNULL(SRC.start_time, ''),
                    DST.application_start_time = ISNULL(SRC.application_start_time, ''),
                    DST.end_time = ISNULL(SRC.end_time, ''),
                    DST.product_id = ISNULL(SRC.product_id, ''),
                    DST.is_advanced = ISNULL(SRC.is_advanced, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.application_end_time = ISNULL(SRC.application_end_time, ''),
                    DST.description = ISNULL(SRC.description, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [updated_time],
                            [created_by_id],
                            [application_early_end_time],
                            [registration_start_time],
                            [title],
                            [created_time],
                            [code],
                            [slug],
                            [registration_end_time],
                            [start_time],
                            [application_start_time],
                            [end_time],
                            [product_id],
                            [is_advanced],
                            [status],
                            [updated_by_id],
                            [application_end_time],
                            [description],
                            [id]
                            )
                            VALUES (
                            SRC.updated_time,
                            SRC.created_by_id,
                            SRC.application_early_end_time,
                            SRC.registration_start_time,
                            SRC.title,
                            SRC.created_time,
                            SRC.code,
                            SRC.slug,
                            SRC.registration_end_time,
                            SRC.start_time,
                            SRC.application_start_time,
                            SRC.end_time,
                            SRC.product_id,
                            SRC.is_advanced,
                            SRC.status,
                            SRC.updated_by_id,
                            SRC.application_end_time,
                            SRC.description,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @pipelinename
            ,'tmp.dj_exam_exam'
            ,'stg.dj_exam_exam'
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
                FROM tmp.dj_exam_exam
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

			/******* myapa_contactrole *******/
     MERGE stg.dj_myapa_contactrole AS DST
     USING tmp.dj_myapa_contactrole AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.phone, '') <> ISNULL(SRC.phone, '') OR
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
                    ISNULL(DST.email, '') <> ISNULL(SRC.email, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.last_name, '') <> ISNULL(SRC.last_name, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.content_rating, '') <> ISNULL(SRC.content_rating, '') OR
                    ISNULL(DST.confirmed, '') <> ISNULL(SRC.confirmed, '') OR
                    ISNULL(DST.role_type, '') <> ISNULL(SRC.role_type, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.bio, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.bio, '')) OR
                    ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '') OR
                    ISNULL(DST.company, '') <> ISNULL(SRC.company, '') OR
                    ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '') OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.sort_number, '') <> ISNULL(SRC.sort_number, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.external_bio_url, '') <> ISNULL(SRC.external_bio_url, '') OR
                    ISNULL(DST.permission_av, '') <> ISNULL(SRC.permission_av, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.cell_phone, '') <> ISNULL(SRC.cell_phone, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '') OR
                    ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '') OR
                    ISNULL(DST.user_address_num, '') <> ISNULL(SRC.user_address_num, '') OR
                    ISNULL(DST.special_status, '') <> ISNULL(SRC.special_status, '') OR
                    ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '') OR
                    ISNULL(DST.invitation_sent, '') <> ISNULL(SRC.invitation_sent, '') OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '') OR
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
                    ISNULL(DST.middle_name, '') <> ISNULL(SRC.middle_name, '') OR
                    ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '') OR
                    ISNULL(DST.permission_content, '') <> ISNULL(SRC.permission_content, '') OR
                    ISNULL(DST.first_name, '') <> ISNULL(SRC.first_name, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.phone = ISNULL(SRC.phone, ''),
                    DST.longitude = ISNULL(SRC.longitude, ''),
                    DST.email = ISNULL(SRC.email, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.last_name = ISNULL(SRC.last_name, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.content_rating = ISNULL(SRC.content_rating, ''),
                    DST.confirmed = ISNULL(SRC.confirmed, ''),
                    DST.role_type = ISNULL(SRC.role_type, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.bio = ISNULL(SRC.bio, ''),
                    DST.publish_status = ISNULL(SRC.publish_status, ''),
                    DST.company = ISNULL(SRC.company, ''),
                    DST.published_by_id = ISNULL(SRC.published_by_id, ''),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.sort_number = ISNULL(SRC.sort_number, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.external_bio_url = ISNULL(SRC.external_bio_url, ''),
                    DST.permission_av = ISNULL(SRC.permission_av, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.cell_phone = ISNULL(SRC.cell_phone, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.published_time = ISNULL(SRC.published_time, ''),
                    DST.content_id = ISNULL(SRC.content_id, ''),
                    DST.user_address_num = ISNULL(SRC.user_address_num, ''),
                    DST.special_status = ISNULL(SRC.special_status, ''),
                    DST.publish_time = ISNULL(SRC.publish_time, ''),
                    DST.invitation_sent = ISNULL(SRC.invitation_sent, ''),
                    DST.address1 = ISNULL(SRC.address1, ''),
                    DST.latitude = ISNULL(SRC.latitude, ''),
                    DST.middle_name = ISNULL(SRC.middle_name, ''),
                    DST.publish_uuid = ISNULL(SRC.publish_uuid, ''),
                    DST.permission_content = ISNULL(SRC.permission_content, ''),
                    DST.first_name = ISNULL(SRC.first_name, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [phone],
                            [longitude],
                            [email],
                            [zip_code],
                            [last_name],
                            [updated_time],
                            [contact_id],
                            [content_rating],
                            [confirmed],
                            [role_type],
                            [voter_voice_checksum],
                            [bio],
                            [publish_status],
                            [company],
                            [published_by_id],
                            [state],
                            [address2],
                            [zip_code_extension],
                            [sort_number],
                            [created_time],
                            [external_bio_url],
                            [permission_av],
                            [country],
                            [cell_phone],
                            [city],
                            [published_time],
                            [content_id],
                            [user_address_num],
                            [special_status],
                            [publish_time],
                            [invitation_sent],
                            [address1],
                            [latitude],
                            [middle_name],
                            [publish_uuid],
                            [permission_content],
                            [first_name],
                            [id]
                            )
                            VALUES (
                            SRC.phone,
                            SRC.longitude,
                            SRC.email,
                            SRC.zip_code,
                            SRC.last_name,
                            SRC.updated_time,
                            SRC.contact_id,
                            SRC.content_rating,
                            SRC.confirmed,
                            SRC.role_type,
                            SRC.voter_voice_checksum,
                            SRC.bio,
                            SRC.publish_status,
                            SRC.company,
                            SRC.published_by_id,
                            SRC.state,
                            SRC.address2,
                            SRC.zip_code_extension,
                            SRC.sort_number,
                            SRC.created_time,
                            SRC.external_bio_url,
                            SRC.permission_av,
                            SRC.country,
                            SRC.cell_phone,
                            SRC.city,
                            SRC.published_time,
                            SRC.content_id,
                            SRC.user_address_num,
                            SRC.special_status,
                            SRC.publish_time,
                            SRC.invitation_sent,
                            SRC.address1,
                            SRC.latitude,
                            SRC.middle_name,
                            SRC.publish_uuid,
                            SRC.permission_content,
                            SRC.first_name,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_myapa_contactrole'
            ,'stg.dj_myapa_contactrole'
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
                FROM tmp.dj_myapa_contactrole
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* myapa_individualprofile *******/
     MERGE stg.dj_myapa_individualprofile AS DST
     USING tmp.dj_myapa_individualprofile AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.share_events, '') <> ISNULL(SRC.share_events, '') OR
                    ISNULL(DST.share_social, '') <> ISNULL(SRC.share_social, '') OR
                    ISNULL(DST.share_conference, '') <> ISNULL(SRC.share_conference, '') OR
                    ISNULL(DST.image_id, '') <> ISNULL(SRC.image_id, '') OR
                    ISNULL(DST.share_bio, '') <> ISNULL(SRC.share_bio, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.experience, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.experience, '')) OR
                    ISNULL(DST.share_resume, '') <> ISNULL(SRC.share_resume, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.statement, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.statement, '')) OR
                    ISNULL(DST.resume_id, '') <> ISNULL(SRC.resume_id, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.share_education, '') <> ISNULL(SRC.share_education, '') OR
                    ISNULL(DST.share_jobs, '') <> ISNULL(SRC.share_jobs, '') OR
                    ISNULL(DST.share_leadership, '') <> ISNULL(SRC.share_leadership, '') OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.share_advocacy, '') <> ISNULL(SRC.share_advocacy, '') OR
                    ISNULL(DST.share_profile, '') <> ISNULL(SRC.share_profile, '') OR
                    ISNULL(DST.share_contact, '') <> ISNULL(SRC.share_contact, '') OR
                    ISNULL(DST.speaker_opt_out, '') <> ISNULL(SRC.speaker_opt_out, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.share_events = ISNULL(SRC.share_events, ''),
                    DST.share_social = ISNULL(SRC.share_social, ''),
                    DST.share_conference = ISNULL(SRC.share_conference, ''),
                    DST.image_id = ISNULL(SRC.image_id, ''),
                    DST.share_bio = ISNULL(SRC.share_bio, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.experience = ISNULL(SRC.experience, ''),
                    DST.share_resume = ISNULL(SRC.share_resume, ''),
                    DST.statement = ISNULL(SRC.statement, ''),
                    DST.resume_id = ISNULL(SRC.resume_id, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.share_education = ISNULL(SRC.share_education, ''),
                    DST.share_jobs = ISNULL(SRC.share_jobs, ''),
                    DST.share_leadership = ISNULL(SRC.share_leadership, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.share_advocacy = ISNULL(SRC.share_advocacy, ''),
                    DST.share_profile = ISNULL(SRC.share_profile, ''),
                    DST.share_contact = ISNULL(SRC.share_contact, ''),
                    DST.speaker_opt_out = ISNULL(SRC.speaker_opt_out, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [share_events],
                            [share_social],
                            [share_conference],
                            [image_id],
                            [share_bio],
                            [created_time],
                            [experience],
                            [share_resume],
                            [statement],
                            [resume_id],
                            [updated_time],
                            [contact_id],
                            [share_education],
                            [share_jobs],
                            [share_leadership],
                            [slug],
                            [share_advocacy],
                            [share_profile],
                            [share_contact],
                            [speaker_opt_out],
                            [id]
                            )
                            VALUES (
                            SRC.share_events,
                            SRC.share_social,
                            SRC.share_conference,
                            SRC.image_id,
                            SRC.share_bio,
                            SRC.created_time,
                            SRC.experience,
                            SRC.share_resume,
                            SRC.statement,
                            SRC.resume_id,
                            SRC.updated_time,
                            SRC.contact_id,
                            SRC.share_education,
                            SRC.share_jobs,
                            SRC.share_leadership,
                            SRC.slug,
                            SRC.share_advocacy,
                            SRC.share_profile,
                            SRC.share_contact,
                            SRC.speaker_opt_out,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_myapa_individualprofile'
            ,'stg.dj_myapa_individualprofile'
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
                FROM tmp.dj_myapa_individualprofile
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

			MERGE stg.dj_comments_comment AS DST
USING tmp.dj_comments_comment AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.COMMENT_TYPE, '') <> ISNULL(SRC.COMMENT_TYPE, '')
                     OR ISNULL(DST.SUBMITTED_TIME, '') <> ISNULL(SRC.SUBMITTED_TIME, '')
                     OR ISNULL(DST.COMMENTARY, '') <> ISNULL(SRC.COMMENTARY, '')
                     OR ISNULL(DST.RATING, '') <> ISNULL(SRC.RATING, '')
                     OR ISNULL(DST.FLAGGED, '') <> ISNULL(SRC.FLAGGED, '')
                     OR ISNULL(DST.PUBLISH, '') <> ISNULL(SRC.PUBLISH, '')
                     OR ISNULL(DST.CONTACT_ID, '') <> ISNULL(SRC.CONTACT_ID, '')
                     OR ISNULL(DST.CONTENT_ID, '') <> ISNULL(SRC.CONTENT_ID, '')
					 OR ISNULL(DST.CONTACTROLE_ID, '') <> ISNULL(SRC.CONTACTROLE_ID, '')
					 OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
					 OR ISNULL(DST.CREATED_TIME, '') <> ISNULL(SRC.CREATED_TIME, '')
					
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.COMMENT_TYPE = ISNULL(SRC.COMMENT_TYPE, '')
                     ,DST.SUBMITTED_TIME = ISNULL(SRC.SUBMITTED_TIME, '')
                     ,DST.COMMENTARY = ISNULL(SRC.COMMENTARY, '')
                     ,DST.RATING = ISNULL(SRC.RATING, '')
                     ,DST.FLAGGED = ISNULL(SRC.FLAGGED, '')
                     ,DST.PUBLISH = ISNULL(SRC.PUBLISH, '')
                     ,DST.CONTACT_ID = ISNULL(SRC.CONTACT_ID, '')
                     ,DST.CONTENT_ID = ISNULL(SRC.CONTENT_ID, '')
					 ,DST.CONTACTROLE_ID = ISNULL(SRC.CONTACTROLE_ID, '')
					 ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
					 ,DST.CREATED_TIME = ISNULL(SRC.CREATED_TIME, '')
					 
WHEN NOT MATCHED
       THEN
              INSERT (
   [id]
      ,[comment_type]
      ,[submitted_time]
      ,[commentary]
      ,[rating]
      ,[flagged]
      ,[publish]
      ,[contact_id]
      ,[content_id]
      ,[contactrole_id]
      ,[updated_time]
      ,[created_time]
          
                     )
              VALUES (
                   SRC.[id]
      ,SRC.[comment_type]
      ,SRC.[submitted_time]
      ,SRC.[commentary]
      ,SRC.[rating]
      ,SRC.[flagged]
      ,SRC.[publish]
      ,SRC.[contact_id]
      ,SRC.[content_id]
      ,SRC.[contactrole_id]
      ,SRC.[updated_time]
      ,SRC.[created_time]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_comments_comment'
       ,'stg.dj_comments_comment'
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
				from tmp.dj_comments_comment
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp

/************ DJ_COMMENTS_EXTENDEDEVENTEVALUATION ************/
MERGE stg.dj_comments_extendedeventevaluation AS DST
USING tmp.dj_comments_extendedeventevaluation AS SRC
       ON DST.comment_ptr_id = SRC.comment_ptr_id
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.OBJECTIVE_RATING, '') <> ISNULL(SRC.OBJECTIVE_RATING, '')
                     OR ISNULL(DST.VALUE_RATING, '') <> ISNULL(SRC.VALUE_RATING, '')
                     OR ISNULL(DST.LEARN_MORE_CHOICE, '') <> ISNULL(SRC.LEARN_MORE_CHOICE, '')
                     OR ISNULL(DST.COMMENTARY_SUGGESTIONS, '') <> ISNULL(SRC.COMMENTARY_SUGGESTIONS, '')
                     OR ISNULL(DST.COMMENTARY_TAKEAWAYS, '') <> ISNULL(SRC.COMMENTARY_TAKEAWAYS, '')
                     OR ISNULL(DST.UPDATED_TIME, '') <> ISNULL(SRC.UPDATED_TIME, '')
					
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.OBJECTIVE_RATING = ISNULL(SRC.OBJECTIVE_RATING, '')
                     ,DST.VALUE_RATING = ISNULL(SRC.VALUE_RATING, '')
                     ,DST.LEARN_MORE_CHOICE = ISNULL(SRC.LEARN_MORE_CHOICE, '')
                     ,DST.COMMENTARY_SUGGESTIONS = ISNULL(SRC.COMMENTARY_SUGGESTIONS, '')
                     ,DST.COMMENTARY_TAKEAWAYS = ISNULL(SRC.COMMENTARY_TAKEAWAYS, '')
                     ,DST.UPDATED_TIME = ISNULL(SRC.UPDATED_TIME, '')
					 
WHEN NOT MATCHED
       THEN
              INSERT (
  [comment_ptr_id]
      ,[objective_rating]
      ,[value_rating]
      ,[learn_more_choice]
      ,[commentary_suggestions]
      ,[commentary_takeaways]
      ,[updated_time]
          
                     )
              VALUES (
                   SRC.[comment_ptr_id]
      ,SRC.[objective_rating]
      ,SRC.[value_rating]
      ,SRC.[learn_more_choice]
      ,SRC.[commentary_suggestions]
      ,SRC.[commentary_takeaways]
      ,SRC.[updated_time]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.COMMENT_PTR_ID
       ,deleted.COMMENT_PTR_ID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_comments_extendedeventevaluation'
       ,'stg.dj_comments_extendedeventevaluation'
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
				from tmp.dj_comments_extendedeventevaluation
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp


			  /**************************DJ_Content_ContentTagType***************************/
MERGE stg.dj_content_contenttagtype AS DST
USING tmp.dj_content_contenttagtype AS SRC
       ON DST.id = SRC.id 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '')
                     OR ISNULL(DST.tag_type_id, '') <> ISNULL(SRC.tag_type_id, '')
                     OR ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '')
					 OR ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '')
					 OR ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '')
					 OR ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '')
					 OR ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.content_id = ISNULL(SRC.content_id, '')
                     ,DST.tag_type_id = ISNULL(SRC.tag_type_id, '')
                     ,DST.publish_status = ISNULL(SRC.publish_status, '')
					 ,DST.publish_time = ISNULL(SRC.publish_time, '')
					 ,DST.published_by_id = ISNULL(SRC.published_by_id, '')
					 ,DST.published_time = ISNULL(SRC.published_time, '')
					 ,DST.publish_uuid = ISNULL(SRC.publish_uuid, '')
WHEN NOT MATCHED
       THEN
              INSERT (
     [id]
      ,[content_id]
      ,[tag_type_id]
      ,[publish_status]
      ,[publish_time]
      ,[published_by_id]
      ,[published_time]
      ,[publish_uuid]
                     )
              VALUES (
                   src.[id]
      ,src.[content_id]
      ,src.[tag_type_id]
      ,src.[publish_status]
      ,src.[publish_time]
      ,src.[published_by_id]
      ,src.[published_time]
      ,src.[publish_uuid]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_content_contenttagtype'
       ,'stg.dj_content_contenttagtype'
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
				from tmp.dj_content_contenttagtype
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp

/**************************DJ_Content_ContentTagType_Tags***************************/
MERGE stg.dj_content_contenttagtype_tags AS DST
USING tmp.dj_content_contenttagtype_tags AS SRC
       ON DST.id = SRC.id 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.contenttagtype_id, '') <> ISNULL(SRC.contenttagtype_id, '')
                     OR ISNULL(DST.tag_id, '') <> ISNULL(SRC.tag_id, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.contenttagtype_id = ISNULL(SRC.contenttagtype_id, '')
                     ,DST.tag_id = ISNULL(SRC.tag_id, '')
WHEN NOT MATCHED
       THEN
              INSERT (
      [id] 
	  ,[contenttagtype_id]
      ,[tag_id]
                     )
              VALUES (
      src.[id]
	  ,src.[contenttagtype_id]
      ,src.[tag_id]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_content_contenttagtype_tags'
       ,'stg.dj_content_contenttagtype_tags'
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
				from tmp.dj_content_contenttagtype_tags
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp


/**************************DJ_Content_Tag***************************/
MERGE stg.dj_content_tag AS DST
USING tmp.dj_content_tag AS SRC
       ON DST.id = SRC.id 
WHEN MATCHED
             
              AND (
                     ISNULL(DST.code, '') <> ISNULL(SRC.code, '')
                     OR ISNULL(DST.title, '') <> ISNULL(SRC.title, '')
                     OR ISNULL(DST.status, '') <> ISNULL(SRC.status, '')
					 OR ISNULL(DST.description, '') <> ISNULL(SRC.description, '')
					 OR ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '')
					 OR ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '')
					 OR ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
					 OR ISNULL(DST.sort_number, '') <> ISNULL(SRC.sort_number, '')
					 OR ISNULL(DST.taxo_term, '') <> ISNULL(SRC.taxo_term, '')
					 OR ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '')
					 OR ISNULL(DST.parent_id, '') <> ISNULL(SRC.parent_id, '')
					 OR ISNULL(DST.tag_type_id, '') <> ISNULL(SRC.tag_type_id, '')
					 OR ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.code = ISNULL(SRC.code, '')
                     ,DST.title = ISNULL(SRC.title, '')
                     ,DST.status = ISNULL(SRC.status, '')
					 ,DST.description = ISNULL(SRC.description, '')
					 ,DST.slug = ISNULL(SRC.slug, '')
					 ,DST.created_time = ISNULL(SRC.created_time, '')
					 ,DST.updated_time = ISNULL(SRC.updated_time, '')
					 ,DST.sort_number = ISNULL(SRC.sort_number, '')
					 ,DST.taxo_term = ISNULL(SRC.taxo_term, '')
					 ,DST.created_by_id = ISNULL(SRC.created_by_id, '')
					 ,DST.parent_id = ISNULL(SRC.parent_id, '')
					 ,DST.tag_type_id = ISNULL(SRC.tag_type_id, '')
					 ,DST.updated_by_id = ISNULL(SRC.updated_by_id, '')
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
      ,[sort_number]
      ,[taxo_term]
      ,[created_by_id]
      ,[parent_id]
      ,[tag_type_id]
      ,[updated_by_id]
                     )
              VALUES (
                  src.[id]
      ,src.[code]
      ,src.[title]
      ,src.[status]
      ,src.[description]
      ,src.[slug]
      ,src.[created_time]
      ,src.[updated_time]
      ,src.[sort_number]
      ,src.[taxo_term]
      ,src.[created_by_id]
      ,src.[parent_id]
      ,src.[tag_type_id]
      ,src.[updated_by_id]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_content_tag'
       ,'stg.dj_content_tag'
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
				from tmp.dj_content_tag
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp

/**************************DJ_Content_TagType***************************/
MERGE stg.dj_content_tagtype AS DST
USING tmp.dj_content_tagtype AS SRC
       ON DST.id = SRC.id 
WHEN MATCHED
             
              AND (
                     ISNULL(DST.code, '') <> ISNULL(SRC.code, '')
                     OR ISNULL(DST.title, '') <> ISNULL(SRC.title, '')
                     OR ISNULL(DST.status, '') <> ISNULL(SRC.status, '')
					 OR ISNULL(DST.description, '') <> ISNULL(SRC.description, '')
					 OR ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '')
					 OR ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '')
					 OR ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
					 OR ISNULL(DST.min_allowed, '') <> ISNULL(SRC.min_allowed, '')
					 OR ISNULL(DST.max_allowed, '') <> ISNULL(SRC.max_allowed, '')
					 OR ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '')
					 OR ISNULL(DST.parent_id, '') <> ISNULL(SRC.parent_id, '')
					 OR ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '')
					 OR ISNULL(DST.submission_only, '') <> ISNULL(SRC.submission_only, '')
					 OR ISNULL(DST.sort_number, '') <> ISNULL(SRC.sort_number, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.code = ISNULL(SRC.code, '')
                     ,DST.title = ISNULL(SRC.title, '')
                     ,DST.status = ISNULL(SRC.status, '')
					 ,DST.description = ISNULL(SRC.description, '')
					 ,DST.slug = ISNULL(SRC.slug, '')
					 ,DST.created_time = ISNULL(SRC.created_time, '')
					 ,DST.updated_time = ISNULL(SRC.updated_time, '')
					 ,DST.min_allowed = ISNULL(SRC.min_allowed, '')
					 ,DST.max_allowed = ISNULL(SRC.max_allowed, '')
					 ,DST.created_by_id = ISNULL(SRC.created_by_id, '')
					 ,DST.parent_id = ISNULL(SRC.parent_id, '')
					 ,DST.updated_by_id = ISNULL(SRC.updated_by_id, '')
					 ,DST.submission_only = ISNULL(SRC.submission_only, '')
					 ,DST.sort_number = ISNULL(SRC.sort_number, '')
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
      ,[min_allowed]
      ,[max_allowed]
      ,[created_by_id]
      ,[parent_id]
      ,[updated_by_id]
      ,[submission_only]
      ,[sort_number]
                     )
              VALUES (
                  src.[id]
      ,src.[code]
      ,src.[title]
      ,src.[status]
      ,src.[description]
      ,src.[slug]
      ,src.[created_time]
      ,src.[updated_time]
      ,src.[min_allowed]
      ,src.[max_allowed]
      ,src.[created_by_id]
      ,src.[parent_id]
      ,src.[updated_by_id]
      ,src.[submission_only]
      ,src.[sort_number]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.dj_content_tagtype'
       ,'stg.dj_content_tagtype'
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
				from tmp.dj_content_tagtype
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