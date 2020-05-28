 CREATE PROCEDURE [etl].[usp_DJ_Tmp_to_Stage_MBS]
@PipelineName varchar(60) = 'ssms'
as


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

/******* conference_cadmiumsync *******/

     MERGE stg.dj_conference_cadmiumsync AS DST
     USING tmp.dj_conference_cadmiumsync AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.microsite_id, '') <> ISNULL(SRC.microsite_id, '') OR
                    ISNULL(DST.cadmium_event_key, '') <> ISNULL(SRC.cadmium_event_key, '') OR
                    ISNULL(DST.registration_task_id, '') <> ISNULL(SRC.registration_task_id, '') OR
                    ISNULL(DST.endpoint, '') <> ISNULL(SRC.endpoint, '') OR
                    ISNULL(DST.parent_landing_master_id, '') <> ISNULL(SRC.parent_landing_master_id, '') OR
                    ISNULL(DST.track_tag_type_code, '') <> ISNULL(SRC.track_tag_type_code, '') OR
                    ISNULL(DST.activity_tag_type_code, '') <> ISNULL(SRC.activity_tag_type_code, '') OR
                    ISNULL(DST.division_tag_type_code, '') <> ISNULL(SRC.division_tag_type_code, '') OR
                    ISNULL(DST.npc_category_tag_type_code, '') <> ISNULL(SRC.npc_category_tag_type_code, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.microsite_id = ISNULL(SRC.microsite_id, ''),
                    DST.cadmium_event_key = ISNULL(SRC.cadmium_event_key, ''),
                    DST.registration_task_id = ISNULL(SRC.registration_task_id, ''),
                    DST.endpoint = ISNULL(SRC.endpoint, ''),
                    DST.parent_landing_master_id = ISNULL(SRC.parent_landing_master_id, ''),
                    DST.track_tag_type_code = ISNULL(SRC.track_tag_type_code, ''),
                    DST.activity_tag_type_code = ISNULL(SRC.activity_tag_type_code, ''),
                    DST.division_tag_type_code = ISNULL(SRC.division_tag_type_code, ''),
                    DST.npc_category_tag_type_code = ISNULL(SRC.npc_category_tag_type_code, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [microsite_id],
                            [cadmium_event_key],
                            [registration_task_id],
                            [endpoint],
                            [parent_landing_master_id],
                            [track_tag_type_code],
                            [activity_tag_type_code],
                            [division_tag_type_code],
                            [npc_category_tag_type_code],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.microsite_id,
                            SRC.cadmium_event_key,
                            SRC.registration_task_id,
                            SRC.endpoint,
                            SRC.parent_landing_master_id,
                            SRC.track_tag_type_code,
                            SRC.activity_tag_type_code,
                            SRC.division_tag_type_code,
                            SRC.npc_category_tag_type_code,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_conference_cadmiumsync'
            ,'stg.dj_conference_cadmiumsync'
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
                FROM tmp.dj_conference_cadmiumsync
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp








     /******* conference_cadmiummapping *******/
     /******* Confirmed field names MBS *******/
     MERGE stg.dj_conference_cadmiummapping AS DST
     USING tmp.dj_conference_cadmiummapping AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.mapping_type, '') <> ISNULL(SRC.mapping_type, '') OR
                    ISNULL(DST.from_string, '') <> ISNULL(SRC.from_string, '') OR
                    ISNULL(DST.to_string, '') <> ISNULL(SRC.to_string, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.mapping_type = ISNULL(SRC.mapping_type, ''),
                    DST.from_string = ISNULL(SRC.from_string, ''),
                    DST.to_string = ISNULL(SRC.to_string, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [mapping_type],
                            [from_string],
                            [to_string],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.mapping_type,
                            SRC.from_string,
                            SRC.to_string,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_conference_cadmiummapping'
            ,'stg.dj_conference_cadmiummapping'
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
                FROM tmp.dj_conference_cadmiummapping
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp



     /******* conference_microsite *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_conference_microsite AS DST
     USING tmp.dj_conference_microsite AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.event_master_id, '') <> ISNULL(SRC.event_master_id, '') OR
                    ISNULL(DST.home_page_id, '') <> ISNULL(SRC.home_page_id, '') OR
                    ISNULL(DST.search_filters_id, '') <> ISNULL(SRC.search_filters_id, '') OR
                    ISNULL(DST.is_npc, '') <> ISNULL(SRC.is_npc, '') OR
                    ISNULL(DST.short_title, '') <> ISNULL(SRC.short_title, '') OR
                    ISNULL(DST.url_path_stem, '') <> ISNULL(SRC.url_path_stem, '') OR
                    ISNULL(DST.home_page_code, '') <> ISNULL(SRC.home_page_code, '') OR
                    ISNULL(DST.show_skip_to_dates, '') <> ISNULL(SRC.show_skip_to_dates, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    ISNULL(DST.deactivation_date, '') <> ISNULL(SRC.deactivation_date, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.header_ad, '')) <> CONVERT(VARCHAR, ISNULL(SRC.header_ad, '')) OR
                    CONVERT(VARCHAR, ISNULL(DST.sidebar_ad, '')) <> CONVERT(VARCHAR, ISNULL(SRC.sidebar_ad, '')) OR
                    CONVERT(VARCHAR, ISNULL(DST.footer_ad, '')) <> CONVERT(VARCHAR, ISNULL(SRC.footer_ad, '')) OR
                    ISNULL(DST.custom_color, '') <> ISNULL(SRC.custom_color, '') OR
                    ISNULL(DST.hero_image_path, '') <> ISNULL(SRC.hero_image_path, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.home_summary_blurb, '')) <> CONVERT(VARCHAR, ISNULL(SRC.home_summary_blurb, '')) OR
                    ISNULL(DST.signpost_logo_image_path, '') <> ISNULL(SRC.signpost_logo_image_path, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.program_blurb, '')) <> CONVERT(VARCHAR, ISNULL(SRC.program_blurb, '')) OR
                    ISNULL(DST.nosidebar_breakout_image_path, '') <> ISNULL(SRC.nosidebar_breakout_image_path, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.details_local_blurb, '')) <> CONVERT(VARCHAR, ISNULL(SRC.details_local_blurb, '')) OR
                    CONVERT(VARCHAR, ISNULL(DST.interactive_educational_session, '')) <> CONVERT(VARCHAR, ISNULL(SRC.interactive_educational_session, '')) OR
                    CONVERT(VARCHAR, ISNULL(DST.text_blurb_one, '')) <> CONVERT(VARCHAR, ISNULL(SRC.text_blurb_one, '')) OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.event_master_id = ISNULL(SRC.event_master_id, ''),
                    DST.home_page_id = ISNULL(SRC.home_page_id, ''),
                    DST.search_filters_id = ISNULL(SRC.search_filters_id, ''),
                    DST.is_npc = ISNULL(SRC.is_npc, ''),
                    DST.short_title = ISNULL(SRC.short_title, ''),
                    DST.url_path_stem = ISNULL(SRC.url_path_stem, ''),
                    DST.home_page_code = ISNULL(SRC.home_page_code, ''),
                    DST.show_skip_to_dates = ISNULL(SRC.show_skip_to_dates, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.deactivation_date = ISNULL(SRC.deactivation_date, ''),
                    DST.header_ad = ISNULL(SRC.header_ad, ''),
                    DST.sidebar_ad = ISNULL(SRC.sidebar_ad, ''),
                    DST.footer_ad = ISNULL(SRC.footer_ad, ''),
                    DST.custom_color = ISNULL(SRC.custom_color, ''),
                    DST.hero_image_path = ISNULL(SRC.hero_image_path, ''),
                    DST.home_summary_blurb = ISNULL(SRC.home_summary_blurb, ''),
                    DST.signpost_logo_image_path = ISNULL(SRC.signpost_logo_image_path, ''),
                    DST.program_blurb = ISNULL(SRC.program_blurb, ''),
                    DST.nosidebar_breakout_image_path = ISNULL(SRC.nosidebar_breakout_image_path, ''),
                    DST.details_local_blurb = ISNULL(SRC.details_local_blurb, ''),
                    DST.interactive_educational_session = ISNULL(SRC.interactive_educational_session, ''),
                    DST.text_blurb_one = ISNULL(SRC.text_blurb_one, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [event_master_id],
                            [home_page_id],
                            [search_filters_id],
                            [is_npc],
                            [short_title],
                            [url_path_stem],
                            [home_page_code],
                            [show_skip_to_dates],
                            [status],
                            [deactivation_date],
                            [header_ad],
                            [sidebar_ad],
                            [footer_ad],
                            [custom_color],
                            [hero_image_path],
                            [home_summary_blurb],
                            [signpost_logo_image_path],
                            [program_blurb],
                            [nosidebar_breakout_image_path],
                            [details_local_blurb],
                            [interactive_educational_session],
                            [text_blurb_one],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.event_master_id,
                            SRC.home_page_id,
                            SRC.search_filters_id,
                            SRC.is_npc,
                            SRC.short_title,
                            SRC.url_path_stem,
                            SRC.home_page_code,
                            SRC.show_skip_to_dates,
                            SRC.status,
                            SRC.deactivation_date,
                            SRC.header_ad,
                            SRC.sidebar_ad,
                            SRC.footer_ad,
                            SRC.custom_color,
                            SRC.hero_image_path,
                            SRC.home_summary_blurb,
                            SRC.signpost_logo_image_path,
                            SRC.program_blurb,
                            SRC.nosidebar_breakout_image_path,
                            SRC.details_local_blurb,
                            SRC.interactive_educational_session,
                            SRC.text_blurb_one,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_conference_microsite'
            ,'stg.dj_conference_microsite'
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
                FROM tmp.dj_conference_microsite
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


/******* conference_syncmapping *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_conference_syncmapping AS DST
     USING tmp.dj_conference_syncmapping AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.sync_id, '') <> ISNULL(SRC.sync_id, '') OR
                    ISNULL(DST.mapping_id, '') <> ISNULL(SRC.mapping_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.sync_id = ISNULL(SRC.sync_id, ''),
                    DST.mapping_id = ISNULL(SRC.mapping_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [sync_id],
                            [mapping_id],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.sync_id,
                            SRC.mapping_id,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_conference_syncmapping'
            ,'stg.dj_conference_syncmapping'
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
                FROM tmp.dj_conference_syncmapping
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

        /******* consultants_rfp *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_consultants_rfp AS DST
     USING tmp.dj_consultants_rfp AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.user_address_num, '') <> ISNULL(SRC.user_address_num, '') OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '') OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
                    ISNULL(DST.rfp_type, '') <> ISNULL(SRC.rfp_type, '') OR
                    ISNULL(DST.deadline, '') <> ISNULL(SRC.deadline, '') OR
                    ISNULL(DST.email, '') <> ISNULL(SRC.email, '') OR
                    ISNULL(DST.website, '') <> ISNULL(SRC.website, '') OR
                    ISNULL(DST.company, '') <> ISNULL(SRC.company, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.user_address_num = ISNULL(SRC.user_address_num, ''),
                    DST.address1 = ISNULL(SRC.address1, ''),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.longitude = ISNULL(SRC.longitude, ''),
                    DST.latitude = ISNULL(SRC.latitude, ''),
                    DST.rfp_type = ISNULL(SRC.rfp_type, ''),
                    DST.deadline = ISNULL(SRC.deadline, ''),
                    DST.email = ISNULL(SRC.email, ''),
                    DST.website = ISNULL(SRC.website, ''),
                    DST.company = ISNULL(SRC.company, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [user_address_num],
                            [address1],
                            [address2],
                            [city],
                            [state],
                            [zip_code],
                            [country],
                            [voter_voice_checksum],
                            [zip_code_extension],
                            [longitude],
                            [latitude],
                            [rfp_type],
                            [deadline],
                            [email],
                            [website],
                            [company],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.user_address_num,
                            SRC.address1,
                            SRC.address2,
                            SRC.city,
                            SRC.state,
                            SRC.zip_code,
                            SRC.country,
                            SRC.voter_voice_checksum,
                            SRC.zip_code_extension,
                            SRC.longitude,
                            SRC.latitude,
                            SRC.rfp_type,
                            SRC.deadline,
                            SRC.email,
                            SRC.website,
                            SRC.company,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_consultants_rfp'
            ,'stg.dj_consultants_rfp'
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
                FROM tmp.dj_consultants_rfp
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* content_contentrelationship *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_content_contentrelationship AS DST
     USING tmp.dj_content_contentrelationship AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '') OR
                    ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '') OR
                    ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '') OR
                    ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '') OR
                    ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '') OR
                    ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '') OR
                    ISNULL(DST.content_master_related_id, '') <> ISNULL(SRC.content_master_related_id, '') OR
                    ISNULL(DST.relationship, '') <> ISNULL(SRC.relationship, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.published_time = ISNULL(SRC.published_time, ''),
                    DST.publish_time = ISNULL(SRC.publish_time, ''),
                    DST.published_by_id = ISNULL(SRC.published_by_id, ''),
                    DST.publish_status = ISNULL(SRC.publish_status, ''),
                    DST.publish_uuid = ISNULL(SRC.publish_uuid, ''),
                    DST.content_id = ISNULL(SRC.content_id, ''),
                    DST.content_master_related_id = ISNULL(SRC.content_master_related_id, ''),
                    DST.relationship = ISNULL(SRC.relationship, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [published_time],
                            [publish_time],
                            [published_by_id],
                            [publish_status],
                            [publish_uuid],
                            [content_id],
                            [content_master_related_id],
                            [relationship],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.published_time,
                            SRC.publish_time,
                            SRC.published_by_id,
                            SRC.publish_status,
                            SRC.publish_uuid,
                            SRC.content_id,
                            SRC.content_master_related_id,
                            SRC.relationship,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_content_contentrelationship'
            ,'stg.dj_content_contentrelationship'
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
                FROM tmp.dj_content_contentrelationship
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* knowledgebase_collectionrelationship *******/

     MERGE stg.dj_knowledgebase_collectionrelationship AS DST
     USING tmp.dj_knowledgebase_collectionrelationship AS SRC
                 ON DST.contentrelationship_ptr_id = SRC.contentrelationship_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') 
                   
                )
                THEN
                    UPDATE
                    SET
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                           [contentrelationship_ptr_id],
                            [updated_time]
                            
                          
                            )
                            VALUES (
                           src.[contentrelationship_ptr_id],
                            src.[updated_time] )

    OUTPUT $ACTION AS action
       ,inserted.contentrelationship_ptr_id
       ,deleted.contentrelationship_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_knowledgebase_collectionrelationship'
            ,'stg.dj_knowledgebase_collectionrelationship'
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
                FROM tmp.dj_knowledgebase_collectionrelationship
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* learn_grouplicense *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_learn_grouplicense AS DST
     USING tmp.dj_learn_grouplicense AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.purchase_id, '') <> ISNULL(SRC.purchase_id, '') OR
                    ISNULL(DST.license_code, '') <> ISNULL(SRC.license_code, '') OR
                    ISNULL(DST.redemption_date, '') <> ISNULL(SRC.redemption_date, '') OR
                    ISNULL(DST.redemption_contact_id, '') <> ISNULL(SRC.redemption_contact_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.purchase_id = ISNULL(SRC.purchase_id, ''),
                    DST.license_code = ISNULL(SRC.license_code, ''),
                    DST.redemption_date = ISNULL(SRC.redemption_date, ''),
                    DST.redemption_contact_id = ISNULL(SRC.redemption_contact_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [purchase_id],
                            [license_code],
                            [redemption_date],
                            [redemption_contact_id],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.purchase_id,
                            SRC.license_code,
                            SRC.redemption_date,
                            SRC.redemption_contact_id,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_learn_grouplicense'
            ,'stg.dj_learn_grouplicense'
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
                FROM tmp.dj_learn_grouplicense
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* learn_learncourseinfo *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_learn_learncourseinfo AS DST
     USING tmp.dj_learn_learncourseinfo AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.activity_from_id, '') <> ISNULL(SRC.activity_from_id, '') OR
                    ISNULL(DST.course_to_id, '') <> ISNULL(SRC.course_to_id, '') OR
                    ISNULL(DST.run_time, '') <> ISNULL(SRC.run_time, '') OR
                    ISNULL(DST.run_time_cm, '') <> ISNULL(SRC.run_time_cm, '') OR
                    ISNULL(DST.vimeo_id, '') <> ISNULL(SRC.vimeo_id, '') OR
                    ISNULL(DST.lms_course_id, '') <> ISNULL(SRC.lms_course_id, '') OR
                    ISNULL(DST.lms_template, '') <> ISNULL(SRC.lms_template, '') OR
                    ISNULL(DST.lms_product_page_url, '') <> ISNULL(SRC.lms_product_page_url, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.activity_from_id = ISNULL(SRC.activity_from_id, ''),
                    DST.course_to_id = ISNULL(SRC.course_to_id, ''),
                    DST.run_time = ISNULL(SRC.run_time, ''),
                    DST.run_time_cm = ISNULL(SRC.run_time_cm, ''),
                    DST.vimeo_id = ISNULL(SRC.vimeo_id, ''),
                    DST.lms_course_id = ISNULL(SRC.lms_course_id, ''),
                    DST.lms_template = ISNULL(SRC.lms_template, ''),
                    DST.lms_product_page_url = ISNULL(SRC.lms_product_page_url, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [activity_from_id],
                            [course_to_id],
                            [run_time],
                            [run_time_cm],
                            [vimeo_id],
                            [lms_course_id],
                            [lms_template],
                            [lms_product_page_url],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.activity_from_id,
                            SRC.course_to_id,
                            SRC.run_time,
                            SRC.run_time_cm,
                            SRC.vimeo_id,
                            SRC.lms_course_id,
                            SRC.lms_template,
                            SRC.lms_product_page_url,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_learn_learncourseinfo'
            ,'stg.dj_learn_learncourseinfo'
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
                FROM tmp.dj_learn_learncourseinfo
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* media_media *******/
     /***** Confirmed field names MBS ****/

     MERGE stg.dj_media_media AS DST
     USING tmp.dj_media_media AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.media_format, '') <> ISNULL(SRC.media_format, '') OR
                    ISNULL(DST.url_source, '') <> ISNULL(SRC.url_source, '') OR
                    ISNULL(DST.file_type, '') <> ISNULL(SRC.file_type, '') OR
                    ISNULL(DST.resolution, '') <> ISNULL(SRC.resolution, '') OR
                    ISNULL(DST.height, '') <> ISNULL(SRC.height, '') OR
                    ISNULL(DST.width, '') <> ISNULL(SRC.width, '') OR
                    ISNULL(DST.image_file, '') <> ISNULL(SRC.image_file, '') OR
                    ISNULL(DST.uploaded_file, '') <> ISNULL(SRC.uploaded_file, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.media_format = ISNULL(SRC.media_format, ''),
                    DST.url_source = ISNULL(SRC.url_source, ''),
                    DST.file_type = ISNULL(SRC.file_type, ''),
                    DST.resolution = ISNULL(SRC.resolution, ''),
                    DST.height = ISNULL(SRC.height, ''),
                    DST.width = ISNULL(SRC.width, ''),
                    DST.image_file = ISNULL(SRC.image_file, ''),
                    DST.uploaded_file = ISNULL(SRC.uploaded_file, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [media_format],
                            [url_source],
                            [file_type],
                            [resolution],
                            [height],
                            [width],
                            [image_file],
                            [uploaded_file],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.media_format,
                            SRC.url_source,
                            SRC.file_type,
                            SRC.resolution,
                            SRC.height,
                            SRC.width,
                            SRC.image_file,
                            SRC.uploaded_file,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_media_media'
            ,'stg.dj_media_media'
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
                FROM tmp.dj_media_media
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* pages_landingpage *******/
     /***** Confirmed field names MBS ****/

     MERGE stg.dj_pages_landingpage AS DST
     USING tmp.dj_pages_landingpage AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.show_search_results, '') <> ISNULL(SRC.show_search_results, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.search_query, '')) <> CONVERT(VARCHAR, ISNULL(SRC.search_query, '')) OR
                    ISNULL(DST.sort_field, '') <> ISNULL(SRC.sort_field, '') OR
                    ISNULL(DST.search_max, '') <> ISNULL(SRC.search_max, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.show_search_results = ISNULL(SRC.show_search_results, ''),
                    DST.search_query = ISNULL(SRC.search_query, ''),
                    DST.sort_field = ISNULL(SRC.sort_field, ''),
                    DST.search_max = ISNULL(SRC.search_max, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [show_search_results],
                            [search_query],
                            [sort_field],
                            [search_max],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.show_search_results,
                            SRC.search_query,
                            SRC.sort_field,
                            SRC.search_max,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_pages_landingpage'
            ,'stg.dj_pages_landingpage'
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
                FROM tmp.dj_pages_landingpage
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp



     /******* places_contentplace *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_places_contentplace AS DST
     USING tmp.dj_places_contentplace AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '') OR
                    ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '') OR
                    ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '') OR
                    ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '') OR
                    ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '') OR
                    ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '') OR
                    ISNULL(DST.place_id, '') <> ISNULL(SRC.place_id, '') OR
                    ISNULL(DST.tag_parent_state, '') <> ISNULL(SRC.tag_parent_state, '') OR
                    ISNULL(DST.tag_parent_region, '') <> ISNULL(SRC.tag_parent_region, '') OR
                    ISNULL(DST.tag_place_data, '') <> ISNULL(SRC.tag_place_data, '') OR
                    ISNULL(DST.sort_number, '') <> ISNULL(SRC.sort_number, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.published_time = ISNULL(SRC.published_time, ''),
                    DST.publish_time = ISNULL(SRC.publish_time, ''),
                    DST.published_by_id = ISNULL(SRC.published_by_id, ''),
                    DST.publish_status = ISNULL(SRC.publish_status, ''),
                    DST.publish_uuid = ISNULL(SRC.publish_uuid, ''),
                    DST.content_id = ISNULL(SRC.content_id, ''),
                    DST.place_id = ISNULL(SRC.place_id, ''),
                    DST.tag_parent_state = ISNULL(SRC.tag_parent_state, ''),
                    DST.tag_parent_region = ISNULL(SRC.tag_parent_region, ''),
                    DST.tag_place_data = ISNULL(SRC.tag_place_data, ''),
                    DST.sort_number = ISNULL(SRC.sort_number, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [published_time],
                            [publish_time],
                            [published_by_id],
                            [publish_status],
                            [publish_uuid],
                            [content_id],
                            [place_id],
                            [tag_parent_state],
                            [tag_parent_region],
                            [tag_place_data],
                            [sort_number],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.published_time,
                            SRC.publish_time,
                            SRC.published_by_id,
                            SRC.publish_status,
                            SRC.publish_uuid,
                            SRC.content_id,
                            SRC.place_id,
                            SRC.tag_parent_state,
                            SRC.tag_parent_region,
                            SRC.tag_place_data,
                            SRC.sort_number,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_places_contentplace'
            ,'stg.dj_places_contentplace'
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
                FROM tmp.dj_places_contentplace
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* places_place *******/
     /***** Confirmed field names MBS ****/

     MERGE stg.dj_places_place AS DST
     USING tmp.dj_places_place AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.description, '')) <> CONVERT(VARCHAR, ISNULL(SRC.description, '')) OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.place_type, '') <> ISNULL(SRC.place_type, '') OR
                    ISNULL(DST.lsad, '') <> ISNULL(SRC.lsad, '') OR
                    ISNULL(DST.region_id, '') <> ISNULL(SRC.region_id, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.state_code, '') <> ISNULL(SRC.state_code, '') OR
                    ISNULL(DST.state_name, '') <> ISNULL(SRC.state_name, '') OR
                    ISNULL(DST.place_descriptor_name, '') <> ISNULL(SRC.place_descriptor_name, '') OR
                    ISNULL(DST.census_geo_id, '') <> ISNULL(SRC.census_geo_id, '') OR
                    ISNULL(DST.un_region_id, '') <> ISNULL(SRC.un_region_id, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.code = ISNULL(SRC.code, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.description = ISNULL(SRC.description, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.place_type = ISNULL(SRC.place_type, ''),
                    DST.lsad = ISNULL(SRC.lsad, ''),
                    DST.region_id = ISNULL(SRC.region_id, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.state_code = ISNULL(SRC.state_code, ''),
                    DST.state_name = ISNULL(SRC.state_name, ''),
                    DST.place_descriptor_name = ISNULL(SRC.place_descriptor_name, ''),
                    DST.census_geo_id = ISNULL(SRC.census_geo_id, ''),
                    DST.un_region_id = ISNULL(SRC.un_region_id, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [code],
                            [title],
                            [status],
                            [description],
                            [slug],
                            [created_by_id],
                            [updated_by_id],
                            [created_time],
                            [updated_time],
                            [place_type],
                            [lsad],
                            [region_id],
                            [country],
                            [state_code],
                            [state_name],
                            [place_descriptor_name],
                            [census_geo_id],
                            [un_region_id],
                            [id]
                            )
                            VALUES (
                            SRC.code,
                            SRC.title,
                            SRC.status,
                            SRC.description,
                            SRC.slug,
                            SRC.created_by_id,
                            SRC.updated_by_id,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.place_type,
                            SRC.lsad,
                            SRC.region_id,
                            SRC.country,
                            SRC.state_code,
                            SRC.state_name,
                            SRC.place_descriptor_name,
                            SRC.census_geo_id,
                            SRC.un_region_id,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_places_place'
            ,'stg.dj_places_place'
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
                FROM tmp.dj_places_place
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

/******* places_placedata *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_places_placedata AS DST
     USING tmp.dj_places_placedata AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.place_id, '') <> ISNULL(SRC.place_id, '') OR
                    ISNULL(DST.priority, '') <> ISNULL(SRC.priority, '') OR
                    ISNULL(DST.year, '') <> ISNULL(SRC.year, '') OR
                    ISNULL(DST.source_name, '') <> ISNULL(SRC.source_name, '') OR
                    ISNULL(DST.population, '') <> ISNULL(SRC.population, '') OR
                    ISNULL(DST.housing_units, '') <> ISNULL(SRC.housing_units, '') OR
                    ISNULL(DST.density, '') <> ISNULL(SRC.density, '') OR
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
                    ISNULL(DST.land_sq_miles, '') <> ISNULL(SRC.land_sq_miles, '') OR
                    ISNULL(DST.water_sq_miles, '') <> ISNULL(SRC.water_sq_miles, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.place_id = ISNULL(SRC.place_id, ''),
                    DST.priority = ISNULL(SRC.priority, ''),
                    DST.year = ISNULL(SRC.year, ''),
                    DST.source_name = ISNULL(SRC.source_name, ''),
                    DST.population = ISNULL(SRC.population, ''),
                    DST.housing_units = ISNULL(SRC.housing_units, ''),
                    DST.density = ISNULL(SRC.density, ''),
                    DST.latitude = ISNULL(SRC.latitude, ''),
                    DST.longitude = ISNULL(SRC.longitude, ''),
                    DST.land_sq_miles = ISNULL(SRC.land_sq_miles, ''),
                    DST.water_sq_miles = ISNULL(SRC.water_sq_miles, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [place_id],
                            [priority],
                            [year],
                            [source_name],
                            [population],
                            [housing_units],
                            [density],
                            [latitude],
                            [longitude],
                            [land_sq_miles],
                            [water_sq_miles],
                            [created_time],
                            [updated_time],
                            [id]
                            )
                            VALUES (
                            SRC.place_id,
                            SRC.priority,
                            SRC.year,
                            SRC.source_name,
                            SRC.population,
                            SRC.housing_units,
                            SRC.density,
                            SRC.latitude,
                            SRC.longitude,
                            SRC.land_sq_miles,
                            SRC.water_sq_miles,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_places_placedata'
            ,'stg.dj_places_placedata'
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
                FROM tmp.dj_places_placedata
                )
            ,GETDATE()
            )

         TRUNCATE TABLE #audittemp

/******* research_inquiries_inquiry *******/
     /***** Confirmed field names MBS ****/
            TRUNCATE TABLE #audittemp


     MERGE stg.dj_research_inquiries_inquiry AS DST
     USING tmp.dj_research_inquiries_inquiry AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    CONVERT(VARCHAR, ISNULL(DST.response_text, '')) <> CONVERT(VARCHAR, ISNULL(SRC.response_text, '')) OR
                    ISNULL(DST.review_status, '') <> ISNULL(SRC.review_status, '') OR
                    ISNULL(DST.hours, '') <> ISNULL(SRC.hours, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.response_text = ISNULL(SRC.response_text, ''),
                    DST.review_status = ISNULL(SRC.review_status, ''),
                    DST.hours = ISNULL(SRC.hours, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [response_text],
                            [review_status],
                            [hours],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.response_text,
                            SRC.review_status,
                            SRC.hours,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;

    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_research_inquiries_inquiry'
            ,'stg.dj_research_inquiries_inquiry'
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
                FROM tmp.dj_research_inquiries_inquiry
                )
            ,GETDATE()
            )

/******* support_ticket *******/
     /***** Confirmed field names MBS ****/
     MERGE stg.dj_support_ticket AS DST
     USING tmp.dj_support_ticket AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.description, '')) <> CONVERT(VARCHAR, ISNULL(SRC.description, '')) OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.apa_id, '') <> ISNULL(SRC.apa_id, '') OR
                    ISNULL(DST.full_name, '') <> ISNULL(SRC.full_name, '') OR
                    ISNULL(DST.email, '') <> ISNULL(SRC.email, '') OR
                    ISNULL(DST.phone, '') <> ISNULL(SRC.phone, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.category, '') <> ISNULL(SRC.category, '') OR
                    ISNULL(DST.ticket_status, '') <> ISNULL(SRC.ticket_status, '') OR
                    CONVERT(VARCHAR, ISNULL(DST.staff_comments, '')) <> CONVERT(VARCHAR, ISNULL(SRC.staff_comments, ''))
                )
                THEN
                    UPDATE
                    SET
                    DST.code = ISNULL(SRC.code, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.description = ISNULL(SRC.description, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.apa_id = ISNULL(SRC.apa_id, ''),
                    DST.full_name = ISNULL(SRC.full_name, ''),
                    DST.email = ISNULL(SRC.email, ''),
                    DST.phone = ISNULL(SRC.phone, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.category = ISNULL(SRC.category, ''),
                    DST.ticket_status = ISNULL(SRC.ticket_status, ''),
                    DST.staff_comments = ISNULL(SRC.staff_comments, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [code],
                            [title],
                            [status],
                            [description],
                            [slug],
                            [created_by_id],
                            [updated_by_id],
                            [created_time],
                            [updated_time],
                            [apa_id],
                            [full_name],
                            [email],
                            [phone],
                            [contact_id],
                            [category],
                            [ticket_status],
                            [staff_comments],
                            [id]
                            )
                            VALUES (
                            SRC.code,
                            SRC.title,
                            SRC.status,
                            SRC.description,
                            SRC.slug,
                            SRC.created_by_id,
                            SRC.updated_by_id,
                            SRC.created_time,
                            SRC.updated_time,
                            SRC.apa_id,
                            SRC.full_name,
                            SRC.email,
                            SRC.phone,
                            SRC.contact_id,
                            SRC.category,
                            SRC.ticket_status,
                            SRC.staff_comments,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_support_ticket'
            ,'stg.dj_support_ticket'
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
                FROM tmp.dj_support_ticket
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

/******* conference_microsite_program_search_filters *******/

     MERGE stg.dj_conference_microsite_program_search_filters AS DST
     USING tmp.dj_conference_microsite_program_search_filters AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.microsite_id, '') <> ISNULL(SRC.microsite_id, '') OR
                    ISNULL(DST.tagtype_id, '') <> ISNULL(SRC.tagtype_id, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.microsite_id = ISNULL(SRC.microsite_id, ''),
                    DST.tagtype_id = ISNULL(SRC.tagtype_id, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [microsite_id],
                            [tagtype_id],
                            [id]
                            )
                            VALUES (
                            SRC.microsite_id,
                            SRC.tagtype_id,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_conference_microsite_program_search_filters'
            ,'stg.dj_conference_microsite_program_search_filters'
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
                FROM tmp.dj_conference_microsite_program_search_filters
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


/******* content_content_permission_groups *******/

     MERGE stg.dj_content_content_permission_groups AS DST
     USING tmp.dj_content_content_permission_groups AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '') OR
                    ISNULL(DST.group_id, '') <> ISNULL(SRC.group_id, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.content_id = ISNULL(SRC.content_id, ''),
                    DST.group_id = ISNULL(SRC.group_id, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [content_id],
                            [group_id],
                            [id]
                            )
                            VALUES (
                            SRC.content_id,
                            SRC.group_id,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_content_content_permission_groups'
            ,'stg.dj_content_content_permission_groups'
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
                FROM tmp.dj_content_content_permission_groups
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp