
CREATE PROCEDURE [etl].[usp_DJ_Tmp_to_Stage_Phil_Test]
@PipelineName varchar(60) = 'ssms'
as

BEGIN

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

     /******* content_content *******/
     MERGE stg.dj_content_content AS DST
     USING tmp.dj_content_content AS SRC
                 ON DST.ID = SRC.ID
     WHEN MATCHED

                AND (
                    ISNULL(DST.make_public_time, '') <> ISNULL(SRC.make_public_time, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.archive_time, '') <> ISNULL(SRC.archive_time, '') OR
                    ISNULL(DST.rating_average, '') <> ISNULL(SRC.rating_average, '') OR
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
                    DST.archive_time = ISNULL(SRC.archive_time, ''),
                    DST.rating_average = ISNULL(SRC.rating_average, ''),
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
                            SRC.archive_time,
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
            @PipelineName
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
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
                    ISNULL(DST.cm_approved, '') <> ISNULL(SRC.cm_approved, '') OR
                    ISNULL(DST.cm_law_requested, '') <> ISNULL(SRC.cm_law_requested, '') OR
                    ISNULL(DST.zip_code_extension, '') <> ISNULL(SRC.zip_code_extension, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.price_late_cutoff_time, '') <> ISNULL(SRC.price_late_cutoff_time, '') OR
                    ISNULL(DST.digital_product_url, '') <> ISNULL(SRC.digital_product_url, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.more_details, '')) <> CONVERT(VARCHAR,ISNULL(SRC.more_details, '')) OR
                    ISNULL(DST.cm_ethics_approved, '') <> ISNULL(SRC.cm_ethics_approved, '') OR
                    ISNULL(DST.begin_time, '') <> ISNULL(SRC.begin_time, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.voter_voice_checksum, '') <> ISNULL(SRC.voter_voice_checksum, '') OR
                    ISNULL(DST.always_on_schedule, '') <> ISNULL(SRC.always_on_schedule, '') OR
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.cm_law_approved, '') <> ISNULL(SRC.cm_law_approved, '') OR
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
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
                    ISNULL(DST.cm_requested, '') <> ISNULL(SRC.cm_requested, '') OR
                    ISNULL(DST.address2, '') <> ISNULL(SRC.address2, '') OR
                    ISNULL(DST.price_default, '') <> ISNULL(SRC.price_default, '') OR
                    ISNULL(DST.is_online, '') <> ISNULL(SRC.is_online, '') OR
                    ISNULL(DST.cm_ethics_requested, '') <> ISNULL(SRC.cm_ethics_requested, '') OR
                    CONVERT(VARCHAR,ISNULL(DST.location, '')) <> CONVERT(VARCHAR,ISNULL(SRC.location, '')) OR
                    ISNULL(DST.external_key, '') <> ISNULL(SRC.external_key, '') OR
                    ISNULL(DST.ticket_template, '') <> ISNULL(SRC.ticket_template, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.end_time, '') <> ISNULL(SRC.end_time, '') OR
                    ISNULL(DST.address1, '') <> ISNULL(SRC.address1, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.longitude = ISNULL(SRC.longitude, ''),
                    DST.cm_approved = ISNULL(SRC.cm_approved, ''),
                    DST.cm_law_requested = ISNULL(SRC.cm_law_requested, ''),
                    DST.zip_code_extension = ISNULL(SRC.zip_code_extension, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.price_late_cutoff_time = ISNULL(SRC.price_late_cutoff_time, ''),
                    DST.digital_product_url = ISNULL(SRC.digital_product_url, ''),
                    DST.more_details = ISNULL(SRC.more_details, ''),
                    DST.cm_ethics_approved = ISNULL(SRC.cm_ethics_approved, ''),
                    DST.begin_time = ISNULL(SRC.begin_time, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.voter_voice_checksum = ISNULL(SRC.voter_voice_checksum, ''),
                    DST.always_on_schedule = ISNULL(SRC.always_on_schedule, ''),
                    DST.state = ISNULL(SRC.state, ''),
                    DST.cm_law_approved = ISNULL(SRC.cm_law_approved, ''),
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
                    DST.latitude = ISNULL(SRC.latitude, ''),
                    DST.cm_requested = ISNULL(SRC.cm_requested, ''),
                    DST.address2 = ISNULL(SRC.address2, ''),
                    DST.price_default = ISNULL(SRC.price_default, ''),
                    DST.is_online = ISNULL(SRC.is_online, ''),
                    DST.cm_ethics_requested = ISNULL(SRC.cm_ethics_requested, ''),
                    DST.location = ISNULL(SRC.location, ''),
                    DST.external_key = ISNULL(SRC.external_key, ''),
                    DST.ticket_template = ISNULL(SRC.ticket_template, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.end_time = ISNULL(SRC.end_time, ''),
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
                            SRC.begin_time,
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
                            SRC.end_time,
                            SRC.address1,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
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
            @PipelineName
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


     /******* exam_examregistration *******/
     MERGE stg.dj_exam_examregistration AS DST
     USING tmp.dj_exam_examregistration AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.verified, '') <> ISNULL(SRC.verified, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.is_pass, '') <> ISNULL(SRC.is_pass, '') OR
                    ISNULL(DST.legacy_id, '') <> ISNULL(SRC.legacy_id, '') OR
                    ISNULL(DST.exam_id, '') <> ISNULL(SRC.exam_id, '') OR
                    ISNULL(DST.registration_type, '') <> ISNULL(SRC.registration_type, '') OR
                    ISNULL(DST.application_id, '') <> ISNULL(SRC.application_id, '') OR
                    ISNULL(DST.code_of_ethics, '') <> ISNULL(SRC.code_of_ethics, '') OR
                    ISNULL(DST.ada_requirement, '') <> ISNULL(SRC.ada_requirement, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.release_information, '') <> ISNULL(SRC.release_information, '') OR
                    ISNULL(DST.gee_eligibility_id, '') <> ISNULL(SRC.gee_eligibility_id, '') OR
                    ISNULL(DST.purchase_id, '') <> ISNULL(SRC.purchase_id, '') OR
                    ISNULL(DST.certificate_name, '') <> ISNULL(SRC.certificate_name, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.verified = ISNULL(SRC.verified, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.is_pass = ISNULL(SRC.is_pass, ''),
                    DST.legacy_id = ISNULL(SRC.legacy_id, ''),
                    DST.exam_id = ISNULL(SRC.exam_id, ''),
                    DST.registration_type = ISNULL(SRC.registration_type, ''),
                    DST.application_id = ISNULL(SRC.application_id, ''),
                    DST.code_of_ethics = ISNULL(SRC.code_of_ethics, ''),
                    DST.ada_requirement = ISNULL(SRC.ada_requirement, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.release_information = ISNULL(SRC.release_information, ''),
                    DST.gee_eligibility_id = ISNULL(SRC.gee_eligibility_id, ''),
                    DST.purchase_id = ISNULL(SRC.purchase_id, ''),
                    DST.certificate_name = ISNULL(SRC.certificate_name, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [verified],
                            [contact_id],
                            [created_time],
                            [is_pass],
                            [legacy_id],
                            [exam_id],
                            [registration_type],
                            [application_id],
                            [code_of_ethics],
                            [ada_requirement],
                            [updated_time],
                            [release_information],
                            [gee_eligibility_id],
                            [purchase_id],
                            [certificate_name],
                            [id]
                            )
                            VALUES (
                            SRC.verified,
                            SRC.contact_id,
                            SRC.created_time,
                            SRC.is_pass,
                            SRC.legacy_id,
                            SRC.exam_id,
                            SRC.registration_type,
                            SRC.application_id,
                            SRC.code_of_ethics,
                            SRC.ada_requirement,
                            SRC.updated_time,
                            SRC.release_information,
                            SRC.gee_eligibility_id,
                            SRC.purchase_id,
                            SRC.certificate_name,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_exam_examregistration'
            ,'stg.dj_exam_examregistration'
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
                FROM tmp.dj_exam_examregistration
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* jobs_job *******/
     MERGE stg.dj_jobs_job AS DST
     USING tmp.dj_jobs_job AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.legacy_id, '') <> ISNULL(SRC.legacy_id, '') OR
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
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
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
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
                    DST.longitude = ISNULL(SRC.longitude, ''),
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
                    DST.latitude = ISNULL(SRC.latitude, ''),
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
            @PipelineName
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
                    ISNULL(DST.longitude, '') <> ISNULL(SRC.longitude, '') OR
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
                    ISNULL(DST.latitude, '') <> ISNULL(SRC.latitude, '') OR
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
                    DST.longitude = ISNULL(SRC.longitude, ''),
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
                    DST.latitude = ISNULL(SRC.latitude, ''),
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
            @PipelineName
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


     /******* myapa_jobhistory *******/
     MERGE stg.dj_myapa_jobhistory AS DST
     USING tmp.dj_myapa_jobhistory AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.state, '') <> ISNULL(SRC.state, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.end_date, '') <> ISNULL(SRC.end_date, '') OR
                    ISNULL(DST.start_date, '') <> ISNULL(SRC.start_date, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.phone, '') <> ISNULL(SRC.phone, '') OR
                    ISNULL(DST.is_current, '') <> ISNULL(SRC.is_current, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.zip_code, '') <> ISNULL(SRC.zip_code, '') OR
                    ISNULL(DST.city, '') <> ISNULL(SRC.city, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.country, '') <> ISNULL(SRC.country, '') OR
                    ISNULL(DST.company, '') <> ISNULL(SRC.company, '') OR
                    ISNULL(DST.is_part_time, '') <> ISNULL(SRC.is_part_time, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.state = ISNULL(SRC.state, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.end_date = ISNULL(SRC.end_date, ''),
                    DST.start_date = ISNULL(SRC.start_date, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.phone = ISNULL(SRC.phone, ''),
                    DST.is_current = ISNULL(SRC.is_current, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.zip_code = ISNULL(SRC.zip_code, ''),
                    DST.city = ISNULL(SRC.city, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.country = ISNULL(SRC.country, ''),
                    DST.company = ISNULL(SRC.company, ''),
                    DST.is_part_time = ISNULL(SRC.is_part_time, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [state],
                            [contact_id],
                            [end_date],
                            [start_date],
                            [title],
                            [phone],
                            [is_current],
                            [created_time],
                            [zip_code],
                            [city],
                            [updated_time],
                            [country],
                            [company],
                            [is_part_time],
                            [id]
                            )
                            VALUES (
                            SRC.state,
                            SRC.contact_id,
                            SRC.end_date,
                            SRC.start_date,
                            SRC.title,
                            SRC.phone,
                            SRC.is_current,
                            SRC.created_time,
                            SRC.zip_code,
                            SRC.city,
                            SRC.updated_time,
                            SRC.country,
                            SRC.company,
                            SRC.is_part_time,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_myapa_jobhistory'
            ,'stg.dj_myapa_jobhistory'
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
                FROM tmp.dj_myapa_jobhistory
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


     /******* myapa_organizationprofile *******/
     MERGE stg.dj_myapa_organizationprofile AS DST
     USING tmp.dj_myapa_organizationprofile AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.number_of_aicp_members, '') <> ISNULL(SRC.number_of_aicp_members, '') OR
                    ISNULL(DST.image_id, '') <> ISNULL(SRC.image_id, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.employer_bio, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.employer_bio, '')) OR
                    ISNULL(DST.date_founded, '') <> ISNULL(SRC.date_founded, '') OR
                    ISNULL(DST.research_inquiry_hours, '') <> ISNULL(SRC.research_inquiry_hours, '') OR
                    ISNULL(DST.principals, '') <> ISNULL(SRC.principals, '') OR
                    ISNULL(DST.number_of_staff, '') <> ISNULL(SRC.number_of_staff, '') OR
                    ISNULL(DST.consultant_listing_until, '') <> ISNULL(SRC.consultant_listing_until, '') OR
                    ISNULL(DST.number_of_planners, '') <> ISNULL(SRC.number_of_planners, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.number_of_aicp_members = ISNULL(SRC.number_of_aicp_members, ''),
                    DST.image_id = ISNULL(SRC.image_id, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.employer_bio = ISNULL(SRC.employer_bio, ''),
                    DST.date_founded = ISNULL(SRC.date_founded, ''),
                    DST.research_inquiry_hours = ISNULL(SRC.research_inquiry_hours, ''),
                    DST.principals = ISNULL(SRC.principals, ''),
                    DST.number_of_staff = ISNULL(SRC.number_of_staff, ''),
                    DST.consultant_listing_until = ISNULL(SRC.consultant_listing_until, ''),
                    DST.number_of_planners = ISNULL(SRC.number_of_planners, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [updated_time],
                            [contact_id],
                            [number_of_aicp_members],
                            [image_id],
                            [created_time],
                            [employer_bio],
                            [date_founded],
                            [research_inquiry_hours],
                            [principals],
                            [number_of_staff],
                            [consultant_listing_until],
                            [number_of_planners],
                            [id]
                            )
                            VALUES (
                            SRC.updated_time,
                            SRC.contact_id,
                            SRC.number_of_aicp_members,
                            SRC.image_id,
                            SRC.created_time,
                            SRC.employer_bio,
                            SRC.date_founded,
                            SRC.research_inquiry_hours,
                            SRC.principals,
                            SRC.number_of_staff,
                            SRC.consultant_listing_until,
                            SRC.number_of_planners,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_myapa_organizationprofile'
            ,'stg.dj_myapa_organizationprofile'
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
                FROM tmp.dj_myapa_organizationprofile
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* publications_publication *******/
     MERGE stg.dj_publications_publication AS DST
     USING tmp.dj_publications_publication AS SRC
                 ON DST.content_ptr_id = SRC.content_ptr_id
     WHEN MATCHED

                AND (
                    ISNULL(DST.edition, '') <> ISNULL(SRC.edition, '') OR
                    ISNULL(DST.page_count, '') <> ISNULL(SRC.page_count, '') OR
                    ISNULL(DST.publication_format, '') <> ISNULL(SRC.publication_format, '') OR
                    ISNULL(DST.publication_download, '') <> ISNULL(SRC.publication_download, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.table_of_contents, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.table_of_contents, ''))
                )
                THEN
                    UPDATE
                    SET
                    DST.edition = ISNULL(SRC.edition, ''),
                    DST.page_count = ISNULL(SRC.page_count, ''),
                    DST.publication_format = ISNULL(SRC.publication_format, ''),
                    DST.publication_download = ISNULL(SRC.publication_download, ''),
                    DST.table_of_contents = ISNULL(SRC.table_of_contents, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [edition],
                            [page_count],
                            [publication_format],
                            [publication_download],
                            [table_of_contents],
                            [content_ptr_id]
                            )
                            VALUES (
                            SRC.edition,
                            SRC.page_count,
                            SRC.publication_format,
                            SRC.publication_download,
                            SRC.table_of_contents,
                            SRC.content_ptr_id )

    OUTPUT $ACTION AS action
       ,inserted.content_ptr_id
       ,deleted.content_ptr_id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_publications_publication'
            ,'stg.dj_publications_publication'
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
                FROM tmp.dj_publications_publication
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp

     /******* submissions_review *******/
     MERGE stg.dj_submissions_review AS DST
     USING tmp.dj_submissions_review AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.rating_4, '') <> ISNULL(SRC.rating_4, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.comments, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.comments, '')) OR
                    ISNULL(DST.review_status, '') <> ISNULL(SRC.review_status, '') OR
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.rating_3, '') <> ISNULL(SRC.rating_3, '') OR
                    ISNULL(DST.review_time, '') <> ISNULL(SRC.review_time, '') OR
                    ISNULL(DST.content_id, '') <> ISNULL(SRC.content_id, '') OR
                    ISNULL(DST.recused, '') <> ISNULL(SRC.recused, '') OR
                    ISNULL(DST.role_id, '') <> ISNULL(SRC.role_id, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    ISNULL(DST.contact_id, '') <> ISNULL(SRC.contact_id, '') OR
                    ISNULL(DST.deadline_time, '') <> ISNULL(SRC.deadline_time, '') OR
                    ISNULL(DST.rating_1, '') <> ISNULL(SRC.rating_1, '') OR
                    CONVERT(NVARCHAR, ISNULL(DST.custom_text_2, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.custom_text_2, '')) OR
                    CONVERT(NVARCHAR, ISNULL(DST.custom_text_1, '')) <> CONVERT(NVARCHAR, ISNULL(SRC.custom_text_1, '')) OR
                    ISNULL(DST.assigned_time, '') <> ISNULL(SRC.assigned_time, '') OR
                    ISNULL(DST.custom_boolean_1, '') <> ISNULL(SRC.custom_boolean_1, '') OR
                    ISNULL(DST.review_round, '') <> ISNULL(SRC.review_round, '') OR
                    ISNULL(DST.review_type, '') <> ISNULL(SRC.review_type, '') OR
                    ISNULL(DST.rating_2, '') <> ISNULL(SRC.rating_2, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.rating_4 = ISNULL(SRC.rating_4, ''),
                    DST.comments = ISNULL(SRC.comments, ''),
                    DST.review_status = ISNULL(SRC.review_status, ''),
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.rating_3 = ISNULL(SRC.rating_3, ''),
                    DST.review_time = ISNULL(SRC.review_time, ''),
                    DST.content_id = ISNULL(SRC.content_id, ''),
                    DST.recused = ISNULL(SRC.recused, ''),
                    DST.role_id = ISNULL(SRC.role_id, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.contact_id = ISNULL(SRC.contact_id, ''),
                    DST.deadline_time = ISNULL(SRC.deadline_time, ''),
                    DST.rating_1 = ISNULL(SRC.rating_1, ''),
                    DST.custom_text_2 = ISNULL(SRC.custom_text_2, ''),
                    DST.custom_text_1 = ISNULL(SRC.custom_text_1, ''),
                    DST.assigned_time = ISNULL(SRC.assigned_time, ''),
                    DST.custom_boolean_1 = ISNULL(SRC.custom_boolean_1, ''),
                    DST.review_round = ISNULL(SRC.review_round, ''),
                    DST.review_type = ISNULL(SRC.review_type, ''),
                    DST.rating_2 = ISNULL(SRC.rating_2, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [rating_4],
                            [comments],
                            [review_status],
                            [created_time],
                            [rating_3],
                            [review_time],
                            [content_id],
                            [recused],
                            [role_id],
                            [updated_time],
                            [contact_id],
                            [deadline_time],
                            [rating_1],
                            [custom_text_2],
                            [custom_text_1],
                            [assigned_time],
                            [custom_boolean_1],
                            [review_round],
                            [review_type],
                            [rating_2],
                            [id]
                            )
                            VALUES (
                            SRC.rating_4,
                            SRC.comments,
                            SRC.review_status,
                            SRC.created_time,
                            SRC.rating_3,
                            SRC.review_time,
                            SRC.content_id,
                            SRC.recused,
                            SRC.role_id,
                            SRC.updated_time,
                            SRC.contact_id,
                            SRC.deadline_time,
                            SRC.rating_1,
                            SRC.custom_text_2,
                            SRC.custom_text_1,
                            SRC.assigned_time,
                            SRC.custom_boolean_1,
                            SRC.review_round,
                            SRC.review_type,
                            SRC.rating_2,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_submissions_review'
            ,'stg.dj_submissions_review'
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
                FROM tmp.dj_submissions_review
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp


END