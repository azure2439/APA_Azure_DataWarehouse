CREATE   PROCEDURE [etl].[usp_DJ_Tmp_to_Stage_ProductOption_Test]
@PipelineName varchar(60) = 'ssms'
as

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


   
   /******* store_productoption *******/
     MERGE stg.dj_store_productoption AS DST
     USING tmp.dj_store_productoption AS SRC
                 ON DST.id = SRC.id
     WHEN MATCHED

                AND (
                    ISNULL(DST.created_time, '') <> ISNULL(SRC.created_time, '') OR
                    ISNULL(DST.sort_number, '') <> ISNULL(SRC.sort_number, '') OR
                    ISNULL(DST.updated_by_id, '') <> ISNULL(SRC.updated_by_id, '') OR
                    ISNULL(DST.slug, '') <> ISNULL(SRC.slug, '') OR
                    ISNULL(DST.updated_time, '') <> ISNULL(SRC.updated_time, '') OR
                    CONVERT(VARCHAR(MAX), ISNULL(DST.description, '')) <> CONVERT(VARCHAR(MAX), ISNULL(SRC.description, '')) OR
                    ISNULL(DST.status, '') <> ISNULL(SRC.status, '') OR
                    ISNULL(DST.published_by_id, '') <> ISNULL(SRC.published_by_id, '') OR
                    ISNULL(DST.created_by_id, '') <> ISNULL(SRC.created_by_id, '') OR
                    ISNULL(DST.publish_uuid, '') <> ISNULL(SRC.publish_uuid, '') OR
                    ISNULL(DST.title, '') <> ISNULL(SRC.title, '') OR
                    ISNULL(DST.published_time, '') <> ISNULL(SRC.published_time, '') OR
                    ISNULL(DST.publish_time, '') <> ISNULL(SRC.publish_time, '') OR
                    ISNULL(DST.code, '') <> ISNULL(SRC.code, '') OR
                    ISNULL(DST.product_id, '') <> ISNULL(SRC.product_id, '') OR
                    ISNULL(DST.publish_status, '') <> ISNULL(SRC.publish_status, '')
                )
                THEN
                    UPDATE
                    SET
                    DST.created_time = ISNULL(SRC.created_time, ''),
                    DST.sort_number = ISNULL(SRC.sort_number, ''),
                    DST.updated_by_id = ISNULL(SRC.updated_by_id, ''),
                    DST.slug = ISNULL(SRC.slug, ''),
                    DST.updated_time = ISNULL(SRC.updated_time, ''),
                    DST.description = ISNULL(SRC.description, ''),
                    DST.status = ISNULL(SRC.status, ''),
                    DST.published_by_id = ISNULL(SRC.published_by_id, ''),
                    DST.created_by_id = ISNULL(SRC.created_by_id, ''),
                    DST.publish_uuid = ISNULL(SRC.publish_uuid, ''),
                    DST.title = ISNULL(SRC.title, ''),
                    DST.published_time = ISNULL(SRC.published_time, ''),
                    DST.publish_time = ISNULL(SRC.publish_time, ''),
                    DST.code = ISNULL(SRC.code, ''),
                    DST.product_id = ISNULL(SRC.product_id, ''),
                    DST.publish_status = ISNULL(SRC.publish_status, '')
        WHEN NOT MATCHED
             THEN
                 INSERT (
                            [created_time],
                            [sort_number],
                            [updated_by_id],
                            [slug],
                            [updated_time],
                            [description],
                            [status],
                            [published_by_id],
                            [created_by_id],
                            [publish_uuid],
                            [title],
                            [published_time],
                            [publish_time],
                            [code],
                            [product_id],
                            [publish_status],
                            [id]
                            )
                            VALUES (
                            SRC.created_time,
                            SRC.sort_number,
                            SRC.updated_by_id,
                            SRC.slug,
                            SRC.updated_time,
                            SRC.description,
                            SRC.status,
                            SRC.published_by_id,
                            SRC.created_by_id,
                            SRC.publish_uuid,
                            SRC.title,
                            SRC.published_time,
                            SRC.publish_time,
                            SRC.code,
                            SRC.product_id,
                            SRC.publish_status,
                            SRC.id )

    OUTPUT $ACTION AS action
       ,inserted.id
       ,deleted.id
    INTO #audittemp;


    INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
    VALUES (
            @PipelineName
            ,'tmp.dj_store_productoption'
            ,'stg.dj_store_productoption'
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
                FROM tmp.dj_store_productoption
                )
            ,GETDATE()
            )

            TRUNCATE TABLE #audittemp