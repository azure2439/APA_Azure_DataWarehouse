CREATE procedure [etl].[usp_DJ_Job_Fact_Load]
@PipelineName varchar(60) 
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



/************truncate tmp.rptfact_jobs*********************/
truncate table tmp.rptfactjob

/***************Insert Keys into temp rpt from tmp fact**************************/
INSERT INTO [tmp].[rptFactJob] (JobIDKey, JobTypeKey, StatusKey, JobAddressKey, company, job_title, salary_range, PostTimeKey, MemberIDAuthorKey, MemberIDProviderKey)
SELECT j.job_id, JobTypeKey, ContentStatusKey, JobAddressKey, j.company, j.title, j.salary_range, d.Date_Key, ma.memberidkey, mp.memberidkey
FROM [tmp].Fact_Jobs j
INNER JOIN rpt.dimJobType jt on jt.job_type = j.job_type 
							and jt.isactive = 1
INNER JOIN rpt.dimContentStatus cs on cs.status = j.[status]
							and cs.isactive = 1
INNER JOIN rpt.dimJobAddress ja on ja.job_id = j.job_id
							and ja.isactive = 1
INNER JOIN rpt.dimDate d on d.date = CAST(j.post_time AS DATE) OR 
								(d.date is null and j.post_time is null)
-- author
left join rpt.dimMember ma on (ma.member_id = j.memberid_author or (ma.member_id IS NULL and j.memberid_author is NULL))
							and ma.isactive = 1
-- provider
left join rpt.dimMember mp on (mp.member_id = j.memberid_provider or (mp.member_id IS NULL and j.memberid_provider IS NULL))
							and mp.isactive = 1




/**************Merge Tmp to Rpt Fact**********************/

MERGE rpt.tblfactjob as DST
USING tmp.rptfactjob as SRC
on DST.jobidkey = SRC.jobidkey
       WHEN MATCHED
       THEN UPDATE 
       SET DST.JobTypeKey = SRC.JobTypeKey
       ,DST.StatusKey = SRC.StatusKey
	   ,DST.JobAddressKey = SRC.JobAddressKey
	   ,DST.Company = SRC.Company
	   ,DST.Job_Title = SRC.Job_Title
	   ,DST.Salary_Range = SRC.Salary_Range
	   ,DST.PostTimeKey = SRC.PostTimeKey
	   ,DST.MemberIDAuthorKey = SRC.MemberIDAuthorKey
	   ,DST.MemberIDProviderKey = SRC.MemberIDProviderKey

WHEN NOT MATCHED 
then insert (
		JobIDKey
       ,JobTypeKey
       ,StatusKey
	   ,JobAddressKey
	   ,Company
	   ,Job_Title
	   ,Salary_Range
	   ,PostTimeKey
	   ,MemberIDAuthorKey
	   ,MemberIDProviderKey
)
values (
src.JobIDKey, 
src.JobTypeKey,
src.StatusKey, 
src.JobAddressKey,
src.Company,
src.Job_Title,
src.Salary_Range, 
src.PostTimeKey, 
src.MemberIDAuthorKey, 
src.MemberIDProviderKey
)

OUTPUT $ACTION AS action
       ,inserted.jobidkey
       ,deleted.jobidkey
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'JobFact_InitialLoad'
       ,'rpt.tblfactjob'
       ,'tmp.rptfactjob'
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
                           from tmp.rptfactjob
                           ) 
       ,getdate()
       )


                       Truncate TABLE #audittemp




/***************Merge Tmp to Staging Fact - Natural Keys *************************/
MERGE stg.Fact_Jobs AS DST
USING tmp.Fact_Jobs AS SRC
       ON DST.Job_ID = SRC.Job_ID
WHEN MATCHED
             
              AND (
                     
                        ISNULL(DST.City, '') <> ISNULL(SRC.City, '')
                     OR ISNULL(DST.State, '') <> ISNULL(SRC.State, '')
					 OR ISNULL(DST.Country, '') <> ISNULL(SRC.Country, '')
                     OR ISNULL(DST.Job_Type, '') <> ISNULL(SRC.Job_Type, '')
					 OR ISNULL(DST.Company, '') <> ISNULL(SRC.Company, '')
					 OR ISNULL(DST.Salary_Range, '') <> ISNULL(SRC.Salary_Range, '')
					 OR DST.Post_Time <> SRC.Post_Time
					 OR ISNULL(DST.Title, '') <> ISNULL(SRC.Title, '')
					 OR ISNULL(DST.Status, '') <> ISNULL(SRC.Status, '')
					 OR ISNULL(DST.MemberID_Author, '') <> ISNULL(SRC.MemberID_Author, '')
					 OR ISNULL(DST.MemberID_Provider, '') <> ISNULL(SRC.MemberID_Provider, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET 
                      DST.City = ISNULL(SRC.City, '')
					 ,DST.State = ISNULL(SRC.State, '')
					 ,DST.Country = ISNULL(SRC.Country, '')
					 ,DST.Job_Type = ISNULL(SRC.Job_Type, '')
					 ,DST.Company = ISNULL(SRC.Company, '')
					 ,DST.Salary_Range = ISNULL(SRC.Salary_Range, '')
					 ,DST.Post_Time = SRC.Post_Time
					 ,DST.Title = ISNULL(SRC.Title, '')
					 ,DST.Status = ISNULL(SRC.Status, '')
					 ,DST.MemberID_Author = ISNULL(SRC.MemberID_Author, '')
					 ,DST.MemberID_Provider = ISNULL(SRC.MemberID_Provider, '')


WHEN NOT MATCHED
       THEN
              INSERT (
           [Job_ID]
           ,[City]
           ,[State]
           ,[Country]
           ,[Job_Type]
           ,[Company]
           ,[Salary_Range]
           ,[Post_Time]
           ,[Title]
           ,[Status]
           ,[MemberID_Author]
           ,[MemberID_Provider]
           ,[Lastupdated]
           ,[LastModifiedBy]
                     )
              VALUES (
            SRC.[Job_ID]
           ,SRC.[City]
           ,SRC.[State]
           ,SRC.[Country]
           ,SRC.[Job_Type]
           ,SRC.[Company]
           ,SRC.[Salary_Range]
           ,SRC.[Post_Time]
           ,SRC.[Title]
           ,SRC.[Status]
           ,SRC.[MemberID_Author]
           ,SRC.[MemberID_Provider]
           ,SRC.[Lastupdated]
           ,SRC.[LastModifiedBy]
                     )
                     

OUTPUT $ACTION AS action
       ,inserted.job_id
       ,deleted.job_id
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'stg_Fact_Jobss_Initial_Load'
       ,'tmp.Fact_Jobs'
       ,'stg.Fact_Jobs'
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
                           from tmp.Fact_Jobs
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