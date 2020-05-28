CREATE procedure [etl].[usp_DJ_AICPExamScores_Fact_Load]
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



/************truncate tmp.rptfact_aicpexamscores*********************/
truncate table tmp.rptfactaicpexamscores

/***************Insert Keys into temp rpt from tmp fact**************************/
INSERT INTO [tmp].[rptfactaicpexamscores] (
	 [MemberIDKey]
	,[SEQN]
	,[ExamIDKey]
	,[RegistrantTypeIDKey]
	,[MemberTypeIDKey]
	,[ChapterKey]
	,[SalaryIDKey]
	,[RaceIDKey]
	,[StatusKey]
	,[MemberStatusKey]
	,[AddressNum1Key]
	,[AddressNum2Key]
	,[ScaledScore]
	,[RawScore]
	,[Section_AreasOfPractice]
	,[Section_PlanMaking]
	,[Section_PlanKnowledge]
	,[Section_CodeOfEthics]
	,[Section_Leadership]
	,[Pass]
)
SELECT distinct m.MemberIDKey, a.SEQN, e.ExamIDKey, rt.RegistrantTypeIDKey,
mt.MemberTypeIDKey, c.ChapterKey, s.SalaryIDKey, r.RaceIDKey, st.StatusKey,
ms.MemberStatusKey, a1.Address_Key, a2.Address_Key,
a.Scaled_Score, a.Raw_Score,
a.Section_AreasOfPractice, a.Section_PlanMaking, a.Section_PlanKnowledge,
a.Section_CodeOfEthics, a.Section_Leadership, a.Pass

FROM [tmp].Fact_AICPExamScores a
left join rpt.dimMember m on m.member_id = a.member_id and m.isactive = 1
left join rpt.dimMemberType mt on mt.Member_Type = a.Member_Type and mt.IsActive = 1
left join rpt.dimChapter c on c.Chapter_Minor = a.Primary_Chapter and c.IsActive = 1
left join rpt.dimSalary s on s.SalaryID = a.Salary_Range and s.IsActive = 1
left join rpt.dimRace r on r.RaceID = a.Race_ID and  r.Origin = a.Origin and r.IsActive = 1
left join rpt.dimStatus st on st.Status = a.Status and st.Category = a.Category and st.Gender = a.Gender and st.IsActive = 1
left join rpt.dimMemberStatus ms on ms.Member_Status = a.Member_Status and ms.IsActive = 1
left join rpt.dimAddress a1 on a1.Address_Num = a.Address_Num1
left join rpt.dimAddress a2 on a2.Address_Num = a.Address_Num2
left join rpt.dimAICPExam e on e.ExamCode = a.exam_code and e.IsActive = 1
left join rpt.dimAICPExamRegistrantType rt on rt.registrant_type = a.Registrant_Type and rt.IsActive = 1


/**************Merge Tmp to Rpt Fact**********************/

MERGE rpt.tblFactAICPExamScores as DST
USING tmp.rptfactaicpexamscores as SRC
on DST.seqn = SRC.seqn and DST.MemberIDKey = SRC.MemberIDKey
       WHEN MATCHED
       THEN UPDATE 
       SET 
        DST.[ExamIDKey] = SRC.[ExamIDKey]
	   ,DST.[RegistrantTypeIDKey] = SRC.[RegistrantTypeIDKey]
	   ,DST.[MemberTypeIDKey] = SRC.[MemberTypeIDKey]
	   ,DST.[ChapterKey] = SRC.[ChapterKey]
	   ,DST.[SalaryIDKey] = SRC.[SalaryIDKey]
	   ,DST.[RaceIDKey] = SRC.[RaceIDKey]
	   ,DST.[StatusKey] = SRC.[StatusKey]
	   ,DST.[MemberStatusKey] = SRC.[MemberStatusKey]
	   ,DST.[AddressNum1Key] = SRC.[AddressNum1Key]
	   ,DST.[AddressNum2Key] = SRC.[AddressNum2Key]
	   ,DST.[ScaledScore] = SRC.[ScaledScore]
	   ,DST.[RawScore] = SRC.[RawScore]
	   ,DST.[Section_AreasOfPractice] = SRC.[Section_AreasOfPractice]
	   ,DST.[Section_PlanMaking] = SRC.[Section_PlanMaking]
	   ,DST.[Section_PlanKnowledge] = SRC.[Section_PlanKnowledge]
	   ,DST.[Section_CodeOfEthics] =  SRC.[Section_CodeOfEthics]
	   ,DST.[Section_Leadership] = SRC.[Section_Leadership]
	   ,DST.[Pass] = SRC.[Pass]

WHEN NOT MATCHED 
then insert (
		[MemberIDKey]
		,[SEQN]
		,[ExamIDKey]
       ,[RegistrantTypeIDKey]
	   ,[MemberTypeIDKey]
	   ,[ChapterKey]
	   ,[SalaryIDKey]
	   ,[RaceIDKey]
	   ,[StatusKey]
	   ,[MemberStatusKey]
	   ,[AddressNum1Key] 
       ,[AddressNum2Key]
	   ,[ScaledScore]
	   ,[RawScore]
	   ,[Section_AreasOfPractice]
	   ,[Section_PlanMaking]
	   ,[Section_PlanKnowledge]
	   ,[Section_CodeOfEthics]
	   ,[Section_Leadership]
	   ,[Pass])
values (
		SRC.[MemberIDKey]
	   ,SRC.[SEQN]
	   ,SRC.[ExamIDKey] 
       ,SRC.[RegistrantTypeIDKey]
	   ,SRC.[MemberTypeIDKey]
	   ,SRC.[ChapterKey]
	   ,SRC.[SalaryIDKey]
	   ,SRC.[RaceIDKey]
	   ,SRC.[StatusKey]
	   ,SRC.[MemberStatusKey]
	   ,SRC.[AddressNum1Key] 
	   ,SRC.[AddressNum2Key]
	   ,SRC.[ScaledScore]
	   ,SRC.[RawScore]
	   ,SRC.[Section_AreasOfPractice]
	   ,SRC.[Section_PlanMaking]
	   ,SRC.[Section_PlanKnowledge]
	   ,SRC.[Section_CodeOfEthics]
	   ,SRC.[Section_Leadership]
	   ,SRC.[Pass]
)

OUTPUT $ACTION AS action
       ,inserted.seqn
       ,deleted.seqn
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'AICPExamFact_InitialLoad'
       ,'rpt.tblfactaicpexamscores'
       ,'tmp.rptfactaicpexamscores'
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
                           from tmp.rptfactaicpexamscores
                           ) 
       ,getdate()
       )


                       Truncate TABLE #audittemp




/***************Merge Tmp to Staging Fact - Natural Keys *************************/
MERGE stg.Fact_AICPExamScores AS DST
USING tmp.Fact_AICPExamScores AS SRC
       ON DST.Member_ID = SRC.Member_ID and DST.SEQN = SRC.SEQN
WHEN MATCHED 
	AND (            
			ISNULL(DST.Exam_Code, '') <> ISNULL(SRC.Exam_Code, '')
			OR DST.Exam_Date <> SRC.Exam_Date
			OR ISNULL(DST.Pass, 0) <> ISNULL(SRC.Pass, 0)
			OR ISNULL(DST.Scaled_Score, 0) <> ISNULL(SRC.Scaled_Score, 0)
			OR ISNULL(DST.Raw_Score, 0) <> ISNULL(SRC.Raw_Score, 0)
			OR ISNULL(DST.Section_AreasOfPractice, 0) <> ISNULL(SRC.Section_AreasOfPractice, 0)
			OR ISNULL(DST.Section_PlanMaking, 0) <> ISNULL(SRC.Section_PlanMaking, 0)
			OR ISNULL(DST.Section_PlanKnowledge, 0) <> ISNULL(SRC.Section_PlanKnowledge, 0)
			OR ISNULL(DST.Section_CodeOfEthics, 0) <> ISNULL(SRC.Section_CodeOfEthics, 0)
			OR ISNULL(DST.Section_Leadership, 0) <> ISNULL(SRC.Section_Leadership, 0)		 
			OR ISNULL(DST.Registrant_Type, '') <> ISNULL(SRC.Registrant_Type, '')
			OR ISNULL(DST.Gee_Eligibility_ID, '') <> ISNULL(SRC.Gee_Eligibility_ID, '')
			OR ISNULL(DST.Member_Type, '') <> ISNULL(SRC.Member_Type, '')
			OR ISNULL(DST.Race_ID, '') <> ISNULL(SRC.Race_ID, '')
			OR ISNULL(DST.Origin, '') <> ISNULL(SRC.Origin, '')
			OR ISNULL(DST.Gender, '') <> ISNULL(SRC.Gender, '')     
			OR ISNULL(DST.Primary_Chapter, '') <> ISNULL(SRC.Primary_Chapter, '')
			OR ISNULL(DST.Status, '') <> ISNULL(SRC.Status, '')
			OR ISNULL(DST.Category, '') <> ISNULL(SRC.Category, '')
			OR ISNULL(DST.Member_Status, '') <> ISNULL(SRC.Member_Status, '') 
			OR ISNULL(DST.Salary_Range, '') <> ISNULL(SRC.Salary_Range, '')
			OR ISNULL(DST.Address_Num1, '') <> ISNULL(SRC.Address_Num1, '')
			OR ISNULL(DST.Address_Num2, '') <> ISNULL(SRC.Address_Num2, '')
			)

	-- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
	THEN
		UPDATE
		SET 
		DST.Pass = ISNULL(SRC.Pass, 0),
		DST.Scaled_Score = ISNULL(SRC.Scaled_Score, 0),
		DST.Raw_Score = ISNULL(SRC.Raw_Score, 0),
		DST.Section_AreasOfPractice = ISNULL(SRC.Section_AreasOfPractice, 0),
		DST.Section_PlanMaking = ISNULL(SRC.Section_PlanMaking, 0),
		DST.Section_PlanKnowledge = ISNULL(SRC.Section_PlanKnowledge, 0),
		DST.Section_CodeOfEthics = ISNULL(SRC.Section_CodeOfEthics, 0),
		DST.Section_Leadership =  ISNULL(SRC.Section_Leadership, 0), 
		DST.Registrant_Type = ISNULL(SRC.Registrant_Type, ''),
		DST.Gee_Eligibility_ID = ISNULL(SRC.Gee_Eligibility_ID, ''),
		DST.Member_Type =  ISNULL(SRC.Member_Type, ''),
		DST.Race_ID = ISNULL(SRC.Race_ID, ''),
		DST.Origin = ISNULL(SRC.Origin, ''),
		DST.Gender = ISNULL(SRC.Gender, ''),    
		DST.Primary_Chapter =  ISNULL(SRC.Primary_Chapter, ''),
		DST.Status = ISNULL(SRC.Status, ''),
		DST.Category = ISNULL(SRC.Category, ''),
		DST.Member_Status = ISNULL(SRC.Member_Status, ''),
		DST.Salary_Range = ISNULL(SRC.Salary_Range, ''),
		DST.Address_Num1 = ISNULL(SRC.Address_Num1, ''),
		DST.Address_Num2 = ISNULL(SRC.Address_Num2, '')
			
WHEN NOT MATCHED
       THEN
              INSERT (
			[Member_ID]
           ,[Seqn]
           ,[Exam_Code]
           ,[Exam_Date]
           ,[Pass]
           ,[Scaled_Score]
           ,[Raw_Score]
           ,[Section_AreasOfPractice]
           ,[Section_PlanMaking]
           ,[Section_PlanKnowledge]
           ,[Section_CodeOfEthics]
           ,[Section_Leadership]
           ,[Registrant_Type]
           ,[Gee_Eligibility_ID]
           ,[Member_Type]
           ,[Race_ID]
           ,[Origin]
           ,[Gender]
           ,[Primary_Chapter]
           ,[Status]
           ,[Category]
           ,[Member_Status]
           ,[Salary_Range]
           ,[Address_Num1]
           ,[Address_Num2]
           ,[LastUpdatedBy]
           ,[LastModified]
                     )
              VALUES (
			[Member_ID]
			,[Seqn]
			,[Exam_Code]
			,[Exam_Date]
			,[Pass]
			,[Scaled_Score]
			,[Raw_Score]
			,[Section_AreasOfPractice]
			,[Section_PlanMaking]
			,[Section_PlanKnowledge]
			,[Section_CodeOfEthics]
			,[Section_Leadership]
			,[Registrant_Type]
			,[Gee_Eligibility_ID]
			,[Member_Type]
			,[Race_ID]
			,[Origin]
			,[Gender]
			,[Primary_Chapter]
			,[Status]
			,[Category]
			,[Member_Status]
			,[Salary_Range]
			,[Address_Num1]
			,[Address_Num2]
			,[LastupdatedBy]
			,[LastModified]
                     )
                     

OUTPUT $ACTION AS action
       ,inserted.[Seqn]
       ,deleted.[Seqn]
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'stg_Fact_AICPExamScores_Initial_Load'
       ,'tmp.Fact_AICPExamScores'
       ,'stg.Fact_AICPExamScores'
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
                           from tmp.Fact_AICPExamScores
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