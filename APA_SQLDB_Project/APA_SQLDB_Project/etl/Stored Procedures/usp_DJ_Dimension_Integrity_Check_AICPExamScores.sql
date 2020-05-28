
CREATE   procedure [etl].[usp_DJ_Dimension_Integrity_Check_AICPExamScores] as

BEGIN

BEGIN TRY

BEGIN TRAN

/*
truncate table rpt.dimAICPExam;
truncate table rpt.dimAICPExamRegistrantType;
truncate table rpt.dimAICPExamScore;

select count(*) from tmp.Fact_AICPExamScores;
select count(*) from rpt.dimAICPExam;
select count(*) from rpt.dimAICPExamRegistrantType;
select count(*) from rpt.dimAICPExamScore;
*/



/************truncate tmp.Fact_AICPExamScores*********************/

--This process will load the fact data for Jobs in the RAW format
/****************************************************************/
truncate table tmp.[Fact_AICPExamScores];

with cte as
(
select  member_id, max(transdate) as trans_date from stg.FactMember_Current where IsCurrent =1 
group by member_id)

,
cte2 as
(
select distinct A.member_id, SalaryRange, Gender, member_type, category, RaceID, origin, Member_Status, Chapter, Status, Home_Address_Num as address_num_1, Work_Address_Num as address_num_2 from stg.FactMember_Current A
inner join cte B on
A.Member_ID = B.Member_ID and A.transdate = B.trans_date
where iscurrent = 1
)

, cte3 as
(
select distinct ID, STATUS  from stg.imis_Subscriptions where IsActive = 1
and PRODUCT_CODE in ('APA', 'STAFF_ONLY')
)

/**********************load tmp.fact*****************/

-- removed birth_date and designation
insert into [tmp].[Fact_AICPExamScores] (
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
           ,[Address_Num1]
           ,[Address_Num2]
		   ,[Salary_Range]
)

SELECT DISTINCT S.ID, SEQN, EXAM_CODE, EXAM_DATE, PASS, SCALED_SCORE,
RAW_SCORE, SCORE_1, SCORE_2, SCORE_3, SCORE_4, SCORE_5,
REGISTRANT_TYPE, GEE_ELIGIBILITY_ID, 
ISNULL(B.Member_Type,''),
ISNULL(B.RaceID,''),
ISNULL(B.ORIGIN,''),
ISNULL(B.GENDER,''),
ISNULL(B.CHAPTER,''),
ISNULL(A.STATUS,''), -- status for the APA product
ISNULL(B.CATEGORY,''),
ISNULL(B.Member_Status,''), 
ISNULL(B.Address_Num_1,''),
ISNULL(B.Address_Num_2,''),
ISNULL(B.SalaryRange, '')
FROM tmp.imis_Custom_AICP_Exam_Score S

-- IS THIS JOIN OKAY TO GET MEMBER DATA?
LEFT JOIN cte3 A on A.id = S.ID -- apa free_apa subscription
LEFT JOIN cte2 B on B.Member_ID = S.ID

/* ********************************* 
  INSERT RECORDS INTO DIMENSIONS
  ********************************** */


-- Insert new exam_codes into the rpt.dimAICPExam
insert into rpt.[dimAICPExam] (
	[ExamCode]
	,[Title]
	,[Status]
	,[ExamStartDate]
	,[ExamEndDate]
	,[RegistrationStartDate]
	,[RegistrationEndDate]
	,[ApplicationStartDate]
	,[ApplicationEndDate]
	,[IsAdvanced]
)
SELECT code, title, status, start_time, end_time,
registration_start_time, registration_end_time,
application_start_time, application_end_time,
is_advanced
FROM tmp.dj_exam_exam;

-- insert unknown exam codes into rpt.dimAICPExam
insert into rpt.dimAICPExam (
ExamCode, Title
)
SELECT DISTINCT EXAM_CODE, 'Unmapped'
FROM tmp.imis_Custom_AICP_Exam_Score
WHERE EXAM_CODE NOT IN (
	SELECT ExamCode
	FROM rpt.dimAICPExam
)

-- insert unknown registration types into rpt.dimAICPExamRegistrationType
insert into rpt.dimAICPExamRegistrantType (
Registrant_Type, Title
)
SELECT DISTINCT REGISTRANT_TYPE, 'Unmapped'
FROM tmp.imis_Custom_AICP_Exam_Score
WHERE REGISTRANT_TYPE NOT IN (
	SELECT DISTINCT REGISTRANT_TYPE
	FROM rpt.dimAICPExamRegistrantType
)

-- insert new exam results into rpt.dimAICPExamScore
/*
insert into rpt.dimAICPExamScore (
	[Member_ID],
	[SEQN],
	[ExamCode]
	,[ExamDate]
	,[Pass]
	,[ScaledScore]
	,[RawScore]
	,[Section_AreasOfPractice]
	,[Section_PlanMaking]
	,[Section_PlanKnowledge]
	,[Section_CodeOfEthics]
	,[Section_Leadership]
	,[TestFormCode]
	,[TestCenter]
	,[File_Name]
	,[Gee_Eligibility_ID]
)
SELECT ID, SEQN, EXAM_CODE, EXAM_DATE, PASS, SCALED_SCORE, RAW_SCORE,
SCORE_1, SCORE_2, SCORE_3, SCORE_4, SCORE_5, TESTFORM_CODE, 
TEST_CENTER, [FILE_NAME], GEE_ELIGIBILITY_ID
FROM tmp.imis_Custom_AICP_Exam_Score
WHERE SEQN NOT IN (SELECT SEQN FROM rpt.dimAICPExamScore) -- this is needed to exclude records already imported? 
*/

 -- Insert any missing members that have data and are in the registrations table
insert into rpt.dimMember (Parent, Member_ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, JoinDate, Cohort, CohortQuarter, 
IsActive)
select distinct 'Unmapped', member_ID, '', '', '', '', '', '', '', '', '','', '', '', '', '', 1
from tmp.fact_AICPExamScores where member_id not in
(Select Member_ID from rpt.dimMember where IsActive = 1) 







COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END