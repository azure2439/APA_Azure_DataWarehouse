CREATE procedure [etl].[usp_DJ_Dimension_Integrity_Check] as

/*******************************************************************************************************************************
 Description:   This Stored Procedure checks if there are any Dimension registration or claims values that are in the 
                Incremental Fact tables but not in the Dimension tables, these are the values that did not get pulled 
				from the Lookup tables for each of the dimensions. 
				

Added By:		Morgan Diestler on 4/30/2020				
*******************************************************************************************************************************/


BEGIN

BEGIN TRY

BEGIN TRAN



/************truncate tmp.Fact_AICPExamScores*********************/

--This process will load the fact data for Jobs in the RAW format
/****************************************************************/
truncate table tmp.[Fact_AICPExamScores];

;with cte as
(
select  member_id, max(transdate) as trans_date from stg.FactMember_Current where IsCurrent =1 
group by member_id)

,
cte2 as
(
select distinct A.member_id, SalaryRange, Gender, member_type, category, RaceID, origin, Member_Status, Home_Address_Num as address_num_2, Work_Address_Num as address_num_1, Chapter from stg.FactMember_Current A
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

/************truncate tmp.Fact_Registrations*********************/

--This process will load the fact data for Registrations in the RAW format
/****************************************************************/
truncate table tmp.Fact_Registrations

;with cte as
(
select  member_id, max(transdate) as trans_date from stg.FactMember_Current where IsCurrent =1 
group by member_id)

,
cte2 as
(
select distinct A.member_id, SalaryRange, Gender, member_type, category, RaceID, origin, Member_Status, Home_Address_Num as address_num_2, Work_Address_Num as address_num_1 from stg.FactMember_Current A
inner join cte B on
A.Member_ID = B.Member_ID and A.transdate = B.trans_date
where iscurrent = 1
)

, cte3 as
(
select distinct ID, STATUS  from stg.imis_Subscriptions where IsActive = 1
and PRODUCT_CODE in ('APA', 'STAFF_ONLY')
)

insert into tmp.Fact_Registrations(Seqn, Member_ID, Product_Major,Product_Code, Order_Number, Line_Number, Quantity_Ordered, Quantity_Shipped,
Quantity_Backordered, Unit_Price, Registrant_Class, Registrant_Status, Function_Type, Member_Type, Salary_Range, Race_ID, Origin, Primary_Chapter,
Gender, Status, Category, Member_Status, Address_Num1, Address_Num2, Source_TableName)
select  distinct A.Seqn, isnull(A.ID,''), A.MEETING,case when charindex('/',A.PRODUCT_CODE) > 0 then A.PRODUCT_CODE
else A.meeting +'/'+A.PRODUCT_CODE end as Product_Code , 0, 0, 1, 1, 0, A.UNIT_PRICE, isnull(A.REGISTRANT_CLASS,''), isnull(A.STATUS,'') as Registrant_Status, 'Free', B.Member_Type, B.SalaryRange,
B.RaceID, B.Origin, E.CHAPTER, B.Gender, isnull(D.Status, '') as Status, B.Category, isnull(B.Member_Status,''), E.ADDRESS_NUM_1, E.ADDRESS_NUM_2, 'Custom_Event_Schedule'
from tmp.imis_custom_event_schedule A  
left join 
cte2 B on A.ID = B.Member_ID
left join
stg.dj_store_product C on A.product_code = C.imis_code --and C.publish_status = 'PUBLISHED'
left join 
stg.IMIS_Name E on B.Member_ID = E.ID and E.IsActive = 1
left join 
cte3 D on D.id = B.Member_ID
where c.content_id is null 

union 

select distinct '99999', isnull(F.ST_ID,''), case when charindex('/',A.Product_Code) > 0 then left(A.Product_Code,charindex('/',A.Product_Code)-1) else
A.PRODUCT_CODE end,A.PRODUCT_CODE, A.Order_Number, A.Line_Number, A.Quantity_Ordered, A.Quantity_Shipped, A.Quantity_Backordered, A.UNIT_PRICE, isnull(G.REGISTRANT_CLASS,''), isnull(F.STATUS,'') as Registrant_Status, 'Ticketed', E.Member_Type, B.SalaryRange,
B.RaceID, B.Origin, E.CHAPTER, B.Gender, isnull(D.Status, '') as Status, B.Category, isnull(B.Member_Status,''), E.ADDRESS_NUM_1, E.ADDRESS_NUM_2, 'Order_Lines'
from tmp.imis_order_lines A 
inner join
stg.dj_store_product C on A.product_code = C.imis_code and C.publish_status = 'PUBLISHED'
left join
stg.imis_orders F on A.order_number = F.order_number and F.ISActive = 1
left join 
cte2 B on F.ST_ID = B.Member_ID
left join
stg.imis_order_meet G on A.Order_number = G.Order_Number and G.IsActive  = 1
left join 
stg.IMIS_Name E on B.Member_ID = E.ID and E.IsActive = 1
left join 
cte3 D on D.id = B.Member_ID
where F.ST_ID <> '' 




--  Registrant Class Dimension: Add new Classes into the Registrant Class dimension
Insert into rpt.dimRegistrantClass (Registrant_Class, IsActive)
select distinct Registrant_Class, 1
from  tmp.[fact_registrations] where registrant_class not in
(Select registrant_class  from rpt.dimMemberType where IsActive = 1)



--  Registrant Status Dimension: Add new Status' into the Registrant Status dimension
Insert into rpt.dimRegistrantStatus (Registrant_Status, IsActive)
select distinct Registrant_Status, 1
from  [tmp].[Fact_Registrations] where Registrant_Status not in
(Select Registrant_Status  from rpt.dimRegistrantStatus where IsActive = 1)


--  member_status Dimension: Add new Status' into the Member_Status dimension
Insert into rpt.dimMember_Status (member_status, IsActive)
select distinct member_Status, 1
from  [tmp].[Fact_Registrations] where Member_Status not in
(Select member_Status  from rpt.dimMember_Status where IsActive = 1)



-- Insert any missing members that have data and are in the registrations table
insert into rpt.dimMember (Parent, Member_ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, JoinDate, Cohort, CohortQuarter, 
IsActive)
select distinct 'Unmapped', Member_ID, '', '', '', '', '', '', '', '', '','', '', '', '', '', 1
from [tmp].[Fact_Registrations] where member_id not in
(Select Member_ID  from rpt.dimMember where IsActive = 1) 
UNION 
select distinct 'Unmapped', member_ID, '', '', '', '', '', '', '', '', '','', '', '', '', '', 1
from tmp.fact_AICPExamScores where member_id not in
(Select Member_ID from rpt.dimMember where IsActive = 1) 


--  Product Code Dimension: Add new Product Code's into the Product Code dimension
insert into rpt.dimProductCode (ParentID, Product_Code, Product_Major, Product_Minor, Title, Description, LastUpdatedBy,
LastModified, IsActive, StartDate, EndDate)
select distinct 'Unmapped', Product_Code, case when charindex('/',Product_Code) > 0 then left(Product_Code,charindex('/',Product_Code)-1) else
PRODUCT_CODE end, case when charindex('/',Product_Code) > 0 then right(Product_Code,len(Product_Code) - charindex('/',Product_Code)) else
PRODUCT_CODE end,'','',suser_sname(), getdate(), 1,'1901-01-01','2999-12-31'   from tmp.fact_registrations where Product_Code not in
(Select Product_Code  from rpt.dimProductCode)



/************************************************************************************************************************************/
-- CLAIMS DIMENSIONS MAINTENANCE
/************************************************************************************************************************************/

--  This process will load the raw incremental Claims data

truncate table tmp.Fact_Claims

;with cte as
(
select  member_id, max(transdate) as trans_date from stg.FactMember_Current where IsCurrent =1 
group by member_id)

,
cte2 as
(
select distinct A.member_id, SalaryRange, Gender, Chapter, member_type, category, RaceID, origin, Member_Status, Home_Address_Num as address_num_2, Work_Address_Num as address_num_1 from stg.FactMember_Current A
inner join cte B on
A.Member_ID = B.Member_ID and A.transdate = B.trans_date
where iscurrent = 1
)

, cte3 as
(
select distinct ID, STATUS  from stg.imis_Subscriptions where IsActive = 1
and PRODUCT_CODE in ('APA', 'STAFF_ONLY')
)


Insert into tmp.Fact_Claims
select  A.ID, Z.status, H.Code,
D.Member_ID, isnull(P.code ,'') as Product_Major, isnull(F.code, '') as Product_Minor ,
A.verified, isnull(A.credits,0) as Credits, isnull(A.law_credits,0) as Law_Credits, isnull(A.ethics_credits,0) as ethics_Credits, A.is_speaker, A.is_author,
A.self_reported, A.is_carryover, A.is_pro_bono, 0 as is_deleted, convert(date,A.submitted_time) as Submitted_Time, D.Member_Type, D.SalaryRange,
D.RaceID, D.Origin, D.Chapter,D.Gender, G.Status, D.Category, D.Member_Status, D.address_num_1, D.address_num_2
from tmp.dj_cm_claim A 
left join
stg.dj_cm_log Z on A.log_id = Z.id and Z.IsActive = 1
left join
stg.dj_cm_period H on Z.period_id = H.ID
left join 
stg.dj_myapa_contact B on A.contact_id = B.ID
left join
stg.dj_auth_user C on B.user_id = C.id
left join 
cte2 D on C.username = D.Member_ID 
left join 
stg.dj_events_event  E on E.content_ptr_id = A.event_id
left join
cte3 G on D.Member_ID = G.ID
left join 
stg.dj_content_content F on F.ID =E.Content_ptr_ID 
left join
stg.dj_content_content P on P.master_id = F.parent_id and P.status <> 'X' and (P.publish_status = 'PUBLISHED' or P.publish_status is NULL)

/************************************************************************************************************************************/


-- Claims Flags Dimension Maintenance
insert into rpt.dimClaimsFlags (Verified, IS_Speaker, Is_Author, Self_Reported, Is_CarryOver, Is_Probono, Is_Deleted, IsActive)
			   select distinct verified , Is_Speaker , Is_Author , 
			   Self_Reported , Is_CarryOver , Is_Probono , Is_Deleted, 1   from tmp.fact_claims A
			   where not exists
			   (Select * from rpt.dimClaimsFlags B where  
			    A.verified = B.verified and  A.Is_Speaker = B.Is_Speaker and A.Is_Author = B.Is_Author 
			   and A.Self_Reported = B.Self_Reported and  A.Is_CarryOver = B.Is_CarryOver and A.Is_Probono = B.Is_Probono
			   and A.Is_Deleted = B.Is_Deleted )



--  Claims LogStatus Dimension Maintenance
insert into rpt.dimClaimLogStatus (Log_Status,  IsActive)
			   select distinct Log_Status, 1   from tmp.fact_claims where log_status not in
			   (Select distinct Log_Status from rpt.dimClaimLogStatus)


--  Claims Product Dimension Maintenance
insert into rpt.dimClaimsProduct (ClaimsProduct_Major, ClaimsProduct_Minor, IsActive)
				select distinct product_major, product_minor, 1 from tmp.Fact_Claims A
				where not exists
				(Select * from rpt.dimClaimsProduct B where
				A.Product_Major = B.ClaimsProduct_Major and A.Product_Minor = B.ClaimsProduct_Minor)

--  Updating the descriptions field in Claims Product Dimension
update A set ClaimsProduct_Description = B.Description  from  rpt.dimClaimsProduct  A inner join rpt.dimProductCode B on A.ClaimsProduct_Major = B.Product_Major
and A.ClaimsProduct_Minor = B.Product_Minor 

/************************************************************************************************************************************/


COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END