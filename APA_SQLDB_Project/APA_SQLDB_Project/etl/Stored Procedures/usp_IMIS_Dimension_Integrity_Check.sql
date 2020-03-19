



CREATE PROCEDURE [etl].[usp_IMIS_Dimension_Integrity_Check] 
as

/*******************************************************************************************************************************
 Description:   This Stored Procedure checks if there are any Dimension member values that are in the Incremental Fact tables
				but not in the Dimension tables, these are the values that did not get pulled from the Lookup tables for each of
				the dimensions. 
				

Added By:		Ajay Punyapu on 2/27/2020				
*******************************************************************************************************************************/
BEGIN

BEGIN TRY

BEGIN TRAN

--  Truncate the temporary member fact and changed members table
truncate table tmp.FactMember_Current
truncate table tmp.ChangedMemberID


--  Insert all new Member IDs resulted from the incremental load refresh of some of the main tables of IMIS
Insert into tmp.ChangedMemberID
Select distinct ID from  tmp.imis_Name
union
Select distinct ID from tmp.imis_Activity 
union 
--Select distinct BT_ID from tmp.imis_Trans 
--union 
Select distinct ID from tmp.imis_Ind_Demographics 
union 
Select distinct ID from tmp.imis_Org_Demographics 
union 
Select distinct ID from tmp.imis_Race_Origin 
union 
Select distinct ID from tmp.imis_Subscriptions





--  Construct data in temporary Fact Table format with natural Keys 
insert into tmp.FactMember_Current (Member_ID,Member_Type, SalaryRange, RaceID, Origin, Chapter, TransDate, Paid_Thru_Date, 
Home_Address_Num, Work_Address_Num, Status, Gender, Category, Org_Type, Member_Status, Product_Code, Prod_Type, Source_Code, Complimentary)
select distinct  A.ID, isnull(A.MEMBER_TYPE,'') as Member_Type, isnull(B.SALARY_RANGE,'') as SalaryRange, case when C.Race = 'NO_ANSWER' then 'NO_ANSWER'
												   when len(C.RACE) > 4 then 'MultipleSelection'
												   else isnull(C.Race,'') End as Race
    	, isnull(C.ORIGIN,'') as Origin, isnull(A.CHAPTER,'') as Chapter, getdate() as TransDate,S.PAID_THRU as PaidThru, A.ADDRESS_NUM_2  as Home_Address, A.ADDRESS_NUM_1 as WorkAddress, 
		isnull(S.Status,'') as Status, isnull(A.GENDER,'') as Gender,  isnull(A.CATEGORY,'') as Category, isnull(O.ORG_TYPE,'') as OrgType, 
		isnull(A.Member_Status,'')as MemberStatus, isnull(S.Product_Code,'') as Product_Code, isnull(S.Prod_Type,'') as Prod_Type, isnull(S.Source_Code,'') as Source_Code, isnull(S.Complimentary, 0) as Complimentary from tmp.ChangedMemberID  T 
left join  stg.imis_Name A 
on T.member_ID = A.ID and A.IsActive = 1 
left join stg.imis_ind_demographics B 
on A.ID = B.ID and B.IsActive = 1 
left join stg.imis_Race_Origin C 
on A.ID = C.ID and C.IsActive  = 1 
left join stg.imis_ORG_Demographics O 
on A.ID = O.ID and O.IsActive = 1 
left join stg.imis_Subscriptions S 
on A.ID = S.ID and S.IsActive = 1 --and product_code = case when A.Member_Type  in ('ALUM','FCLTI','FCLTS','GPBM','LIFE','MEM','NP','PBM','RET','STU') then  'APA'
--									   when A.MEMBER_TYPE = 'FSTU' then 'FREE_APA' 
--									   end	



--  Date Dimension: Add Missing Paid Through Dates not in the Date dimension
insert into rpt.dimDate (date, day, fiscalmonth, FiscalQuarter, fiscalyear, calendarmonth, calendarquarter, calendaryear)
select distinct convert(date,Paid_Thru_Date) as Date, datepart(day,Paid_Thru_Date) as Day,datepart(MM,dateadd(MM,3,Paid_Thru_Date)) as FiscalMonth ,case when datepart(MM,Paid_Thru_Date) in 
(10,11,12) then 1
when datepart(MM,Paid_Thru_Date) in 
(1,2, 3) then 2
when datepart(MM,Paid_Thru_Date) in 
(4,5,6) then 3
when datepart(MM,Paid_Thru_Date) in 
(7,8,9) then 4
else NULL end as FiscalQuarter, datepart(YEar, case when datepart(MM,Paid_Thru_Date) in 
(10,11,12) then dateadd(year,1,Paid_Thru_Date) else Paid_Thru_Date end) as FiscalYear ,datepart(MM,Paid_Thru_Date) as CalendarMonth,
datepart(qq,Paid_Thru_Date) as CalendarQuarter, datepart(Year,Paid_Thru_Date) as CalendarYear  from tmp.factmember_current A
where not exists
(select distinct date from rpt.dimDate B
where convert(date,Paid_Thru_Date) = B.date) 
and paid_thru_date is not null
union
select distinct convert(date,Thru_Date) as Date, datepart(day,Thru_Date) as Day,datepart(MM,dateadd(MM,3,Thru_Date)) as FiscalMonth ,case when datepart(MM,Thru_Date) in 
(10,11,12) then 1
when datepart(MM,Thru_Date) in 
(1,2, 3) then 2
when datepart(MM,Thru_Date) in 
(4,5,6) then 3
when datepart(MM,Thru_Date) in 
(7,8,9) then 4
else NULL end as FiscalQuarter, datepart(YEar, case when datepart(MM,Thru_Date) in 
(10,11,12) then dateadd(year,1,Thru_Date) else Thru_Date end) as FiscalYear ,datepart(MM,Thru_Date) as CalendarMonth,
datepart(qq,Thru_Date) as CalendarQuarter, datepart(Year,Thru_Date) as CalendarYear  from tmp.imis_activity A
where not exists
(select distinct date from rpt.dimDate B
where convert(date,Thru_Date) = B.date)
and thru_date is not null
--order by convert(date,Paid_Thru_Date)

--  Member Dimension: Add new members into the Member dimension
insert into rpt.dimMember (Parent, Member_ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, JoinDate, Cohort, CohortQuarter, 
IsActive)
select distinct ID, ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, Join_Date, 'newcohort', 'newcohortquarter', 
1
from stg.imis_name where id in
((select distinct member_id from tmp.FactMember_Current where member_ID not in
(Select Member_ID  from rpt.dimMember where IsActive = 1))
union 
 (select distinct id from tmp.imis_activity where ID not in
(Select Member_ID  from rpt.dimMember where IsActive = 1)))

-- Insert any missing members that had data but are not in the Name table
insert into rpt.dimMember (Parent, Member_ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, JoinDate, Cohort, CohortQuarter, 
IsActive)
select distinct 'Unmapped', Member_ID, '', '', '', '', '', '', '', '', '','', '', '', '0', '0', 1
from tmp.FactMember_Current where member_id not in
(Select Member_ID  from rpt.dimMember where IsActive = 1) 
union
select distinct 'Unmapped', ID, '', '', '', '', '', '', '', '', '','', '', '', '0', '0', 1
from tmp.imis_Activity where ID not in
(Select Member_ID  from rpt.dimMember where IsActive = 1) 



--  Update Cohort values for the new members only
update A set A.cohort =
case when 
B.member_type = 'MEM' and MEMBER_STATUS = 'N' and CATEGORY = 'NM1' then (case when month(B.Join_Date) < 10 then year(join_date) when month(B.Join_Date) >= 10 then  year(join_date)+1 else '0' end)
when B.member_type = 'STU' and MEMBER_STATUS = 'N'  then (case when month(B.Join_Date) < 10 then year(join_date) when month(B.Join_Date) >= 10 then  year(join_date)+1 else '0' end)
else '0'
end
from rpt.dimmember A join stg.imis_name B on
A.member_id = B.ID
where cohort = 'newcohort'


--  Update Cohort Quarter values for the new members only
update A set A.cohortquarter =
case when 
B.member_type = 'MEM' and MEMBER_STATUS = 'N' and CATEGORY = 'NM1' then (case when month(B.Join_Date) between 10 and 12 then 'Q1' when month(B.Join_Date) between 1 and 3 then 'Q2' when month(B.Join_Date) between 4 and 6 then 'Q3' when month(B.Join_Date) between 7 and 9 then 'Q4' else '0' end)
when B.member_type = 'STU' and MEMBER_STATUS = 'N'  then (case when month(B.Join_Date) between 10 and 12 then 'Q1' when month(B.Join_Date) between 1 and 3 then 'Q2' when month(B.Join_Date) between 4 and 6 then 'Q3' when month(B.Join_Date) between 7 and 9 then 'Q4' else '0' end)
else '0'
end
from rpt.dimmember A join stg.imis_name B on
A.member_id = B.ID
where cohortquarter = 'newcohortquarter'


--  Member Type Dimension: Add new member types into the Member Type dimension
Insert into rpt.dimMemberType (Member_Type, Description, IsActive, Member_Record, Company_Record, Dues_Code_1)
select distinct Member_Type, 'Unmapped', 1, '', '', ''
from  tmp.[factmember_current] where Member_Type not in
(Select Member_Type  from rpt.dimMemberType where IsActive = 1)
union
(Select distinct Member_Type, 'Unmapped', 1, '', '', ''
  from tmp.imis_activity where member_type not in
 (Select Member_Type  from rpt.dimMemberType where  IsActive = 1))


--  Salary Dimension: Add new Salary types into the Salary Type dimension
Insert into rpt.dimSalary (SalaryID, Description, IsActive)
select distinct SalaryRange, 'Unmapped', 1
from  [tmp].[FactMember_Current] where SalaryRange not in
(Select SalaryID  from rpt.dimSalary where IsActive = 1)



--  Chapter Dimension: Add new Chapter types into the Chapter dimension
Insert into rpt.dimchapter (Chapter_Code, Chapter_Minor, Chapter_Major, Description, IsActive)
select distinct 'Unmapped', Chapter, 'CHAPT', 'Unmapped', 1
 from  [tmp].[FactMember_Current] where Chapter not in
(Select Chapter_Minor  from rpt.dimChapter where IsActive = 1)


--  Address Dimension: Add new Addresses into the Address dimension
insert into rpt.dimAddress (Address_Num, Purpose, Company, FullAddress, Street, City, State, country, Zip, County, IsActive)
select distinct Address_Num, Purpose, Company, Full_Address, Address_1, City, State_Province, country, Zip, County, 1
 from stg.imis_name_address where address_num in 
 (select distinct home_address_num 
 from  [tmp].[factmember_current] where Home_Address_Num not in
(Select Address_Num  from rpt.dimAddress where IsActive = 1))

insert into rpt.dimAddress (Address_Num, Purpose, Company, FullAddress, Street, City, State, country, Zip, County, IsActive)
select distinct Address_Num, Purpose, Company, Full_Address, Address_1, City, State_Province, country, Zip, County, 1
 from stg.imis_Name_Address where address_num in
 (select distinct work_address_num
 from  [tmp].[factmember_current] where Work_Address_Num not in
(Select Address_Num  from rpt.dimAddress where IsActive = 1))


--  Status Dimension: Add new Status's into the Status dimension
insert into rpt.dimStatus (Status, Gender, Category, IsActive)
select distinct status, Gender, Category, 1
 from  [tmp].[FactMember_Current] where 
 not exists 
 (select a.status, a.gender, a.category from rpt.dimStatus a, tmp.factmember_current b
 where a.status = b.status and a.gender = b.gender and a.category = b.category)
 and Gender not in
(Select distinct Gender  from rpt.dimStatus where IsActive = 1)

--  Org Dimension: Add new Org Code's into the Org dimension
insert into rpt.dimOrg (OrgCode, Description, IsActive)
select distinct Org_Type, 'Unmapped', 1
 from  [tmp].[FactMember_Current] where Org_Type not in
(Select OrgCode  from rpt.dimOrg where IsActive = 1)


--  Member Status Dimension: Add new Member Status's into the Member Status dimension
insert into rpt.dimMemberStatus (Member_Status, description, IsActive)
select distinct Member_Status, 'Unmapped', 1
 from  [tmp].[FactMember_Current] where Member_Status not in
(Select member_status  from rpt.dimMemberStatus where IsActive = 1) 



--  Product Code Dimension: Add new Product Code's into the Product Code dimension
insert into rpt.dimProductCode (ParentID, Product_Code, Product_Major, Product_Minor, title, Description, IsActive)
select distinct Product_Code, Product_Code, 'Unmapped', 'Unmapped', 'Unmapped', 'Unmapped', 1
 from  [tmp].[FactMember_current] where Product_Code not in
(Select Product_Code  from rpt.dimProductCode where IsActive = 1)
union
(Select distinct Product_Code, Product_Code, 'Unmapped', 'Unmapped', 'Unmapped', 'Unmapped', 1
  from tmp.imis_activity where product_code not in
 (Select Product_Code  from rpt.dimProductCode where  IsActive = 1))


--  Product Type Dimension: Add new product types into the Product Type dimension
insert into rpt.dimProductType (ParentID, Product_Type, Description, IsActive)
select distinct 'Unmapped', Prod_Type, 'Unmapped', 1
 from  [tmp].[FactMember_Current] where Prod_Type not in
(Select Product_Type  from rpt.dimProductType where IsActive = 1)
union
(Select distinct 'Unmapped', Activity_Type, 'Unmapped', 1
  from tmp.imis_activity where activity_type not in
 (Select Product_Type  from rpt.dimProductType where  IsActive = 1))

--  Race Dimension: Add new Race and Origin into the Race dimension
;with cte1 as
(Select distinct RaceID, Ethnicity, custom_rollup from rpt.dimRace )

Insert into rpt.dimRace(RaceID, Ethnicity, Custom_Rollup, Origin, OriginDescription)
select distinct B.RaceID, case when  exists (select RaceID  from  rpt.DimRace A where A.RaceID = B.RaceID)  then C.Ethnicity else  'Unmapped' end as Ethnicity,
case when  exists (select RaceID  from  rpt.DimRace A where A.RaceID = B.RaceID)  then C.Custom_Rollup else  'Unmapped' end as Custom_Rollup,
B.Origin,
'Unmapped' as OriginDescription
from tmp.FactMember_Current B left join cte1 C on  B.RaceID = C.RaceID 
where
B.Origin not in
(Select Origin from rpt.dimRace)


Insert into rpt.dimRace (RaceID, Ethnicity, Custom_Rollup, Origin, OriginDescription)
Select distinct A.RaceID, 'Unmapped','Unmapped',A.Origin,'Unmapped' as OriginDescription from tmp.FactMember_Current A where A.RaceID not in
(Select RaceID from rpt.dimRace)

/***********************************************************************************************************************/
--	Dimensions related to Activity Fact Table 
/***********************************************************************************************************************/

--	Load new values from the incremental fact load that are not in the Dimension tables

--  Activity Category Dimension: Add new Activity Categorie's into the Actvity Category dimension
Insert into rpt.dimActivityCategory (Activity_Category_Code, Description, IsActive)
select distinct Category, 'Unmapped', 1
from  [tmp].[imis_activity] where Category not in
(Select Activity_Category_Code  from rpt.dimActivityCategory where IsActive = 1)

--  Campaign Code Dimension: Add new Campaign Codes into the Campaign Code dimension
Insert into rpt.dimCampaignCode (ParentID, Campaign_Code, Description, IsActive)
select distinct 'Unmapped', Campaign_Code, 'Unmapped', 1
from  [tmp].[imis_activity] where Campaign_Code not in
(Select Campaign_Code  from rpt.dimCampaignCode where IsActive = 1)

--  Source Dimension: Add new Source's into the Source dimension
Insert into rpt.dimSource (ParentID, Source_System_Code, Description, IsActive)
select distinct 'Unmapped', Source_System, 'Unmapped', 1
from  [tmp].[imis_activity] where Source_System not in
(Select Source_System_Code  from rpt.dimSource where IsActive = 1)


--  Source Code Dimension: Add new Source Code's into the Source Code dimension
Insert into rpt.dimSourceCode (ParentID, Source_Code, Description, IsActive)
select distinct 'Unmapped', Source_Code, Description, 1
from  [tmp].[imis_activity] where Source_Code not in
(Select Source_Code  from rpt.dimSourceCode where IsActive = 1)

COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END


