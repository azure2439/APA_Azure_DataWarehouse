


/*******************************************************************************************************************************
 Description:   This Stored Procedure loads active new or updated records into the member fact table. Records are loaded from the 
				the audit fact table.
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE PROCEDURE [etl].[usp_IMIS_Audit_Fact_Update_Insert]
@PipelineName varchar(60) = 'ssms'
as

BEGIN

BEGIN TRY

BEGIN TRAN
 
 --- Drop and create audit fact table. This table is used as the source for inserting new and updated records into the fact table
 IF OBJECT_ID('tempdb..#auditfact') IS NOT NULL
Drop table #auditfact

 Create table #auditfact
 (  [Action] [varchar] (20),
    [Member_ID] [varchar](10) ,
	[Member_Type] [varchar](5) ,
	[SalaryRange] [varchar](5) ,
	[RaceID] [varchar](60) ,
	[Origin] [varchar](60) ,
	[Chapter] [varchar](15) ,
	[TransDate] [datetime] ,
	[Paid_Thru_Date] [datetime] ,
	[Home_Address_Num] [int] ,
	[Work_Address_Num] [int] ,
	[Status] [varchar](5) ,
	[Gender] [varchar](1) ,
	[Category] [varchar](5) ,
	[Org_Type] [varchar](60) ,
	[Member_Status] [varchar](10) ,
	[Product_Code] [varchar](31) ,
	[Prod_Type] [varchar](10) ,
	[Source_Code] [varchar](40),
	[Complimentary] bit,
	[IsCurrent] smallint
)



truncate table tmp.rptfact



--- Using this merge statement, we are able to set old records to IsCurrent = 0 if the record is updated. This allows for
--- historical tracking of members. Within this merge statement, we are also setting new and updated records to IsCurrent = 1.
; with cte as
	(Select * from stg.FactMember_Current where IsCurrent = 1)

	merge cte as DST
	using tmp.FactMember_Current  as SRC
	on (DST.Member_ID = SRC.Member_ID
	and DST.Product_Code = SRC.Product_Code)
	when matched and
	isnull(SRC.Member_Type,'') <> isnull(DST.Member_Type,'')
OR	isnull(SRC.SalaryRange,'') <> isnull(DST.SalaryRange,'') 
OR	isnull(SRC.RaceID,'') <> isnull(DST.RaceID,'') 
OR	isnull(SRC.Origin,'') <> isnull(DST.Origin,'') 
OR	isnull(SRC.Chapter,'') <> isnull(DST.Chapter,'') 
OR	isnull(SRC.Paid_Thru_Date,'') <> isnull(DST.Paid_Thru_Date,'') 
OR	isnull(SRC.Home_Address_Num,'') <> isnull(DST.Home_Address_Num,'') 
OR	isnull(SRC.Work_Address_Num,'') <> isnull(DST.Work_Address_Num,'') 
OR	isnull(SRC.Status,'') <> isnull(DST.Status,'') 
OR	isnull(SRC.Gender,'') <> isnull(DST.Gender,'') 
OR	isnull(SRC.Category,'') <> isnull(DST.Category,'') 
OR	isnull(SRC.Org_Type,'') <> isnull(DST.Org_Type,'') 
OR  isnull(SRC.Prod_Type,'') <> isnull(DST.Prod_Type,'') 
OR  isnull(SRC.Source_Code,'') <> isnull(DST.Source_Code,'')
OR  isnull(SRC.Complimentary,'') <> isnull(DST.Complimentary,'')
	Then Update  SET
	DST.IsCurrent= 0
     when not matched 
	 THEN INSERT (Member_ID,Member_Type, SalaryRange, RaceID, Origin, Chapter, TransDate, Paid_Thru_Date, Home_Address_Num, 
	 Work_Address_Num, Status, Gender, Category, Org_Type, Member_Status,Product_Code, Prod_Type, Source_Code, Complimentary,IsCurrent) 
	 values ( SRC.Member_ID, SRC.Member_Type, SRC.SalaryRange, SRC.RaceID, SRC.Origin, SRC.Chapter, SRC.TransDate, 
	 SRC.Paid_Thru_Date,SRC.Home_Address_Num, SRC.Work_Address_Num, SRC.Status, SRC.Gender, SRC.Category, SRC.Org_Type, 
	 SRC.Member_Status, SRC.Product_Code, SRC.Prod_Type, SRC.Source_Code, SRC.Complimentary,1)

---all inserted records are then pushed to the audit fact table
Output $Action Action, inserted.*
into #auditfact


---all updated records are pushed to the staging fact table
; Insert into stg.FactMember_Current (Member_ID,Member_Type, SalaryRange, RaceID, Origin, Chapter, TransDate, Paid_Thru_Date, Home_Address_Num, Work_Address_Num, Status,
     Gender, Category, Org_Type, Member_Status,Product_Code, Prod_Type, Source_Code, Complimentary, IsCurrent)
Select distinct  A.Member_ID, A.Member_Type, A.SalaryRange, A.RaceID, A.Origin, A.Chapter, A.TransDate, A.Paid_Thru_Date, A.Home_Address_Num, A.Work_Address_Num, A.Status,
     A.Gender, A.Category, A.Org_Type, A.Member_Status,A.Product_Code, A.Prod_Type, A.Source_Code, A.Complimentary, 1
       from tmp.FactMember_Current A inner join #auditfact B on
       A.Member_ID = B.Member_ID and A.Product_Code = B.Product_code
       where B.Action = 'Update'



---insert counts into etl.executionlog to keep track of how many inserts vs. updated records came in
INSERT INTO etl.executionlog
VALUES (
        '@PipelineName'
	   ,'tmp.FactMember_Current'
       ,'stg.FactMember_Current'
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #auditfact
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #auditfact
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
			 ,(select count(*) as RowsRead
				from tmp.FactMember_Current
				) 
       ,getdate()
       )

----insert updated records from audit table into temp rpt fact table
insert into tmp.rptfact (MemberIDKey, ProductTypeKey, ProductCodeKey, MemberTypeIDKey, SalaryIDKey, RaceIDKey, OrgKey, ChapterKey, Trans_Date_Key, Paid_Through_Key, Home_Address_Key,
Work_Address_Key, StatusKey, MemberStatusKey)
select distinct  B.MemberIDKey, C.ProductTypeKey, D.ProductCodeKey, E.MemberTypeIDKey, F.SalaryIDKey, G.RaceIDKey, H.ChapterKey, I.Date_Key, J.Date_Key,
K.Address_Key, L.Address_Key, M.StatusKey, N.MemberStatusKey, O.OrgKey  from #auditfact A 
left join rpt.dimMember B 
on A.Member_ID = B.Member_ID
left join rpt.dimProductType C
on A.Prod_Type = C.Product_Type
left join rpt.dimProductCode D
on A.Product_Code = D.Product_Code 
left join rpt.dimMemberType E
on A.Member_Type = E.Member_Type 
left join rpt.dimSalary F
on A.SalaryRange = F.SalaryID 
left join rpt.dimRace G
on A.RaceID = G.RaceID and A.Origin = G.Origin 
left join rpt.dimChapter H
on A.Chapter = H.Chapter_Minor
left join rpt.dimDate I
on A.TransDate = I.Date
left join  rpt.DimDate J
on (A.Paid_Thru_Date = J.Date or (A.Paid_Thru_Date is NULL and J.Date is null))
left join rpt.dimAddress K
on A.Work_Address_Num = K.Address_Num
left join rpt.dimAddress L
on A.Home_Address_Num = L.Address_Num 
left join rpt.dimStatus M
on A.Status = M.Status and A.Gender = M.Gender and A.Category = M.Category
--(A.APA_Status = '' and A.Category =''  and A.Gender = '' and A.AICP_Active ='' and K.Status = '' and K.Category = '' and K.Gender = '' and K.AICPActive = '')
left join rpt.dimMemberStatus N
on A.Member_Status = N.Member_Status and A.Complimentary = N.Complimentary
left join rpt.dimOrg O 
on A.Org_Type = O.OrgCode 
where A.action = 'update'

----update fact member table with iscurrent = 0 
update B
set iscurrent = 0
from tmp.rptfact A inner join rpt.tblfactmember B on
A.MemberIDKey = B.MemberIDKey and A.ProductCodeKey = B.ProductCodeKey
 


----insert new records into fact table and set iscurrent = 1
insert into rpt.tblfactmember (MemberIDKey, ProductTypeKey, ProductCodeKey, MemberTypeIDKey, SalaryIDKey, RaceIDKey, ChapterKey, Trans_Date_Key, Paid_Through_Key,
Home_Address_Key, Work_Address_Key, StatusKey, MemberStatusKey, OrgKey, IsCurrent)
select distinct B.MemberIDKey, C.ProductTypeKey, D.ProductCodeKey, E.MemberTypeIDKey, F.SalaryIDKey, G.RaceIDKey, H.ChapterKey, I.Date_Key, J.Date_Key,
K.Address_Key, L.Address_Key, M.StatusKey, N.MemberStatusKey, O.OrgKey, 1  from #auditfact A 
left join rpt.dimMember B 
on A.Member_ID = B.Member_ID and B.IsActive = 1
left join rpt.dimProductType C
on A.Prod_Type = C.Product_Type and C.IsActive = 1 
left join rpt.dimProductCode D
on A.Product_Code = D.Product_Code and D.IsActive = 1
left join rpt.dimMemberType E
on A.Member_Type = E.Member_Type and E.IsActive = 1
left join rpt.dimSalary F
on A.SalaryRange = F.SalaryID and F.IsActive = 1
left join rpt.dimRace G
on A.RaceID = G.RaceID and A.Origin = G.Origin and G.IsActive = 1
left join rpt.dimChapter H
on A.Chapter = H.Chapter_Minor and H.IsActive = 1
left join rpt.dimDate I
on A.TransDate = I.Date 
left join  rpt.DimDate J
on (A.Paid_Thru_Date = J.Date or A.Paid_Thru_Date is NULL and J.Date is null )
left join rpt.dimAddress K
on A.Work_Address_Num = K.Address_Num and K.IsActive = 1
left join rpt.dimAddress L
on A.Home_Address_Num = L.Address_Num and L.IsActive = 1
left join rpt.dimStatus M
on A.Status = M.Status and A.Gender = M.Gender and A.Category = M.Category and M.IsActive = 1
--(A.APA_Status = '' and A.Category =''  and A.Gender = '' and A.AICP_Active ='' and K.Status = '' and K.Category = '' and K.Gender = '' and K.AICPActive = '')
left join rpt.dimMemberStatus N
on A.Member_Status = N.Member_Status and A.Complimentary = N.Complimentary  and N.IsActive = 1
left join rpt.dimOrg O 
on A.Org_Type = O.OrgCode and O.IsActive = 1

insert into etl.executionlog 
VALUES (
        '@PipelineName'
	   ,'tmp.FactMember_Current'
       ,'rpt.tblfactmember'
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #auditfact
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #auditfact
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
			 ,(select count(*) as RowsRead
				from tmp.FactMember_Current
				) 
       ,getdate()
       )

	   ;with cte as
	   (
	   select  target_table, max(datetimestamp)  as maxtimestamp from etl.executionlog
	   where target_table = 'rpt.tblfactmember'
	   group by target_table
	   )

	   Update A set A.update_count = (select count(*) from tmp.RptFact)
	   from etl.executionlog A inner join cte B
	   on A.target_table = B.target_table
	   where A.target_table = 'rpt.tblfactmember' and datetimestamp = B.maxtimestamp


COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END




