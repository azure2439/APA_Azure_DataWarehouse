/*******************************************************************************************************************************
 Description:   This Stored Procedure loads active new or updated records into the member fact table. Records are loaded from the 
				the audit fact table.
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE PROCEDURE [etl].[usp_IMIS_Member_Fact_Update_Insert]
@PipelineName varchar(60) = 'ssms'
as

BEGIN

BEGIN TRY

BEGIN TRAN
 

truncate table tmp.rptfact	

----insert updated records from audit table into temp rpt fact table
insert into tmp.rptfact (MemberIDKey,  ProductCodeKey)
select distinct  B.MemberIDKey,  D.ProductCodeKey from tmp.imis_auditfact A 
left join rpt.dimMember B 
on A.Member_ID = B.Member_ID
left join rpt.dimProductCode D
on A.Product_Code = D.Product_Code 
where A.action = 'update'

----update fact member table with iscurrent = 0 for all member product_code combinations from today's load
update B
set iscurrent = 0
from tmp.rptfact A inner join rpt.tblfactmember B on
A.MemberIDKey = B.MemberIDKey and A.ProductCodeKey = B.ProductCodeKey
where B.IsCurrent = 1

---- Update Trans Date to today's date
Update tmp.imis_auditfact set Transdate = convert(date,getdate())

----Insert NULL Paid_Thru_Date records into fact table and set iscurrent = 1
insert into rpt.tblfactmember (MemberIDKey, ProductTypeKey, ProductCodeKey, MemberTypeIDKey, SalaryIDKey, RaceIDKey, ChapterKey, Trans_Date_Key, Paid_Through_Key,
Home_Address_Key, Work_Address_Key, StatusKey, MemberStatusKey, OrgKey, IsCurrent)
select distinct B.MemberIDKey, C.ProductTypeKey, D.ProductCodeKey, E.MemberTypeIDKey, F.SalaryIDKey, G.RaceIDKey, H.ChapterKey, I.Date_Key, J.Date_Key,
K.Address_Key, L.Address_Key, M.StatusKey, N.MemberStatusKey, O.OrgKey, 1  from tmp.imis_auditfact A 
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
inner join  rpt.DimDate J
on  (A.Paid_Thru_Date is NULL and J.Date is null )
left join rpt.dimAddress K
on A.Home_Address_Num = K.Address_Num and K.IsActive = 1
inner join rpt.dimAddress L
on A.Work_Address_Num = L.Address_Num and L.IsActive = 1
inner join rpt.dimStatus M
on A.Status = M.Status and A.Gender = M.Gender and A.Category = M.Category and M.IsActive = 1
--(A.APA_Status = '' and A.Category =''  and A.Gender = '' and A.AICP_Active ='' and K.Status = '' and K.Category = '' and K.Gender = '' and K.AICPActive = '')
left join rpt.dimMemberStatus N
on A.Member_Status = N.Member_Status and A.Complimentary = N.Complimentary  and N.IsActive = 1
left join rpt.dimOrg O 
on A.Org_Type = O.OrgCode and O.IsActive = 1
where A.Paid_Thru_Date is   null

---- Insert Non-Null Paid_Thru_Date data and set iscurrent = 1

insert into rpt.tblfactmember (MemberIDKey, ProductTypeKey, ProductCodeKey, MemberTypeIDKey, SalaryIDKey, RaceIDKey, ChapterKey, Trans_Date_Key, Paid_Through_Key,
Home_Address_Key, Work_Address_Key, StatusKey, MemberStatusKey, OrgKey, IsCurrent)
select distinct B.MemberIDKey, C.ProductTypeKey, D.ProductCodeKey, E.MemberTypeIDKey, F.SalaryIDKey, G.RaceIDKey, H.ChapterKey, I.Date_Key, J.Date_Key,
K.Address_Key, L.Address_Key, M.StatusKey, N.MemberStatusKey, O.OrgKey, 1  from tmp.imis_auditfact A 
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
inner join  rpt.DimDate J
on  A.Paid_Thru_Date = J.Date
left join rpt.dimAddress K
on A.Home_Address_Num = K.Address_Num and K.IsActive = 1
inner join rpt.dimAddress L
on A.Work_Address_Num = L.Address_Num and L.IsActive = 1
inner join rpt.dimStatus M
on A.Status = M.Status and A.Gender = M.Gender and A.Category = M.Category and M.IsActive = 1
--(A.APA_Status = '' and A.Category =''  and A.Gender = '' and A.AICP_Active ='' and K.Status = '' and K.Category = '' and K.Gender = '' and K.AICPActive = '')
left join rpt.dimMemberStatus N
on A.Member_Status = N.Member_Status and A.Complimentary = N.Complimentary  and N.IsActive = 1
left join rpt.dimOrg O 
on A.Org_Type = O.OrgCode and O.IsActive = 1
where A.Paid_Thru_Date is not  null


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
                     FROM tmp.imis_auditfact
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM tmp.imis_auditfact
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




