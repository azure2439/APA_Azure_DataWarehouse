


/*******************************************************************************************************************************
 Description:   This Stored Procedure inserts any new or updated records from the dimension tables into the activity fact table.
				We join the fact table with the keys in each dimension and only insert active updated or new records.
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/

CREATE procedure [etl].[usp_IMIS_Activity_Fact_Insert]
as


BEGIN

BEGIN TRY

BEGIN TRAN

insert into rpt.tblFactActivity (MemberIDKey, MemberTypeIDKey, ProductTypeKey, ProductCodeKey, ActivityCategoryKey, CampaignCodeKey, Trans_Date_Key, Paid_Through_Key,
SourceKey, Quantity, Amount, Units, SourceCodeKey)
select distinct
B.MemberIDKey, 
C.MemberTypeIDKey, 
D.ProductTypeKey, 
E.ProductCodeKey,
F.ActivityCategoryKey,
G.CampaignCodeKey, 
H.Date_Key, 
I.Date_Key,
J.SourceKey,
Quantity,
Amount,
Units,
K.SourceCodeKey
 
from tmp.imis_Activity A

left join rpt.dimMember B 
on A.ID = B.Member_ID and B.IsActive = 1

left join rpt.dimMemberType C
on A.Member_Type = C.Member_Type and C.IsActive = 1

left join rpt.dimProductType D 
on A.activity_type = D.Product_Type and D.IsActive = 1

left join rpt.dimProductCode E
on A.Product_Code = E.Product_Code and E.IsActive = 1

left join rpt.dimActivityCategory F
on A.Category = F.Activity_Category_Code and F.IsActive = 1

left join rpt.dimCampaignCode G
on A.Campaign_Code = G.Campaign_Code and G.IsActive = 1

left join rpt.dimDate H
on cast(A.Transaction_Date as Date) = H.Date or A.Transaction_Date  is NULL and H.Date is null

left join  rpt.DimDate I 
on cast(A.Thru_Date as Date) = I.Date or A.thru_date is NULL and I.Date is null

left join rpt.dimSource J 
on A.Source_System = J.Source_System_Code and J.IsActive = 1

left join rpt.dimSourceCode K
on A.Source_Code = K.Source_Code and K.IsActive = 1

insert into etl.executionlog 
VALUES (
        '@PipelineName'
	   ,'tmp.imis_Activity'
       ,'rpt.tblFactActivity'
       ,(
              select count(*) as RowsInserted from
			  tmp.imis_Activity
              )
       ,0
			 ,(select count(*) as RowsRead
				from tmp.imis_Activity
				) 
       ,getdate()
       )


COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END








