


CREATE procedure [dbo].[zzz_load_rpt_fact_activity] 
as
begin
insert into rpt.tblFactActivity (MemberIDKey, MemberTypeIDKey, ProductTypeKey, ProductCodeKey, ActivityCategoryKey, CampaignCodeKey, Trans_Date_Key, Paid_Through_Key,
SourceKey, Quantity, Amount, Units, SourceCodeKey)
select
B.MemberIDKey, 
C.MemberTypeIDKey, 
D.ProductTypeKey,                                                                                       
E.ProductCodeKey,
F.ActivityCategoryKey,
G.CampaignCodeKey, 
H.Date_Key, 
I.Date_Key,
J.SourceKey,
sum(Quantity),
sum(Amount),
sum(Units),
K.SourceCodeKey
 from stg.imis_Activity A
inner join rpt.dimMember B 
on A.ID = B.Member_ID and B.IsActive = 1
inner join rpt.dimMemberType C
on A.Member_Type = C.Member_Type and C.IsActive = 1
inner join rpt.dimProductType D 
on A.activity_type = D.Product_Type and D.IsActive = 1
inner join rpt.dimProductCode E
on A.Product_Code = E.Product_Code and E.IsActive = 1
inner join rpt.dimActivityCategory F
on A.Category = F.Activity_Category_Code and F.IsActive = 1
inner join rpt.dimCampaignCode G
on A.Campaign_Code = G.Campaign_Code and G.IsActive = 1
inner join rpt.dimDate H
on cast(A.Transaction_Date as Date) = H.Date or (A.Transaction_Date  is NULL and H.Date is null)
inner join  rpt.DimDate I 
on cast(A.Thru_Date as Date) = I.Date or (A.thru_date is NULL and I.Date is null)
inner join rpt.dimSource J 
on A.Source_System = J.Source_System_Code and J.IsActive = 1
inner join rpt.dimSourceCode K
on A.Source_Code = K.Source_Code and K.IsActive = 1
group by 
B.MemberIDKey, 
C.MemberTypeIDKey, 
D.ProductTypeKey,                                                                                       
E.ProductCodeKey,
F.ActivityCategoryKey,
G.CampaignCodeKey, 
H.Date_Key, 
I.Date_Key,
J.SourceKey,
K.SourceCodeKey
end


