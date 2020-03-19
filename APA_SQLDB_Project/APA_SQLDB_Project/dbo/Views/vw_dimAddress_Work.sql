CREATE VIEW [dbo].[vw_dimAddress_Work] AS
Select u.*, d.PREFERRED_MAIL
from rpt.dimAddress u
left join stg.imis_Name_Address d on u.Address_Num = d.ADDRESS_NUM
where u.isactive = '1'
and u.purpose = 'Work Address' 
