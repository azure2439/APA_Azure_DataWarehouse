



/*
Returns django contact roles assigned to a particular content record or member
*/
CREATE VIEW [dbo].[vw_DJ_Content_ContactRole] AS
SELECT 
um.member_ID,
um.contact_ID,
um.contact_type,
um.[status],

cr.role_type,
--FM.Status status,
--FM.Category as member_category,
--FM.Chapter as member_chapter,
--FM.Gender as gender,
--FM.Member_Type,
--FM.SalaryRange,
--FM.Origin,
--FM.RaceID,
--FM.Org_Type,

c.id as content_id,
c.parent_id,
c.master_id

FROM [stg].[dj_content_content] c
INNER JOIN [stg].dj_myapa_contactrole cr on cr.content_id = c.id
INNER JOIN [dbo].vw_DJ_IMIS_UserMapping um ON um.contact_id = cr.contact_id

--INNER JOIN [stg].FactMember_Current FM ON FM.member_id = um.member_id
--										AND FM.IsCurrent = 1
--										AND FM.Product_Code = 'APA'

WHERE c.publish_status = 'PUBLISHED' and c.status <> 'X'