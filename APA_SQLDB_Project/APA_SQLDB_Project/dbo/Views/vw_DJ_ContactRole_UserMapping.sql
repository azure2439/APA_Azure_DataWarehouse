



/*
Returns django contact roles assigned to a particular content record or member
*/
CREATE VIEW [dbo].[vw_DJ_ContactRole_UserMapping] AS
SELECT 
um.member_ID,
um.contact_ID,
um.contact_type,
um.[status],
c.id as content_id,
c.parent_id,
c.master_id

FROM [stg].[dj_content_content] c
INNER JOIN [stg].dj_myapa_contactrole cr on cr.content_id = c.id
INNER JOIN [dbo].vw_DJ_IMIS_UserMapping um ON um.contact_id = cr.contact_id
WHERE c.publish_status = 'PUBLISHED' and c.status <> 'X'