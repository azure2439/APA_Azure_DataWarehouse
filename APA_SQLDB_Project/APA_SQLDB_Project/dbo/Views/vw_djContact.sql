
/*
View used to return django contact data
*/
CREATE VIEW [dbo].[vw_djContact] AS
SELECT 
a.username AS member_ID,
c.id AS contact_ID,
c.contact_type,
c.[status],

-- repeating fields used for exception reports
c.address_type,
c.address1,
c.address2,
c.city,
c.state,
c.zip_code,
c.country,
c.user_address_num,
c.title, -- note: this title is built to not include the middle inital (iMIS FULL_NAME does include it).

c.prefix_name,
c.first_name,
c.middle_name,
c.last_name,
c.suffix_name,
c.designation,
c.salary_range,
c.chapter,
c.secondary_email,
c.secondary_phone,
c.latitude, -- are these needed? should look at Custom_Address_Geocode
c.longitude,
c.voter_voice_checksum,
c.phone,
c.cell_phone,
c.email,
c.job_title,
c.member_type,
c.company,
c.birth_date,

-- these fields are only found in django about the contact
c.bio,
c.about_me,
c.personal_url,
c.linkedin_url,
c.facebook_url,
c.instagram_url,
c.slug,

-- company fields
c.pas_type,
c.ein_number,
c.organization_type,
c.parent_organization_id

FROM [tmp].[dj_auth_user] a 
INNER JOIN [tmp].[dj_myapa_contact] c on c.user_id = a.id