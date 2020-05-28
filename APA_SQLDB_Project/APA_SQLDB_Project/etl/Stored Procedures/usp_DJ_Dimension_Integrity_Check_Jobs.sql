
CREATE procedure [etl].[usp_DJ_Dimension_Integrity_Check_Jobs] as

BEGIN

BEGIN TRY

BEGIN TRAN



/************truncate tmp.Fact_Jobs*********************/

--This process will load the fact data for Jobs in the RAW format
/****************************************************************/
truncate table tmp.Fact_Jobs

-- Insert new jobs into the tmp fact table
insert into tmp.Fact_Jobs (Job_ID, City, State, Country, 
Job_Type, Company, Salary_Range, Post_Time, Title, Status, 
MemberID_Author, MemberID_Provider)

select Content_Ptr_ID, j.City, j.State, j.Country, 
j.Job_Type, j.Company, j.Salary_Range, j.Post_Time, c.Title, c.Status, 
ca.member_id, cp.member_id
from tmp.dj_jobs_job j
inner join tmp.dj_content_content c on c.id = j.content_ptr_id and 
									c.publish_status = 'PUBLISHED' and 
									c.[status] <> 'X'
-- author
Left join tmp.dj_myapa_contactrole cra on cra.content_id = j.content_ptr_id -- authors and providers roles of the job
									and cra.role_type = 'AUTHOR'
Left join vw_djContact ca on ca.contact_id = cra.contact_id
-- provider
Left join tmp.dj_myapa_contactrole crp on crp.content_id = j.content_ptr_id -- authors and providers roles of the job
									and crp.role_type = 'PROVIDER'
left join vw_djContact cp on cp.contact_id = crp.contact_id

-- Insert any missing members that are not in the ContactRole table
insert into rpt.dimMember (Parent, Member_ID, First_Name, Middle_Name, Last_Name, Full_Name, Email, Home_Phone, Mobile_Phone, Work_Phone, Designation,Functional_Title, Birth_Date, JoinDate, Cohort, CohortQuarter, 
IsActive)
select distinct 'Unmapped', [MemberID_Author], '', '', '', '', '', '', '', '', '','', '', '', '', '', 1
from [tmp].Fact_Jobs where [MemberID_Author] not in
(Select Member_ID  from rpt.dimMember where IsActive = 1) 

UNION 

select distinct 'Unmapped', [MemberID_Provider], '', '', '', '', '', '', '', '', '','', '', '', '', '', 1
from [tmp].Fact_Jobs where [MemberID_Provider] not in
(Select Member_ID  from rpt.dimMember where IsActive = 1) 


/* ********************************* 
  INSERT RECORDS INTO DIMENSIONS
  ********************************** */

-- JOB
INSERT INTO [rpt].[dimJob]
           ([JobIDKey],[Job_Type],[City],[State],[Country],[Company],
		   [Salary_Range],[Post_Time],[Title],[Status],
		   [MemberID_Author],[MemberID_Provider])

select Content_Ptr_ID, j.job_type, j.City, j.State, j.Country, 
j.Company, j.Salary_Range, j.Post_Time, c.Title, c.Status, 
ca.member_id, cp.member_id
from tmp.dj_jobs_job j
inner join tmp.dj_content_content c on c.id = j.content_ptr_id and 
									c.publish_status = 'PUBLISHED' and 
									c.[status] <> 'X'
Left join tmp.dj_myapa_contactrole cra on cra.content_id = j.content_ptr_id -- authors and providers roles of the job
									and cra.role_type = 'AUTHOR'
Left join vw_djContact ca on ca.contact_id = cra.contact_id
Left join tmp.dj_myapa_contactrole crp on crp.content_id = j.content_ptr_id -- authors and providers roles of the job
									and crp.role_type = 'PROVIDER'
left join vw_djContact cp on cp.contact_id = crp.contact_id
where Content_Ptr_ID not in
(Select JobIDKey  from rpt.dimJob where IsActive = 1)

-- ADDRESS
INSERT INTO [rpt].[dimJobAddress] ([Job_ID],[City],[State],[Country])
SELECT content_ptr_id, city, state, country
from [tmp].dj_jobs_job j
inner join [tmp].dj_content_content c on c.id = j.content_ptr_id
where c.publish_status = 'PUBLISHED'
and c.[status] <> 'X' 
and content_ptr_id not in (Select Job_ID  from rpt.dimJobAddress where IsActive = 1)

-- CONTENT STATUS
INSERT INTO [rpt].[dimContentStatus] (Status, Title)
SELECT DISTINCT STATUS, 'Unmapped'
FROM [tmp].dj_content_content
WHERE Status not in (
SELECT [Status] from [rpt].[dimContentStatus] where IsActive = 1
)




COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END