
CREATE   procedure [etl].[usp_DJ_Dimension_Integrity_Check_Events] as


BEGIN

BEGIN TRY

BEGIN TRAN

/************truncate tmp.Fact_Events*********************/

--This process will load the fact data for Jobs in the RAW format
/****************************************************************/
truncate table tmp.[Fact_Events];


/**********************load tmp.fact*****************/

insert into [tmp].[Fact_Events] (
			[Event_ID]
           ,[Master_ID]
           ,[Parent_Master_ID]
           ,[Event_Type]
           ,[Title]
           ,[Status]
           ,[Address1]
           ,[Address2]
           ,[City]
           ,[State]
           ,[Zip_Code]
           ,[Country]
           ,[Begin_Time]
           ,[End_Time]
           ,[Is_Online]
           ,[CM_Status]
           ,[CM_Requested]
           ,[CM_Approved]
           ,[CM_Law_Requested]
           ,[CM_Law_Approved]
           ,[Is_Free]
           ,[Length_In_Minutes]
           ,[Timezone]
           ,[Provider_ID]
)

SELECT DISTINCT 
			C.[id]
           ,C.[Master_ID]
           ,C.[Parent_ID]
           ,[Event_Type]
           ,C.[Title]
           ,C.[Status]
           ,E.[Address1]
           ,E.[Address2]
           ,E.[City]
           ,E.[State]
           ,E.[Zip_Code]
           ,E.[Country]
           ,[Begin_Time]
           ,[End_Time]
           ,[Is_Online]
           ,[CM_Status]
           ,[CM_Requested]
           ,[CM_Approved]
           ,[CM_Law_Requested]
           ,[CM_Law_Approved]
           ,[Is_Free]
           ,[Length_In_Minutes]
           ,[Timezone]
           ,CON.Member_ID

FROM tmp.dj_events_event E
INNER JOIN stg.dj_content_content C ON C.ID = E.content_ptr_id -- IF only the event is updated
									AND C.publish_status = 'PUBLISHED'
LEFT JOIN stg.dj_myapa_contactrole CR ON CR.content_id = C.id
													AND CR.role_type = 'PROVIDER'
LEFT JOIN vw_dj_imis_usermapping CON ON CON.contact_id = CR.contact_id


/* ********************************* 
  INSERT RECORDS INTO DIMENSIONS
  ********************************** */

-- Insert new event types into the rpt.dimEventType
insert into rpt.[dimEventType] (
	[EventTypeCode],
	[Title]
	
)
SELECT distinct event_type, 'Unmapped'
FROM tmp.dj_events_event
WHERE event_type NOT IN (
	SELECT EventTypeCode
	FROM rpt.dimEventType
);

-- insert unknown exam codes into rpt.dimAICPExam
insert into rpt.dimEventCMStatus (
EventCMStatusCode, Title
)
SELECT DISTINCT cm_status, 'Unmapped'
FROM [tmp].[dj_events_event]
WHERE cm_status NOT IN (
	SELECT EventCMStatusCode
	FROM rpt.dimEventCMStatus
)



COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END