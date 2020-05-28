
CREATE Procedure [etl].[usp_Cadmium_Tmp_to_Stage]
@PipelineName varchar(60) = 'ssms'
as

/*******************************************************************************************************************************
 Description:   This Stored Procedure merges data from the Cadmium temp tables into the staging tables on a daily basis. These tables 
				have been identified to be incrementally merged on a daily basis. 
 				

Added By:		Morgan Diestler on 5/8/2020				
*******************************************************************************************************************************/


BEGIN

BEGIN TRY

BEGIN TRAN

DECLARE @Yesterday DATETIME = DATEADD(dd, - 1, GETDATE())
DECLARE @Today DATETIME = GETDATE()

IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
       Drop TABLE #audittemp

CREATE TABLE #audittemp (
       action NVARCHAR(20)
       ,inserted_id varchar(80)
       ,deleted_id varchar(80)
       );

DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @ErrorSeverity NVARCHAR(MAX)


DECLARE @ErrorState tinyint

/*********************Cadmium_Abstracts**************/
MERGE stg.cadmium_abstracts AS DST
USING tmp.cadmium_abstracts AS SRC
       ON DST.SUBMISSIONID = SRC.SUBMISSIONID 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.SUBMITTERID, '') <> ISNULL(SRC.SUBMITTERID, '')
                     OR ISNULL(DST.ABSTRACTID, '') <> ISNULL(SRC.ABSTRACTID, '')
                     OR ISNULL(DST.ABSTRACTTOPIC, '') <> ISNULL(SRC.ABSTRACTTOPIC, '')
                     OR ISNULL(DST.ABSTRACTTOPICALT, '') <> ISNULL(SRC.ABSTRACTTOPICALT, '')
                     OR ISNULL(DST.ABSTRACTSUBTOPIC, '') <> ISNULL(SRC.ABSTRACTSUBTOPIC, '')
                     OR ISNULL(DST.ABSTRACTTEXT0, '') <> ISNULL(SRC.ABSTRACTTEXT0, '')
                     OR ISNULL(DST.ABSTRACTTEXT1, '') <> ISNULL(SRC.ABSTRACTTEXT1, '')
                     OR ISNULL(DST.ABSTRACTTEXT2, '') <> ISNULL(SRC.ABSTRACTTEXT2, '')
                     OR ISNULL(DST.ABSTRACTTEXT3, '') <> ISNULL(SRC.ABSTRACTTEXT3, '')
					 OR ISNULL(DST.ABSTRACTTEXT4, '') <> ISNULL(SRC.ABSTRACTTEXT4, '')
					 OR ISNULL(DST.ABSTRACTTEXT5, '') <> ISNULL(SRC.ABSTRACTTEXT5, '')
					 OR ISNULL(DST.ABSTRACTTEXT11, '') <> ISNULL(SRC.ABSTRACTTEXT11, '')
					 OR ISNULL(DST.ABSTRACTTEXT12, '') <> ISNULL(SRC.ABSTRACTTEXT12, '')
					 OR ISNULL(DST.ABSTRACTTEXT13, '') <> ISNULL(SRC.ABSTRACTTEXT13, '')
					 OR ISNULL(DST.ABSTRACTTEXT14, '') <> ISNULL(SRC.ABSTRACTTEXT14, '')
					 OR ISNULL(DST.ABSTRACTTEXT15, '') <> ISNULL(SRC.ABSTRACTTEXT15, '')
					 OR ISNULL(DST.ABSTRACTTEXT16, '') <> ISNULL(SRC.ABSTRACTTEXT16, '')
					 OR ISNULL(DST.ABSTRACTTEXT27, '') <> ISNULL(SRC.ABSTRACTTEXT27, '')
					 OR ISNULL(DST.ABSTRACTTEXT28, '') <> ISNULL(SRC.ABSTRACTTEXT28, '')
					 OR ISNULL(DST.ABSTRACTTEXT29, '') <> ISNULL(SRC.ABSTRACTTEXT29, '')
					 OR ISNULL(DST.ABSTRACTTEXT30, '') <> ISNULL(SRC.ABSTRACTTEXT30, '')
					 OR ISNULL(DST.ABSTRACTTEXT31, '') <> ISNULL(SRC.ABSTRACTTEXT31, '')
					 OR ISNULL(DST.ABSTRACTTEXT32, '') <> ISNULL(SRC.ABSTRACTTEXT32, '')
					 OR ISNULL(DST.ABSTRACTTEXT33, '') <> ISNULL(SRC.ABSTRACTTEXT33, '')
					 OR ISNULL(DST.ABSTRACTTEXT34, '') <> ISNULL(SRC.ABSTRACTTEXT34, '')
					 OR ISNULL(DST.ABSTRACTTEXT35, '') <> ISNULL(SRC.ABSTRACTTEXT35, '')
					 OR ISNULL(DST.ABSTRACTTEXT36, '') <> ISNULL(SRC.ABSTRACTTEXT36, '')
					 OR ISNULL(DST.ABSTRACTTARGETAUDIENCE, '') <> ISNULL(SRC.ABSTRACTTARGETAUDIENCE, '')
					 OR ISNULL(DST.ABSTRACTJOBFUNCTION, '') <> ISNULL(SRC.ABSTRACTJOBFUNCTION, '')
					 OR ISNULL(DST.ABSTRACTPICKLIST, '') <> ISNULL(SRC.ABSTRACTPICKLIST, '')
					 OR ISNULL(DST.TARGETAUDIENCEOTHER, '') <> ISNULL(SRC.TARGETAUDIENCEOTHER, '')
                     OR ISNULL(DST.JOBFUNCTIONOTHER, '') <> ISNULL(SRC.JOBFUNCTIONOTHER, '')
					 OR ISNULL(DST.PICKLISTOTHER, '') <> ISNULL(SRC.PICKLISTOTHER, '')
					 OR ISNULL(DST.ABSTRACTQUESTION1, '') <> ISNULL(SRC.ABSTRACTQUESTION1, '')
					 OR ISNULL(DST.ABSTRACTQUESTION2, '') <> ISNULL(SRC.ABSTRACTQUESTION2, '')
					 OR ISNULL(DST.ABSTRACTQUESTION3, '') <> ISNULL(SRC.ABSTRACTQUESTION3, '')
					 OR ISNULL(DST.ABSTRACTQUESTION4, '') <> ISNULL(SRC.ABSTRACTQUESTION4, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.SUBMITTERID = ISNULL(SRC.SUBMITTERID, '')
                     ,DST.ABSTRACTID = ISNULL(SRC.ABSTRACTID, '')
                     ,DST.ABSTRACTTOPIC = ISNULL(SRC.ABSTRACTTOPIC, '')
                     ,DST.ABSTRACTTOPICALT = ISNULL(SRC.ABSTRACTTOPICALT, '')
                     ,DST.ABSTRACTSUBTOPIC = ISNULL(SRC.ABSTRACTSUBTOPIC, '')
                     ,DST.ABSTRACTTEXT0 = ISNULL(SRC.ABSTRACTTEXT0, '')
                     ,DST.ABSTRACTTEXT1 = ISNULL(SRC.ABSTRACTTEXT1, '')
                     ,DST.ABSTRACTTEXT2 = ISNULL(SRC.ABSTRACTTEXT2, '')
                     ,DST.ABSTRACTTEXT3 = ISNULL(SRC.ABSTRACTTEXT3, '')
					 ,DST.ABSTRACTTEXT4 = ISNULL(SRC.ABSTRACTTEXT4, '')
					 ,DST.ABSTRACTTEXT5 = ISNULL(SRC.ABSTRACTTEXT5, '')
					 ,DST.ABSTRACTTEXT11 = ISNULL(SRC.ABSTRACTTEXT11, '')
					 ,DST.ABSTRACTTEXT12 = ISNULL(SRC.ABSTRACTTEXT12, '')
					 ,DST.ABSTRACTTEXT13 = ISNULL(SRC.ABSTRACTTEXT13, '')
					 ,DST.ABSTRACTTEXT14 = ISNULL(SRC.ABSTRACTTEXT14, '')
					 ,DST.ABSTRACTTEXT15 = ISNULL(SRC.ABSTRACTTEXT15, '')
					 ,DST.ABSTRACTTEXT16 = ISNULL(SRC.ABSTRACTTEXT16, '')
					 ,DST.ABSTRACTTEXT27 = ISNULL(SRC.ABSTRACTTEXT27, '')
					 ,DST.ABSTRACTTEXT28 = ISNULL(SRC.ABSTRACTTEXT28, '')
					 ,DST.ABSTRACTTEXT29 = ISNULL(SRC.ABSTRACTTEXT29, '')
					 ,DST.ABSTRACTTEXT30 = ISNULL(SRC.ABSTRACTTEXT30, '')
					 ,DST.ABSTRACTTEXT31 = ISNULL(SRC.ABSTRACTTEXT31, '')
					 ,DST.ABSTRACTTEXT32 = ISNULL(SRC.ABSTRACTTEXT32, '')
					 ,DST.ABSTRACTTEXT33 = ISNULL(SRC.ABSTRACTTEXT33, '')
					 ,DST.ABSTRACTTEXT34 = ISNULL(SRC.ABSTRACTTEXT34, '')
					 ,DST.ABSTRACTTEXT35 = ISNULL(SRC.ABSTRACTTEXT35, '')
					 ,DST.ABSTRACTTEXT36 = ISNULL(SRC.ABSTRACTTEXT36, '')
					 ,DST.ABSTRACTTARGETAUDIENCE =ISNULL(SRC.ABSTRACTTARGETAUDIENCE, '')
					 ,DST.ABSTRACTJOBFUNCTION = ISNULL(SRC.ABSTRACTJOBFUNCTION, '')
					 ,DST.ABSTRACTPICKLIST = ISNULL(SRC.ABSTRACTPICKLIST, '')
					 ,DST.TARGETAUDIENCEOTHER = ISNULL(SRC.TARGETAUDIENCEOTHER, '')
                     ,DST.JOBFUNCTIONOTHER = ISNULL(SRC.JOBFUNCTIONOTHER, '')
					 ,DST.PICKLISTOTHER = ISNULL(SRC.PICKLISTOTHER, '')
					 ,DST.ABSTRACTQUESTION1 = ISNULL(SRC.ABSTRACTQUESTION1, '')
					 ,DST.ABSTRACTQUESTION2 = ISNULL(SRC.ABSTRACTQUESTION2, '')
					 ,DST.ABSTRACTQUESTION3 = ISNULL(SRC.ABSTRACTQUESTION3, '')
					 ,DST.ABSTRACTQUESTION4 = ISNULL(SRC.ABSTRACTQUESTION4, '')
WHEN NOT MATCHED
       THEN
              INSERT (
       [SubmissionID]
      ,[SubmitterID]
      ,[AbstractID]
      ,[AbstractTopic]
      ,[AbstractTopicAlt]
      ,[AbstractSubTopic]
      ,[AbstractText0]
      ,[AbstractText1]
      ,[AbstractText2]
      ,[AbstractText3]
      ,[AbstractText4]
      ,[AbstractText5]
      ,[AbstractText11]
      ,[AbstractText12]
      ,[AbstractText13]
      ,[AbstractText14]
      ,[AbstractText15]
      ,[AbstractText16]
      ,[AbstractText27]
      ,[AbstractText28]
      ,[AbstractText29]
      ,[AbstractText30]
      ,[AbstractText31]
      ,[AbstractText32]
      ,[AbstractText33]
      ,[AbstractText34]
      ,[AbstractText35]
      ,[AbstractText36]
      ,[AbstractTargetAudience]
      ,[AbstractJobFunction]
      ,[AbstractPickList]
      ,[TargetAudienceOther]
      ,[JobFunctionOther]
      ,[PickListOther]
      ,[AbstractQuestion1]
      ,[AbstractQuestion2]
      ,[AbstractQuestion3]
      ,[AbstractQuestion4]
          
                     )
              VALUES (
                     SRC.[SubmissionID]
      ,SRC.[SubmitterID]
      ,SRC.[AbstractID]
      ,SRC.[AbstractTopic]
      ,SRC.[AbstractTopicAlt]
      ,SRC.[AbstractSubTopic]
      ,SRC.[AbstractText0]
      ,SRC.[AbstractText1]
      ,SRC.[AbstractText2]
      ,SRC.[AbstractText3]
      ,SRC.[AbstractText4]
      ,SRC.[AbstractText5]
      ,SRC.[AbstractText11]
      ,SRC.[AbstractText12]
      ,SRC.[AbstractText13]
      ,SRC.[AbstractText14]
      ,SRC.[AbstractText15]
      ,SRC.[AbstractText16]
      ,SRC.[AbstractText27]
      ,SRC.[AbstractText28]
      ,SRC.[AbstractText29]
      ,SRC.[AbstractText30]
      ,SRC.[AbstractText31]
      ,SRC.[AbstractText32]
      ,SRC.[AbstractText33]
      ,SRC.[AbstractText34]
      ,SRC.[AbstractText35]
      ,SRC.[AbstractText36]
      ,SRC.[AbstractTargetAudience]
      ,SRC.[AbstractJobFunction]
      ,SRC.[AbstractPickList]
      ,SRC.[TargetAudienceOther]
      ,SRC.[JobFunctionOther]
      ,SRC.[PickListOther]
      ,SRC.[AbstractQuestion1]
      ,SRC.[AbstractQuestion2]
      ,SRC.[AbstractQuestion3]
      ,SRC.[AbstractQuestion4]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.SUBMISSIONID
       ,deleted.SUBMISSIONID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.cadmium_abstracts'
       ,'stg.cadmium_abstracts'
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
			 ,(select count(*) as RowsRead
				from tmp.cadmium_abstracts 
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp


/*********************Cadmium_Authors**************/
MERGE stg.cadmium_authors AS DST
USING tmp.cadmium_authors AS SRC
       ON DST.authorid = SRC.authorid 
WHEN MATCHED
             
              AND (
                   
                     ISNULL(DST.AUTHORADDRESS1, '') <> ISNULL(SRC.AUTHORADDRESS1, '')
                     OR ISNULL(DST.AUTHORADDRESS2, '') <> ISNULL(SRC.AUTHORADDRESS2, '')
                     OR ISNULL(DST.AUTHORADDRESS3, '') <> ISNULL(SRC.AUTHORADDRESS3, '')
                     OR ISNULL(DST.AUTHORASSISTANTEMAIL, '') <> ISNULL(SRC.AUTHORASSISTANTEMAIL, '')
                     OR ISNULL(DST.AUTHORASSISTANTNAME, '') <> ISNULL(SRC.AUTHORASSISTANTNAME, '')
                     OR ISNULL(DST.AUTHORASSISTANTTELEPHONE, '') <> ISNULL(SRC.AUTHORASSISTANTTELEPHONE, '')
                     OR ISNULL(DST.AUTHORASSOCIATIONMEMBERNUMBER, '') <> ISNULL(SRC.AUTHORASSOCIATIONMEMBERNUMBER, '')
                     OR ISNULL(DST.AUTHORASSOCIATIONMEMBERYESNO, '') <> ISNULL(SRC.AUTHORASSOCIATIONMEMBERYESNO, '')
                     OR ISNULL(DST.AUTHORBIOGRAPHYTEXT, '') <> ISNULL(SRC.AUTHORBIOGRAPHYTEXT, '')
					 OR ISNULL(DST.AUTHORBIOSKETCHTEXT, '') <> ISNULL(SRC.AUTHORBIOSKETCHTEXT, '')
					 OR ISNULL(DST.AUTHORCITY, '') <> ISNULL(SRC.AUTHORCITY, '')
					 OR ISNULL(DST.AUTHORCOUNTRY, '') <> ISNULL(SRC.AUTHORCOUNTRY, '')
					 OR ISNULL(DST.AUTHORCREDENTIALS, '') <> ISNULL(SRC.AUTHORCREDENTIALS, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD1, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD1, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD10, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD10, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD2, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD2, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD3, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD3, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD4, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD4, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD5, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD5, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD6, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD6, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD7, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD7, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD8, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD8, '')
					 OR ISNULL(DST.AUTHORCUSTOMFIELD9, '') <> ISNULL(SRC.AUTHORCUSTOMFIELD9, '')
					 OR ISNULL(DST.AUTHORDEPARTMENT, '') <> ISNULL(SRC.AUTHORDEPARTMENT, '')
					 OR ISNULL(DST.AUTHOREMAIL, '') <> ISNULL(SRC.AUTHOREMAIL, '')
					 OR ISNULL(DST.AUTHOREMAILALTERNATIVE, '') <> ISNULL(SRC.AUTHOREMAILALTERNATIVE, '')
					 OR ISNULL(DST.AUTHORFACEBOOK, '') <> ISNULL(SRC.AUTHORFACEBOOK, '')
					 OR ISNULL(DST.AUTHORFIRSTNAME, '') <> ISNULL(SRC.AUTHORFIRSTNAME, '')
					 OR ISNULL(DST.AUTHORGOOGLEPLUS, '') <> ISNULL(SRC.AUTHORGOOGLEPLUS, '')
					 OR ISNULL(DST.AUTHORINSTAGRAM, '') <> ISNULL(SRC.AUTHORINSTAGRAM, '')
					 OR ISNULL(DST.AUTHORKEY, '') <> ISNULL(SRC.AUTHORKEY, '')
                     OR ISNULL(DST.AUTHORLASTNAME, '') <> ISNULL(SRC.AUTHORLASTNAME, '')
					 OR ISNULL(DST.AUTHORLINKEDIN, '') <> ISNULL(SRC.AUTHORLINKEDIN, '')
					 OR ISNULL(DST.AUTHORMEMBERFLAG, '') <> ISNULL(SRC.AUTHORMEMBERFLAG, '')
					 OR ISNULL(DST.AUTHORMEMBERLOGINS, '') <> ISNULL(SRC.AUTHORMEMBERLOGINS, '')
					 OR ISNULL(DST.AUTHORMIDDLEINITIAL, '') <> ISNULL(SRC.AUTHORMIDDLEINITIAL, '')
					 OR ISNULL(DST.AUTHORORGANIZATION, '') <> ISNULL(SRC.AUTHORORGANIZATION, '')
					 OR ISNULL(DST.AUTHORPASTATTENDEEYESNO, '') <> ISNULL(SRC.AUTHORPASTATTENDEEYESNO, '')
					 OR ISNULL(DST.AUTHORPASTCONFERENCESWHERE, '') <> ISNULL(SRC.AUTHORPASTCONFERENCESWHERE, '')
					 OR ISNULL(DST.AUTHORPASTCONFERENCESYESNO, '') <> ISNULL(SRC.AUTHORPASTCONFERENCESYESNO, '')
					 OR ISNULL(DST.AUTHORPHOTOEXTENSION, '') <> ISNULL(SRC.AUTHORPHOTOEXTENSION, '')
					 OR ISNULL(DST.AUTHORPHOTOFILENAME, '') <> ISNULL(SRC.AUTHORPHOTOFILENAME, '')
					 OR ISNULL(DST.AUTHORPHOTOHEIGHT, '') <> ISNULL(SRC.AUTHORPHOTOHEIGHT, '')
					 OR ISNULL(DST.AUTHORPHOTOLOCATION, '') <> ISNULL(SRC.AUTHORPHOTOLOCATION, '')
					 OR ISNULL(DST.AUTHORPHOTOORIGINALNAME, '') <> ISNULL(SRC.AUTHORPHOTOORIGINALNAME, '')
					 OR ISNULL(DST.AUTHORPHOTOSIZE, '') <> ISNULL(SRC.AUTHORPHOTOSIZE, '')
					 OR ISNULL(DST.AUTHORPHOTOWIDTH, '') <> ISNULL(SRC.AUTHORPHOTOWIDTH, '')
					 OR ISNULL(DST.AUTHORPOSITION, '') <> ISNULL(SRC.AUTHORPOSITION, '')
					 OR ISNULL(DST.AUTHORPREFIX, '') <> ISNULL(SRC.AUTHORPREFIX, '')
					 OR ISNULL(DST.AUTHORREGISTEREDFLAG, '') <> ISNULL(SRC.AUTHORREGISTEREDFLAG, '')
					 OR ISNULL(DST.AUTHORREGISTEREDNUMBER, '') <> ISNULL(SRC.AUTHORREGISTEREDNUMBER, '')
					 OR ISNULL(DST.AUTHORSTATE, '') <> ISNULL(SRC.AUTHORSTATE, '')
					 OR ISNULL(DST.AUTHORSUFFIX, '') <> ISNULL(SRC.AUTHORSUFFIX, '')
					 OR ISNULL(DST.AUTHORTELEPHONECELL, '') <> ISNULL(SRC.AUTHORTELEPHONECELL, '')
					 OR ISNULL(DST.AUTHORTELEPHONEFAX, '') <> ISNULL(SRC.AUTHORTELEPHONEFAX, '')
					 OR ISNULL(DST.AUTHORTELEPHONEOFFICE, '') <> ISNULL(SRC.AUTHORTELEPHONEOFFICE, '')
					 OR ISNULL(DST.AUTHORTWITTERHANDLE, '') <> ISNULL(SRC.AUTHORTWITTERHANDLE, '')
					 OR ISNULL(DST.AUTHORTWITTERPAGE, '') <> ISNULL(SRC.AUTHORTWITTERPAGE, '')
					 OR ISNULL(DST.AUTHORWEBSITE, '') <> ISNULL(SRC.AUTHORWEBSITE, '')
					 OR ISNULL(DST.AUTHORZIP, '') <> ISNULL(SRC.AUTHORZIP, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.AUTHORADDRESS1 = ISNULL(SRC.AUTHORADDRESS1, '')
                     ,DST.AUTHORADDRESS2 = ISNULL(SRC.AUTHORADDRESS2, '')
                     ,DST.AUTHORADDRESS3 = ISNULL(SRC.AUTHORADDRESS3, '')
                     ,DST.AUTHORASSISTANTEMAIL = ISNULL(SRC.AUTHORASSISTANTEMAIL, '')
                     ,DST.AUTHORASSISTANTNAME = ISNULL(SRC.AUTHORASSISTANTNAME, '')
                     ,DST.AUTHORASSISTANTTELEPHONE = ISNULL(SRC.AUTHORASSISTANTTELEPHONE, '')
                     ,DST.AUTHORASSOCIATIONMEMBERNUMBER = ISNULL(SRC.AUTHORASSOCIATIONMEMBERNUMBER, '')
                     ,DST.AUTHORASSOCIATIONMEMBERYESNO = ISNULL(SRC.AUTHORASSOCIATIONMEMBERYESNO, '')
                     ,DST.AUTHORBIOGRAPHYTEXT = ISNULL(SRC.AUTHORBIOGRAPHYTEXT, '')
					 ,DST.AUTHORBIOSKETCHTEXT = ISNULL(SRC.AUTHORBIOSKETCHTEXT, '')
					 ,DST.AUTHORCITY = ISNULL(SRC.AUTHORCITY, '')
					 ,DST.AUTHORCOUNTRY = ISNULL(SRC.AUTHORCOUNTRY, '')
					 ,DST.AUTHORCREDENTIALS = ISNULL(SRC.AUTHORCREDENTIALS, '')
					 ,DST.AUTHORCUSTOMFIELD1 = ISNULL(SRC.AUTHORCUSTOMFIELD1, '')
					 ,DST.AUTHORCUSTOMFIELD10 = ISNULL(SRC.AUTHORCUSTOMFIELD10, '')
					 ,DST.AUTHORCUSTOMFIELD2 = ISNULL(SRC.AUTHORCUSTOMFIELD2, '')
					 ,DST.AUTHORCUSTOMFIELD3 = ISNULL(SRC.AUTHORCUSTOMFIELD3, '')
					 ,DST.AUTHORCUSTOMFIELD4 = ISNULL(SRC.AUTHORCUSTOMFIELD4, '')
					 ,DST.AUTHORCUSTOMFIELD5 = ISNULL(SRC.AUTHORCUSTOMFIELD5, '')
					 ,DST.AUTHORCUSTOMFIELD6 = ISNULL(SRC.AUTHORCUSTOMFIELD6, '')
					 ,DST.AUTHORCUSTOMFIELD7 = ISNULL(SRC.AUTHORCUSTOMFIELD7, '')
					 ,DST.AUTHORCUSTOMFIELD8 = ISNULL(SRC.AUTHORCUSTOMFIELD8, '')
					 ,DST.AUTHORCUSTOMFIELD9 = ISNULL(SRC.AUTHORCUSTOMFIELD9, '')
					 ,DST.AUTHORDEPARTMENT = ISNULL(SRC.AUTHORDEPARTMENT, '')
					 ,DST.AUTHOREMAIL = ISNULL(SRC.AUTHOREMAIL, '')
					 ,DST.AUTHOREMAILALTERNATIVE = ISNULL(SRC.AUTHOREMAILALTERNATIVE, '')
					 ,DST.AUTHORFACEBOOK = ISNULL(SRC.AUTHORFACEBOOK, '')
					 ,DST.AUTHORFIRSTNAME = ISNULL(SRC.AUTHORFIRSTNAME, '')
					 ,DST.AUTHORGOOGLEPLUS = ISNULL(SRC.AUTHORGOOGLEPLUS, '')
					 ,DST.AUTHORINSTAGRAM = ISNULL(SRC.AUTHORINSTAGRAM, '')
					 ,DST.AUTHORKEY = ISNULL(SRC.AUTHORKEY, '')
                     ,DST.AUTHORLASTNAME = ISNULL(SRC.AUTHORLASTNAME, '')
					 ,DST.AUTHORLINKEDIN = ISNULL(SRC.AUTHORLINKEDIN, '')
					 ,DST.AUTHORMEMBERFLAG = ISNULL(SRC.AUTHORMEMBERFLAG, '')
					 ,DST.AUTHORMEMBERLOGINS = ISNULL(SRC.AUTHORMEMBERLOGINS, '')
					 ,DST.AUTHORMIDDLEINITIAL = ISNULL(SRC.AUTHORMIDDLEINITIAL, '')
					 ,DST.AUTHORORGANIZATION = ISNULL(SRC.AUTHORORGANIZATION, '')
					 ,DST.AUTHORPASTATTENDEEYESNO = ISNULL(SRC.AUTHORPASTATTENDEEYESNO, '')
					 ,DST.AUTHORPASTCONFERENCESWHERE = ISNULL(SRC.AUTHORPASTCONFERENCESWHERE, '')
					 ,DST.AUTHORPASTCONFERENCESYESNO = ISNULL(SRC.AUTHORPASTCONFERENCESYESNO, '')
					 ,DST.AUTHORPHOTOEXTENSION = ISNULL(SRC.AUTHORPHOTOEXTENSION, '')
					 ,DST.AUTHORPHOTOFILENAME = ISNULL(SRC.AUTHORPHOTOFILENAME, '')
					 ,DST.AUTHORPHOTOHEIGHT = ISNULL(SRC.AUTHORPHOTOHEIGHT, '')
					 ,DST.AUTHORPHOTOLOCATION = ISNULL(SRC.AUTHORPHOTOLOCATION, '')
					 ,DST.AUTHORPHOTOORIGINALNAME = ISNULL(SRC.AUTHORPHOTOORIGINALNAME, '')
					 ,DST.AUTHORPHOTOSIZE = ISNULL(SRC.AUTHORPHOTOSIZE, '')
					 ,DST.AUTHORPHOTOWIDTH = ISNULL(SRC.AUTHORPHOTOWIDTH, '')
					 ,DST.AUTHORPOSITION = ISNULL(SRC.AUTHORPOSITION, '')
					 ,DST.AUTHORPREFIX = ISNULL(SRC.AUTHORPREFIX, '')
					 ,DST.AUTHORREGISTEREDFLAG =ISNULL(SRC.AUTHORREGISTEREDFLAG, '')
					 ,DST.AUTHORREGISTEREDNUMBER = ISNULL(SRC.AUTHORREGISTEREDNUMBER, '')
					 ,DST.AUTHORSTATE = ISNULL(SRC.AUTHORSTATE, '')
					 ,DST.AUTHORSUFFIX = ISNULL(SRC.AUTHORSUFFIX, '')
					 ,DST.AUTHORTELEPHONECELL = ISNULL(SRC.AUTHORTELEPHONECELL, '')
					 ,DST.AUTHORTELEPHONEFAX = ISNULL(SRC.AUTHORTELEPHONEFAX, '')
					 ,DST.AUTHORTELEPHONEOFFICE = ISNULL(SRC.AUTHORTELEPHONEOFFICE, '')
					 ,DST.AUTHORTWITTERHANDLE = ISNULL(SRC.AUTHORTWITTERHANDLE, '')
					 ,DST.AUTHORTWITTERPAGE = ISNULL(SRC.AUTHORTWITTERPAGE, '')
					 ,DST.AUTHORWEBSITE = ISNULL(SRC.AUTHORWEBSITE, '')
					 ,DST.AUTHORZIP = ISNULL(SRC.AUTHORZIP, '')
WHEN NOT MATCHED
       THEN
              INSERT (
       [AuthorAddress1]
      ,[AuthorAddress2]
      ,[AuthorAddress3]
      ,[AuthorAssistantEmail]
      ,[AuthorAssistantName]
      ,[AuthorAssistantTelephone]
      ,[AuthorAssociationMemberNumber]
      ,[AuthorAssociationMemberYesNo]
      ,[AuthorBiographyText]
      ,[AuthorBiosketchText]
      ,[AuthorCity]
      ,[AuthorCountry]
      ,[AuthorCredentials]
      ,[AuthorCustomField1]
      ,[AuthorCustomField10]
      ,[AuthorCustomField2]
      ,[AuthorCustomField3]
      ,[AuthorCustomField4]
      ,[AuthorCustomField5]
      ,[AuthorCustomField6]
      ,[AuthorCustomField7]
      ,[AuthorCustomField8]
      ,[AuthorCustomField9]
      ,[AuthorDepartment]
      ,[AuthorEmail]
      ,[AuthorEmailAlternative]
      ,[AuthorFacebook]
      ,[AuthorFirstName]
      ,[AuthorGooglePlus]
      ,[AuthorID]
      ,[AuthorInstagram]
      ,[AuthorKey]
      ,[AuthorLastName]
      ,[AuthorLinkedIn]
      ,[AuthorMemberFlag]
      ,[AuthorMemberLogins]
      ,[AuthorMiddleInitial]
      ,[AuthorOrganization]
      ,[AuthorPastAttendeeYesNo]
      ,[AuthorPastConferencesWhere]
      ,[AuthorPastConferencesYesNo]
      ,[AuthorPhotoExtension]
      ,[AuthorPhotoFileName]
      ,[AuthorPhotoHeight]
      ,[AuthorPhotoLocation]
      ,[AuthorPhotoOriginalName]
      ,[AuthorPhotoSize]
      ,[AuthorPhotoWidth]
      ,[AuthorPosition]
      ,[AuthorPrefix]
      ,[AuthorRegisteredFlag]
      ,[AuthorRegisteredNumber]
      ,[AuthorState]
      ,[AuthorSuffix]
      ,[AuthorTelephoneCell]
      ,[AuthorTelephoneFax]
      ,[AuthorTelephoneOffice]
      ,[AuthorTwitterHandle]
      ,[AuthorTwitterPage]
      ,[AuthorWebsite]
      ,[AuthorZip]
          
                     )
              VALUES (
                     SRC.[AuthorAddress1]
      ,SRC.[AuthorAddress2]
      ,SRC.[AuthorAddress3]
      ,SRC.[AuthorAssistantEmail]
      ,SRC.[AuthorAssistantName]
      ,SRC.[AuthorAssistantTelephone]
      ,SRC.[AuthorAssociationMemberNumber]
      ,SRC.[AuthorAssociationMemberYesNo]
      ,SRC.[AuthorBiographyText]
      ,SRC.[AuthorBiosketchText]
      ,SRC.[AuthorCity]
      ,SRC.[AuthorCountry]
      ,SRC.[AuthorCredentials]
      ,SRC.[AuthorCustomField1]
      ,SRC.[AuthorCustomField10]
      ,SRC.[AuthorCustomField2]
      ,SRC.[AuthorCustomField3]
      ,SRC.[AuthorCustomField4]
      ,SRC.[AuthorCustomField5]
      ,SRC.[AuthorCustomField6]
      ,SRC.[AuthorCustomField7]
      ,SRC.[AuthorCustomField8]
      ,SRC.[AuthorCustomField9]
      ,SRC.[AuthorDepartment]
      ,SRC.[AuthorEmail]
      ,SRC.[AuthorEmailAlternative]
      ,SRC.[AuthorFacebook]
      ,SRC.[AuthorFirstName]
      ,SRC.[AuthorGooglePlus]
      ,SRC.[AuthorID]
      ,SRC.[AuthorInstagram]
      ,SRC.[AuthorKey]
      ,SRC.[AuthorLastName]
      ,SRC.[AuthorLinkedIn]
      ,SRC.[AuthorMemberFlag]
      ,SRC.[AuthorMemberLogins]
      ,SRC.[AuthorMiddleInitial]
      ,SRC.[AuthorOrganization]
      ,SRC.[AuthorPastAttendeeYesNo]
      ,SRC.[AuthorPastConferencesWhere]
      ,SRC.[AuthorPastConferencesYesNo]
      ,SRC.[AuthorPhotoExtension]
      ,SRC.[AuthorPhotoFileName]
      ,SRC.[AuthorPhotoHeight]
      ,SRC.[AuthorPhotoLocation]
      ,SRC.[AuthorPhotoOriginalName]
      ,SRC.[AuthorPhotoSize]
      ,SRC.[AuthorPhotoWidth]
      ,SRC.[AuthorPosition]
      ,SRC.[AuthorPrefix]
      ,SRC.[AuthorRegisteredFlag]
      ,SRC.[AuthorRegisteredNumber]
      ,SRC.[AuthorState]
      ,SRC.[AuthorSuffix]
      ,SRC.[AuthorTelephoneCell]
      ,SRC.[AuthorTelephoneFax]
      ,SRC.[AuthorTelephoneOffice]
      ,SRC.[AuthorTwitterHandle]
      ,SRC.[AuthorTwitterPage]
      ,SRC.[AuthorWebsite]
      ,SRC.[AuthorZip]
                     )
			

OUTPUT $ACTION AS action
       ,inserted.AUTHORID
       ,deleted.AUTHORID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.cadmium_authors'
       ,'stg.cadmium_authors'
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
			 ,(select count(*) as RowsRead
				from tmp.Cadmium_Authors
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp
			  /************ CADMIUM_SUBMISSIONS ************/
MERGE stg.cadmium_submissions AS DST
USING tmp.cadmium_submissions AS SRC
       ON DST.submitterid = SRC.submitterID AND DST.SUBMISSIONID = SRC.SUBMISSIONID
WHEN MATCHED
             
              AND (
                   
                        ISNULL (DST.[SubmissionStatus], '') <> ISNULL (SRC.[SubmissionStatus], '')
					 OR ISNULL (DST.[SubmissionDateAdded], '') <> ISNULL (SRC.[SubmissionDateAdded], '')
                     OR ISNULL (DST.[SubmissionDateCompleted], '') <> ISNULL (SRC.[SubmissionDateCompleted], '')
                     OR ISNULL (DST.[SubmissionTypeID], '') <> ISNULL (SRC.[SubmissionTypeID], '')
                     OR ISNULL (DST.[SubmissionCategory], '') <> ISNULL (SRC.[SubmissionCategory], '')
                     OR ISNULL (DST.[SubmissionTopic], '') <> ISNULL (SRC.[SubmissionTopic], '')
                     OR ISNULL (DST.[SubmissionTitle], '') <> ISNULL (SRC.[SubmissionTitle], '')
                     OR ISNULL (DST.[SubmissionOAR], '') <> ISNULL (SRC.[SubmissionOAR], '')
                     OR ISNULL (DST.[SubmissionReviewCount], '') <> ISNULL (SRC.[SubmissionReviewCount], '')
                     OR ISNULL (DST.[SubmissionFinalDecision], '') <> ISNULL (SRC.[SubmissionFinalDecision], '')
                     OR ISNULL (DST.[SubmissionLearningObjective1], '') <> ISNULL (SRC.[SubmissionLearningObjective1], '')
                     OR ISNULL (DST.[SubmissionLearningObjective2], '') <> ISNULL (SRC.[SubmissionLearningObjective2], '')
                     OR ISNULL (DST.[SubmissionLearningObjective3], '') <> ISNULL (SRC.[SubmissionLearningObjective3], '')
                     OR ISNULL (DST.[SubmissionLearningObjective4], '') <> ISNULL (SRC.[SubmissionLearningObjective4], '')
                     OR ISNULL (DST.[SubmissionLearningObjective5], '') <> ISNULL (SRC.[SubmissionLearningObjective5], '')
                     OR ISNULL (DST.[SubmissionLearningObjective6], '') <> ISNULL (SRC.[SubmissionLearningObjective6], '')
                     OR ISNULL (DST.[SubmissionLearningObjective7], '') <> ISNULL (SRC.[SubmissionLearningObjective7], '')
                     OR ISNULL (DST.[SubmissionLearningObjective8], '') <> ISNULL (SRC.[SubmissionLearningObjective8], '')
                     OR ISNULL (DST.[SubmissionLearningObjective9], '') <> ISNULL (SRC.[SubmissionLearningObjective9], '')
                     OR ISNULL (DST.[SubmissionLearningObjective10], '') <> ISNULL (SRC.[SubmissionLearningObjective10], '')
                     OR ISNULL (DST.[SubmissionTrackName], '') <> ISNULL (SRC.[SubmissionTrackName], '')
                     OR ISNULL (DST.[SubmissionSessionName], '') <> ISNULL (SRC.[SubmissionSessionName], '')
                     OR ISNULL (DST.[SubmissionSessionTimeStart], '') <> ISNULL (SRC.[SubmissionSessionTimeStart], '')
                     OR ISNULL (DST.[SubmissionSessionTimeEnd], '') <> ISNULL (SRC.[SubmissionSessionTimeEnd], '')
                     OR ISNULL (DST.[SubmissionPresentationDate], '') <> ISNULL (SRC.[SubmissionPresentationDate], '')
                     OR ISNULL (DST.[SubmissionPresentationTimeStart], '') <> ISNULL (SRC.[SubmissionPresentationTimeStart], '')
                     OR ISNULL (DST.[SubmissionPresentationTimeEnd], '') <> ISNULL (SRC.[SubmissionPresentationTimeEnd], '')
                     OR ISNULL (DST.[SubmissionRoom], '') <> ISNULL (SRC.[SubmissionRoom], '')
                     OR ISNULL (DST.[SubmissionCustomLongField1], '') <> ISNULL (SRC.[SubmissionCustomLongField1], '')
                     OR ISNULL (DST.[SubmissionCustomLongField2], '') <> ISNULL (SRC.[SubmissionCustomLongField2], '')
                     OR ISNULL (DST.[SubmissionCustomLongField3], '') <> ISNULL (SRC.[SubmissionCustomLongField3], '')
                     OR ISNULL (DST.[SubmissionCustomLongField4], '') <> ISNULL (SRC.[SubmissionCustomLongField4], '')
                     OR ISNULL (DST.[SubmissionCustomLongField5], '') <> ISNULL (SRC.[SubmissionCustomLongField5], '')
                     OR ISNULL (DST.[SubmissionCustomLongField6], '') <> ISNULL (SRC.[SubmissionCustomLongField6], '')
                     OR ISNULL (DST.[SubmissionCustomLongField7], '') <> ISNULL (SRC.[SubmissionCustomLongField7], '')
                     OR ISNULL (DST.[SubmissionCustomLongField8], '') <> ISNULL (SRC.[SubmissionCustomLongField8], '')
                     OR ISNULL (DST.[SubmissionCustomLongField9], '') <> ISNULL (SRC.[SubmissionCustomLongField9], '')
                     OR ISNULL (DST.[SubmissionCustomLongField10], '') <> ISNULL (SRC.[SubmissionCustomLongField10], '')
                     OR ISNULL (DST.[SubmissionCustomLongField11], '') <> ISNULL (SRC.[SubmissionCustomLongField11], '')
                     OR ISNULL (DST.[SubmissionCustomLongField12], '') <> ISNULL (SRC.[SubmissionCustomLongField12], '')
                     OR ISNULL (DST.[SubmissionCustomLongField13], '') <> ISNULL (SRC.[SubmissionCustomLongField13], '')
                     OR ISNULL (DST.[SubmissionCustomLongField14], '') <> ISNULL (SRC.[SubmissionCustomLongField14], '')
                     OR ISNULL (DST.[SubmissionCustomLongField15], '') <> ISNULL (SRC.[SubmissionCustomLongField15], '')
                     OR ISNULL (DST.[SubmissionCustomLongField16], '') <> ISNULL (SRC.[SubmissionCustomLongField16], '')
                     OR ISNULL (DST.[SubmissionCustomLongField17], '') <> ISNULL (SRC.[SubmissionCustomLongField17], '')
                     OR ISNULL (DST.[SubmissionCustomLongField18], '') <> ISNULL (SRC.[SubmissionCustomLongField18], '')
                     OR ISNULL (DST.[SubmissionCustomLongField19], '') <> ISNULL (SRC.[SubmissionCustomLongField19], '')
                     OR ISNULL (DST.[SubmissionCustomLongField20], '') <> ISNULL (SRC.[SubmissionCustomLongField20], '')
                     OR ISNULL (DST.[SubmissionCustomLongField21], '') <> ISNULL (SRC.[SubmissionCustomLongField21], '')
                     OR ISNULL (DST.[SubmissionCustomLongField22], '') <> ISNULL (SRC.[SubmissionCustomLongField22], '')
                     OR ISNULL (DST.[SubmissionCustomLongField23], '') <> ISNULL (SRC.[SubmissionCustomLongField23], '')
                     OR ISNULL (DST.[SubmissionCustomLongField24], '') <> ISNULL (SRC.[SubmissionCustomLongField24], '')
                     OR ISNULL (DST.[SubmissionCustomLongField25], '') <> ISNULL (SRC.[SubmissionCustomLongField25], '')
                     OR ISNULL (DST.[SubmissionCustomLongField26], '') <> ISNULL (SRC.[SubmissionCustomLongField26], '')
                     OR ISNULL (DST.[SubmissionCustomLongField27], '') <> ISNULL (SRC.[SubmissionCustomLongField27], '')
                     OR ISNULL (DST.[SubmissionCustomLongField28], '') <> ISNULL (SRC.[SubmissionCustomLongField28], '')
                     OR ISNULL (DST.[SubmissionCustomLongField29], '') <> ISNULL (SRC.[SubmissionCustomLongField29], '')
                     OR ISNULL (DST.[SubmissionCustomLongField30], '') <> ISNULL (SRC.[SubmissionCustomLongField30], '')
                     OR ISNULL (DST.[SubmissionCustomLongField31], '') <> ISNULL (SRC.[SubmissionCustomLongField31], '')
                     OR ISNULL (DST.[SubmissionCustomLongField32], '') <> ISNULL (SRC.[SubmissionCustomLongField32], '')
                     OR ISNULL (DST.[SubmissionCustomLongField33], '') <> ISNULL (SRC.[SubmissionCustomLongField33], '')
                     OR ISNULL (DST.[SubmissionCustomLongField34], '') <> ISNULL (SRC.[SubmissionCustomLongField34], '')
                     OR ISNULL (DST.[SubmissionCustomLongField35], '') <> ISNULL (SRC.[SubmissionCustomLongField35], '')
                     OR ISNULL (DST.[SubmissionCustomLongField36], '') <> ISNULL (SRC.[SubmissionCustomLongField36], '')
                     OR ISNULL (DST.[SubmissionCustomLongField37], '') <> ISNULL (SRC.[SubmissionCustomLongField37], '')
                     OR ISNULL (DST.[SubmissionCustomLongField38], '') <> ISNULL (SRC.[SubmissionCustomLongField38], '')
                     OR ISNULL (DST.[SubmissionCustomLongField39], '') <> ISNULL (SRC.[SubmissionCustomLongField39], '')
                     OR ISNULL (DST.[SubmissionCustomLongField40], '') <> ISNULL (SRC.[SubmissionCustomLongField40], '')
                     OR ISNULL (DST.[SubmissionCustomLongField41], '') <> ISNULL (SRC.[SubmissionCustomLongField41], '')
                     OR ISNULL (DST.[SubmissionCustomLongField42], '') <> ISNULL (SRC.[SubmissionCustomLongField42], '')
                     OR ISNULL (DST.[SubmissionCustomLongField43], '') <> ISNULL (SRC.[SubmissionCustomLongField43], '')
                     OR ISNULL (DST.[SubmissionCustomLongField44], '') <> ISNULL (SRC.[SubmissionCustomLongField44], '')
                     OR ISNULL (DST.[SubmissionCustomLongField45], '') <> ISNULL (SRC.[SubmissionCustomLongField45], '')
                     OR ISNULL (DST.[SubmissionCustomLongField46], '') <> ISNULL (SRC.[SubmissionCustomLongField46], '')
                     OR ISNULL (DST.[SubmissionCustomLongField47], '') <> ISNULL (SRC.[SubmissionCustomLongField47], '')
                     OR ISNULL (DST.[SubmissionCustomLongField48], '') <> ISNULL (SRC.[SubmissionCustomLongField48], '')
                     OR ISNULL (DST.[SubmissionCustomLongField49], '') <> ISNULL (SRC.[SubmissionCustomLongField49], '')
                     OR ISNULL (DST.[SubmissionCustomLongField50], '') <> ISNULL (SRC.[SubmissionCustomLongField50], '')
                     OR ISNULL (DST.[SubmissionCustomLongField51], '') <> ISNULL (SRC.[SubmissionCustomLongField51], '')
                     OR ISNULL (DST.[SubmissionCustomLongField52], '') <> ISNULL (SRC.[SubmissionCustomLongField52], '')
                     OR ISNULL (DST.[SubmissionCustomLongField53], '') <> ISNULL (SRC.[SubmissionCustomLongField53], '')
                     OR ISNULL (DST.[SubmissionCustomLongField54], '') <> ISNULL (SRC.[SubmissionCustomLongField54], '')
                     OR ISNULL (DST.[SubmissionCustomLongField55], '') <> ISNULL (SRC.[SubmissionCustomLongField55], '')
                     OR ISNULL (DST.[SubmissionCustomLongField56], '') <> ISNULL (SRC.[SubmissionCustomLongField56], '')
                     OR ISNULL (DST.[SubmissionCustomLongField57], '') <> ISNULL (SRC.[SubmissionCustomLongField57], '')
                     OR ISNULL (DST.[SubmissionCustomLongField58], '') <> ISNULL (SRC.[SubmissionCustomLongField58], '')
                     OR ISNULL (DST.[SubmissionCustomLongField59], '') <> ISNULL (SRC.[SubmissionCustomLongField59], '')
                     OR ISNULL (DST.[SubmissionCustomLongField60], '') <> ISNULL (SRC.[SubmissionCustomLongField60], '')
                     OR ISNULL (DST.[SubmissionCustomLongField61], '') <> ISNULL (SRC.[SubmissionCustomLongField61], '')
                     OR ISNULL (DST.[SubmissionCustomLongField62], '') <> ISNULL (SRC.[SubmissionCustomLongField62], '')
                     OR ISNULL (DST.[SubmissionCustomLongField63], '') <> ISNULL (SRC.[SubmissionCustomLongField63], '')
                     OR ISNULL (DST.[SubmissionCustomLongField64], '') <> ISNULL (SRC.[SubmissionCustomLongField64], '')
                     OR ISNULL (DST.[SubmissionCustomLongField65], '') <> ISNULL (SRC.[SubmissionCustomLongField65], '')
                     OR ISNULL (DST.[SubmissionCustomLongField66], '') <> ISNULL (SRC.[SubmissionCustomLongField66], '')
                     OR ISNULL (DST.[SubmissionCustomLongField67], '') <> ISNULL (SRC.[SubmissionCustomLongField67], '')
                     OR ISNULL (DST.[SubmissionCustomLongField68], '') <> ISNULL (SRC.[SubmissionCustomLongField68], '')
                     OR ISNULL (DST.[SubmissionCustomLongField69], '') <> ISNULL (SRC.[SubmissionCustomLongField69], '')
                     OR ISNULL (DST.[SubmissionCustomLongField70], '') <> ISNULL (SRC.[SubmissionCustomLongField70], '')
                     OR ISNULL (DST.[SubmissionCustomLongField71], '') <> ISNULL (SRC.[SubmissionCustomLongField71], '')
                     OR ISNULL (DST.[SubmissionCustomLongField72], '') <> ISNULL (SRC.[SubmissionCustomLongField72], '')
                     OR ISNULL (DST.[SubmissionCustomLongField73], '') <> ISNULL (SRC.[SubmissionCustomLongField73], '')
                     OR ISNULL (DST.[SubmissionCustomLongField74], '') <> ISNULL (SRC.[SubmissionCustomLongField74], '')
                     OR ISNULL (DST.[SubmissionCustomLongField75], '') <> ISNULL (SRC.[SubmissionCustomLongField75], '')
                     OR ISNULL (DST.[SubmissionCustomLongField76], '') <> ISNULL (SRC.[SubmissionCustomLongField76], '')
                     OR ISNULL (DST.[SubmissionCustomLongField77], '') <> ISNULL (SRC.[SubmissionCustomLongField77], '')
                     OR ISNULL (DST.[SubmissionCustomLongField78], '') <> ISNULL (SRC.[SubmissionCustomLongField78], '')
                     OR ISNULL (DST.[SubmissionCustomLongField79], '') <> ISNULL (SRC.[SubmissionCustomLongField79], '')
                     OR ISNULL (DST.[SubmissionCustomLongField80], '') <> ISNULL (SRC.[SubmissionCustomLongField80], '')
                     OR ISNULL (DST.[SubmissionCustomLongField81], '') <> ISNULL (SRC.[SubmissionCustomLongField81], '')
                     OR ISNULL (DST.[SubmissionCustomLongField82], '') <> ISNULL (SRC.[SubmissionCustomLongField82], '')
                     OR ISNULL (DST.[SubmissionCustomLongField83], '') <> ISNULL (SRC.[SubmissionCustomLongField83], '')
                     OR ISNULL (DST.[SubmissionCustomLongField84], '') <> ISNULL (SRC.[SubmissionCustomLongField84], '')
                     OR ISNULL (DST.[SubmissionCustomLongField85], '') <> ISNULL (SRC.[SubmissionCustomLongField85], '')
                     OR ISNULL (DST.[SubmissionCustomLongField86], '') <> ISNULL (SRC.[SubmissionCustomLongField86], '')
                     OR ISNULL (DST.[SubmissionCustomLongField87], '') <> ISNULL (SRC.[SubmissionCustomLongField87], '')
                     OR ISNULL (DST.[SubmissionCustomLongField88], '') <> ISNULL (SRC.[SubmissionCustomLongField88], '')
                     OR ISNULL (DST.[SubmissionCustomLongField89], '') <> ISNULL (SRC.[SubmissionCustomLongField89], '')
                     OR ISNULL (DST.[SubmissionCustomLongField90], '') <> ISNULL (SRC.[SubmissionCustomLongField90], '')
                     OR ISNULL (DST.[SubmissionCustomLongField91], '') <> ISNULL (SRC.[SubmissionCustomLongField91], '')
                     OR ISNULL (DST.[SubmissionCustomLongField92], '') <> ISNULL (SRC.[SubmissionCustomLongField92], '')
                     OR ISNULL (DST.[SubmissionCustomLongField93], '') <> ISNULL (SRC.[SubmissionCustomLongField93], '')
                     OR ISNULL (DST.[SubmissionCustomLongField94], '') <> ISNULL (SRC.[SubmissionCustomLongField94], '')
                     OR ISNULL (DST.[SubmissionCustomLongField95], '') <> ISNULL (SRC.[SubmissionCustomLongField95], '')
                     OR ISNULL (DST.[SubmissionCustomLongField96], '') <> ISNULL (SRC.[SubmissionCustomLongField96], '')
                     OR ISNULL (DST.[SubmissionCustomLongField97], '') <> ISNULL (SRC.[SubmissionCustomLongField97], '')
                     OR ISNULL (DST.[SubmissionCustomLongField98], '') <> ISNULL (SRC.[SubmissionCustomLongField98], '')
                     OR ISNULL (DST.[SubmissionCustomLongField99], '') <> ISNULL (SRC.[SubmissionCustomLongField99], '')
                     OR ISNULL (DST.[SubmissionCustomLongField100], '') <> ISNULL (SRC.[SubmissionCustomLongField100], '')
                     OR ISNULL (DST.[SubmissionCustomShortField1], '') <> ISNULL (SRC.[SubmissionCustomShortField1], '')
                     OR ISNULL (DST.[SubmissionCustomShortField2], '') <> ISNULL (SRC.[SubmissionCustomShortField2], '')
                     OR ISNULL (DST.[SubmissionCustomShortField3], '') <> ISNULL (SRC.[SubmissionCustomShortField3], '')
                     OR ISNULL (DST.[SubmissionCustomShortField4], '') <> ISNULL (SRC.[SubmissionCustomShortField4], '')
                     OR ISNULL (DST.[SubmissionCustomShortField5], '') <> ISNULL (SRC.[SubmissionCustomShortField5], '')
                     OR ISNULL (DST.[SubmissionCustomShortField6], '') <> ISNULL (SRC.[SubmissionCustomShortField6], '')
                     OR ISNULL (DST.[SubmissionCustomShortField7], '') <> ISNULL (SRC.[SubmissionCustomShortField7], '')
                     OR ISNULL (DST.[SubmissionCustomShortField8], '') <> ISNULL (SRC.[SubmissionCustomShortField8], '')
                     OR ISNULL (DST.[SubmissionCustomShortField9], '') <> ISNULL (SRC.[SubmissionCustomShortField9], '')
                     OR ISNULL (DST.[SubmissionCustomShortField10], '') <> ISNULL (SRC.[SubmissionCustomShortField10], '')
                     OR ISNULL (DST.[SubmissionCustomShortField11], '') <> ISNULL (SRC.[SubmissionCustomShortField11], '')
                     OR ISNULL (DST.[SubmissionCustomShortField12], '') <> ISNULL (SRC.[SubmissionCustomShortField12], '')
                     OR ISNULL (DST.[SubmissionCustomShortField13], '') <> ISNULL (SRC.[SubmissionCustomShortField13], '')
                     OR ISNULL (DST.[SubmissionCustomShortField14], '') <> ISNULL (SRC.[SubmissionCustomShortField14], '')
                     OR ISNULL (DST.[SubmissionCustomShortField15], '') <> ISNULL (SRC.[SubmissionCustomShortField15], '')
                     OR ISNULL (DST.[SubmissionCustomShortField16], '') <> ISNULL (SRC.[SubmissionCustomShortField16], '')
                     OR ISNULL (DST.[SubmissionCustomShortField17], '') <> ISNULL (SRC.[SubmissionCustomShortField17], '')
                     OR ISNULL (DST.[SubmissionCustomShortField18], '') <> ISNULL (SRC.[SubmissionCustomShortField18], '')
                     OR ISNULL (DST.[SubmissionCustomShortField19], '') <> ISNULL (SRC.[SubmissionCustomShortField19], '')
                     OR ISNULL (DST.[SubmissionCustomShortField20], '') <> ISNULL (SRC.[SubmissionCustomShortField20], '')
                     OR ISNULL (DST.[SubmissionCustomShortField21], '') <> ISNULL (SRC.[SubmissionCustomShortField21], '')
                     OR ISNULL (DST.[SubmissionCustomShortField22], '') <> ISNULL (SRC.[SubmissionCustomShortField22], '')
                     OR ISNULL (DST.[SubmissionCustomShortField23], '') <> ISNULL (SRC.[SubmissionCustomShortField23], '')
                     OR ISNULL (DST.[SubmissionCustomShortField24], '') <> ISNULL (SRC.[SubmissionCustomShortField24], '')
                     OR ISNULL (DST.[SubmissionCustomShortField25], '') <> ISNULL (SRC.[SubmissionCustomShortField25], '')
                     OR ISNULL (DST.[SubmissionCustomShortField26], '') <> ISNULL (SRC.[SubmissionCustomShortField26], '')
                     OR ISNULL (DST.[SubmissionCustomShortField27], '') <> ISNULL (SRC.[SubmissionCustomShortField27], '')
                     OR ISNULL (DST.[SubmissionCustomShortField28], '') <> ISNULL (SRC.[SubmissionCustomShortField28], '')
                     OR ISNULL (DST.[SubmissionCustomShortField29], '') <> ISNULL (SRC.[SubmissionCustomShortField29], '')
                     OR ISNULL (DST.[SubmissionCustomShortField30], '') <> ISNULL (SRC.[SubmissionCustomShortField30], '')
                     OR ISNULL (DST.[SubmissionCustomShortField31], '') <> ISNULL (SRC.[SubmissionCustomShortField31], '')
                     OR ISNULL (DST.[SubmissionCustomShortField32], '') <> ISNULL (SRC.[SubmissionCustomShortField32], '')
                     OR ISNULL (DST.[SubmissionCustomShortField33], '') <> ISNULL (SRC.[SubmissionCustomShortField33], '')
                     OR ISNULL (DST.[SubmissionCustomShortField34], '') <> ISNULL (SRC.[SubmissionCustomShortField34], '')
                     OR ISNULL (DST.[SubmissionCustomShortField35], '') <> ISNULL (SRC.[SubmissionCustomShortField35], '')
                     OR ISNULL (DST.[SubmissionCustomShortField36], '') <> ISNULL (SRC.[SubmissionCustomShortField36], '')
                     OR ISNULL (DST.[SubmissionCustomShortField37], '') <> ISNULL (SRC.[SubmissionCustomShortField37], '')
                     OR ISNULL (DST.[SubmissionCustomShortField38], '') <> ISNULL (SRC.[SubmissionCustomShortField38], '')
                     OR ISNULL (DST.[SubmissionCustomShortField39], '') <> ISNULL (SRC.[SubmissionCustomShortField39], '')
                     OR ISNULL (DST.[SubmissionCustomShortField40], '') <> ISNULL (SRC.[SubmissionCustomShortField40], '')
                     OR ISNULL (DST.[SubmissionCustomShortField41], '') <> ISNULL (SRC.[SubmissionCustomShortField41], '')
                     OR ISNULL (DST.[SubmissionCustomShortField42], '') <> ISNULL (SRC.[SubmissionCustomShortField42], '')
                     OR ISNULL (DST.[SubmissionCustomShortField43], '') <> ISNULL (SRC.[SubmissionCustomShortField43], '')
                     OR ISNULL (DST.[SubmissionCustomShortField44], '') <> ISNULL (SRC.[SubmissionCustomShortField44], '')
                     OR ISNULL (DST.[SubmissionCustomShortField45], '') <> ISNULL (SRC.[SubmissionCustomShortField45], '')
                     OR ISNULL (DST.[SubmissionCustomShortField46], '') <> ISNULL (SRC.[SubmissionCustomShortField46], '')
                     OR ISNULL (DST.[SubmissionCustomShortField47], '') <> ISNULL (SRC.[SubmissionCustomShortField47], '')
                     OR ISNULL (DST.[SubmissionCustomShortField48], '') <> ISNULL (SRC.[SubmissionCustomShortField48], '')
                     OR ISNULL (DST.[SubmissionCustomShortField49], '') <> ISNULL (SRC.[SubmissionCustomShortField49], '')
                     OR ISNULL (DST.[SubmissionCustomShortField50], '') <> ISNULL (SRC.[SubmissionCustomShortField50], '')
                     OR ISNULL (DST.[SubmissionCustomShortField51], '') <> ISNULL (SRC.[SubmissionCustomShortField51], '')
                     OR ISNULL (DST.[SubmissionCustomShortField52], '') <> ISNULL (SRC.[SubmissionCustomShortField52], '')
                     OR ISNULL (DST.[SubmissionCustomShortField53], '') <> ISNULL (SRC.[SubmissionCustomShortField53], '')
                     OR ISNULL (DST.[SubmissionCustomShortField54], '') <> ISNULL (SRC.[SubmissionCustomShortField54], '')
                     OR ISNULL (DST.[SubmissionCustomShortField55], '') <> ISNULL (SRC.[SubmissionCustomShortField55], '')
                     OR ISNULL (DST.[SubmissionCustomShortField56], '') <> ISNULL (SRC.[SubmissionCustomShortField56], '')
                     OR ISNULL (DST.[SubmissionCustomShortField57], '') <> ISNULL (SRC.[SubmissionCustomShortField57], '')
                     OR ISNULL (DST.[SubmissionCustomShortField58], '') <> ISNULL (SRC.[SubmissionCustomShortField58], '')
                     OR ISNULL (DST.[SubmissionCustomShortField59], '') <> ISNULL (SRC.[SubmissionCustomShortField59], '')
                     OR ISNULL (DST.[SubmissionCustomShortField60], '') <> ISNULL (SRC.[SubmissionCustomShortField60], '')
                     OR ISNULL (DST.[SubmissionCustomShortField61], '') <> ISNULL (SRC.[SubmissionCustomShortField61], '')
                     OR ISNULL (DST.[SubmissionCustomShortField62], '') <> ISNULL (SRC.[SubmissionCustomShortField62], '')
                     OR ISNULL (DST.[SubmissionCustomShortField63], '') <> ISNULL (SRC.[SubmissionCustomShortField63], '')
                     OR ISNULL (DST.[SubmissionCustomShortField64], '') <> ISNULL (SRC.[SubmissionCustomShortField64], '')
                     OR ISNULL (DST.[SubmissionCustomShortField65], '') <> ISNULL (SRC.[SubmissionCustomShortField65], '')
                     OR ISNULL (DST.[SubmissionCustomShortField66], '') <> ISNULL (SRC.[SubmissionCustomShortField66], '')
                     OR ISNULL (DST.[SubmissionCustomShortField67], '') <> ISNULL (SRC.[SubmissionCustomShortField67], '')
                     OR ISNULL (DST.[SubmissionCustomShortField68], '') <> ISNULL (SRC.[SubmissionCustomShortField68], '')
                     OR ISNULL (DST.[SubmissionCustomShortField69], '') <> ISNULL (SRC.[SubmissionCustomShortField69], '')
                     OR ISNULL (DST.[SubmissionCustomShortField70], '') <> ISNULL (SRC.[SubmissionCustomShortField70], '')
                     OR ISNULL (DST.[SubmissionCustomShortField71], '') <> ISNULL (SRC.[SubmissionCustomShortField71], '')
                     OR ISNULL (DST.[SubmissionCustomShortField72], '') <> ISNULL (SRC.[SubmissionCustomShortField72], '')
                     OR ISNULL (DST.[SubmissionCustomShortField73], '') <> ISNULL (SRC.[SubmissionCustomShortField73], '')
                     OR ISNULL (DST.[SubmissionCustomShortField74], '') <> ISNULL (SRC.[SubmissionCustomShortField74], '')
                     OR ISNULL (DST.[SubmissionCustomShortField75], '') <> ISNULL (SRC.[SubmissionCustomShortField75], '')
                     OR ISNULL (DST.[SubmissionCustomShortField76], '') <> ISNULL (SRC.[SubmissionCustomShortField76], '')
                     OR ISNULL (DST.[SubmissionCustomShortField77], '') <> ISNULL (SRC.[SubmissionCustomShortField77], '')
                     OR ISNULL (DST.[SubmissionCustomShortField78], '') <> ISNULL (SRC.[SubmissionCustomShortField78], '')
                     OR ISNULL (DST.[SubmissionCustomShortField79], '') <> ISNULL (SRC.[SubmissionCustomShortField79], '')
                     OR ISNULL (DST.[SubmissionCustomShortField80], '') <> ISNULL (SRC.[SubmissionCustomShortField80], '')
                     OR ISNULL (DST.[SubmissionCustomShortField81], '') <> ISNULL (SRC.[SubmissionCustomShortField81], '')
                     OR ISNULL (DST.[SubmissionCustomShortField82], '') <> ISNULL (SRC.[SubmissionCustomShortField82], '')
                     OR ISNULL (DST.[SubmissionCustomShortField83], '') <> ISNULL (SRC.[SubmissionCustomShortField83], '')
                     OR ISNULL (DST.[SubmissionCustomShortField84], '') <> ISNULL (SRC.[SubmissionCustomShortField84], '')
                     OR ISNULL (DST.[SubmissionCustomShortField85], '') <> ISNULL (SRC.[SubmissionCustomShortField85], '')
                     OR ISNULL (DST.[SubmissionCustomShortField86], '') <> ISNULL (SRC.[SubmissionCustomShortField86], '')
                     OR ISNULL (DST.[SubmissionCustomShortField87], '') <> ISNULL (SRC.[SubmissionCustomShortField87], '')
                     OR ISNULL (DST.[SubmissionCustomShortField88], '') <> ISNULL (SRC.[SubmissionCustomShortField88], '')
                     OR ISNULL (DST.[SubmissionCustomShortField89], '') <> ISNULL (SRC.[SubmissionCustomShortField89], '')
                     OR ISNULL (DST.[SubmissionCustomShortField90], '') <> ISNULL (SRC.[SubmissionCustomShortField90], '')
                     OR ISNULL (DST.[SubmissionCustomShortField91], '') <> ISNULL (SRC.[SubmissionCustomShortField91], '')
                     OR ISNULL (DST.[SubmissionCustomShortField92], '') <> ISNULL (SRC.[SubmissionCustomShortField92], '')
                     OR ISNULL (DST.[SubmissionCustomShortField93], '') <> ISNULL (SRC.[SubmissionCustomShortField93], '')
                     OR ISNULL (DST.[SubmissionCustomShortField94], '') <> ISNULL (SRC.[SubmissionCustomShortField94], '')
                     OR ISNULL (DST.[SubmissionCustomShortField95], '') <> ISNULL (SRC.[SubmissionCustomShortField95], '')
                     OR ISNULL (DST.[SubmissionCustomShortField96], '') <> ISNULL (SRC.[SubmissionCustomShortField96], '')
                     OR ISNULL (DST.[SubmissionCustomShortField97], '') <> ISNULL (SRC.[SubmissionCustomShortField97], '')
                     OR ISNULL (DST.[SubmissionCustomShortField98], '') <> ISNULL (SRC.[SubmissionCustomShortField98], '')
                     OR ISNULL (DST.[SubmissionCustomShortField99], '') <> ISNULL (SRC.[SubmissionCustomShortField99], '')
                     OR ISNULL (DST.[SubmissionCustomShortField100], '') <> ISNULL (SRC.[SubmissionCustomShortField100], '')
                     OR ISNULL (DST.[SubmissionIDexternal], '') <> ISNULL (SRC.[SubmissionIDexternal], '')
                     OR ISNULL (DST.[SubmissionNumber], '') <> ISNULL (SRC.[SubmissionNumber], '')
                     OR ISNULL (DST.[AuthorAddress1], '') <> ISNULL (SRC.[AuthorAddress1], '')
                     OR ISNULL (DST.[AuthorAddress2], '') <> ISNULL (SRC.[AuthorAddress2], '')
                     OR ISNULL (DST.[AuthorAddress3], '') <> ISNULL (SRC.[AuthorAddress3], '')
                     OR ISNULL (DST.[AuthorAssistantEmail], '') <> ISNULL (SRC.[AuthorAssistantEmail], '')
                     OR ISNULL (DST.[AuthorAssistantName], '') <> ISNULL (SRC.[AuthorAssistantName], '')
                     OR ISNULL (DST.[AuthorAssistantTelephone], '') <> ISNULL (SRC.[AuthorAssistantTelephone], '')
                     OR ISNULL (DST.[AuthorAssociationMemberNumber], '') <> ISNULL (SRC.[AuthorAssociationMemberNumber], '')
                     OR ISNULL (DST.[AuthorAssociationMemberYesNo], '') <> ISNULL (SRC.[AuthorAssociationMemberYesNo], '')
                     OR ISNULL (DST.[AuthorBiographyText], '') <> ISNULL (SRC.[AuthorBiographyText], '')
                     OR ISNULL (DST.[AuthorBiosketchText], '') <> ISNULL (SRC.[AuthorBiosketchText], '')
                     OR ISNULL (DST.[AuthorCity], '') <> ISNULL (SRC.[AuthorCity], '')
                     OR ISNULL (DST.[AuthorCountry], '') <> ISNULL (SRC.[AuthorCountry], '')
                     OR ISNULL (DST.[AuthorCredentials], '') <> ISNULL (SRC.[AuthorCredentials], '')
                     OR ISNULL (DST.[AuthorCustomField1], '') <> ISNULL (SRC.[AuthorCustomField1], '')
                     OR ISNULL (DST.[AuthorCustomField10], '') <> ISNULL (SRC.[AuthorCustomField10], '')
                     OR ISNULL (DST.[AuthorCustomField2], '') <> ISNULL (SRC.[AuthorCustomField2], '')
                     OR ISNULL (DST.[AuthorCustomField3], '') <> ISNULL (SRC.[AuthorCustomField3], '')
                     OR ISNULL (DST.[AuthorCustomField4], '') <> ISNULL (SRC.[AuthorCustomField4], '')
                     OR ISNULL (DST.[AuthorCustomField5], '') <> ISNULL (SRC.[AuthorCustomField5], '')
                     OR ISNULL (DST.[AuthorCustomField6], '') <> ISNULL (SRC.[AuthorCustomField6], '')
                     OR ISNULL (DST.[AuthorCustomField7], '') <> ISNULL (SRC.[AuthorCustomField7], '')
                     OR ISNULL (DST.[AuthorCustomField8], '') <> ISNULL (SRC.[AuthorCustomField8], '')
                     OR ISNULL (DST.[AuthorCustomField9], '') <> ISNULL (SRC.[AuthorCustomField9], '')
                     OR ISNULL (DST.[AuthorDepartment], '') <> ISNULL (SRC.[AuthorDepartment], '')
                     OR ISNULL (DST.[AuthorEmail], '') <> ISNULL (SRC.[AuthorEmail], '')
                     OR ISNULL (DST.[AuthorEmailAlternative], '') <> ISNULL (SRC.[AuthorEmailAlternative], '')
                     OR ISNULL (DST.[AuthorFacebook], '') <> ISNULL (SRC.[AuthorFacebook], '')
                     OR ISNULL (DST.[AuthorFirstName], '') <> ISNULL (SRC.[AuthorFirstName], '')
                     OR ISNULL (DST.[AuthorGooglePlus], '') <> ISNULL (SRC.[AuthorGooglePlus], '')
                     OR ISNULL (DST.[AuthorID], '') <> ISNULL (SRC.[AuthorID], '')
                     OR ISNULL (DST.[AuthorInstagram], '') <> ISNULL (SRC.[AuthorInstagram], '')
                     OR ISNULL (DST.[AuthorKey], '') <> ISNULL (SRC.[AuthorKey], '')
                     OR ISNULL (DST.[AuthorLastName], '') <> ISNULL (SRC.[AuthorLastName], '')
                     OR ISNULL (DST.[AuthorLinkedIn], '') <> ISNULL (SRC.[AuthorLinkedIn], '')
                     OR ISNULL (DST.[AuthorMemberFlag], '') <> ISNULL (SRC.[AuthorMemberFlag], '')
                     OR ISNULL (DST.[AuthorMemberLogins], '') <> ISNULL (SRC.[AuthorMemberLogins], '')
                     OR ISNULL (DST.[AuthorMiddleInitial], '') <> ISNULL (SRC.[AuthorMiddleInitial], '')
                     OR ISNULL (DST.[AuthorOrganization], '') <> ISNULL (SRC.[AuthorOrganization], '')
                     OR ISNULL (DST.[AuthorPastAttendeeYesNo], '') <> ISNULL (SRC.[AuthorPastAttendeeYesNo], '')
                     OR ISNULL (DST.[AuthorPastConferencesWhere], '') <> ISNULL (SRC.[AuthorPastConferencesWhere], '')
                     OR ISNULL (DST.[AuthorPastConferencesYesNo], '') <> ISNULL (SRC.[AuthorPastConferencesYesNo], '')
                     OR ISNULL (DST.[AuthorPhotoExtension], '') <> ISNULL (SRC.[AuthorPhotoExtension], '')
                     OR ISNULL (DST.[AuthorPhotoFileName], '') <> ISNULL (SRC.[AuthorPhotoFileName], '')
                     OR ISNULL (DST.[AuthorPhotoHeight], '') <> ISNULL (SRC.[AuthorPhotoHeight], '')
                     OR ISNULL (DST.[AuthorPhotoLocation], '') <> ISNULL (SRC.[AuthorPhotoLocation], '')
                     OR ISNULL (DST.[AuthorPhotoOriginalName], '') <> ISNULL (SRC.[AuthorPhotoOriginalName], '')
                     OR ISNULL (DST.[AuthorPhotoSize], '') <> ISNULL (SRC.[AuthorPhotoSize], '')
                     OR ISNULL (DST.[AuthorPhotoWidth], '') <> ISNULL (SRC.[AuthorPhotoWidth], '')
                     OR ISNULL (DST.[AuthorPosition], '') <> ISNULL (SRC.[AuthorPosition], '')
                     OR ISNULL (DST.[AuthorPrefix], '') <> ISNULL (SRC.[AuthorPrefix], '')
                     OR ISNULL (DST.[AuthorRegisteredFlag], '') <> ISNULL (SRC.[AuthorRegisteredFlag], '')
                     OR ISNULL (DST.[AuthorRegisteredNumber], '') <> ISNULL (SRC.[AuthorRegisteredNumber], '')
                     OR ISNULL (DST.[AuthorState], '') <> ISNULL (SRC.[AuthorState], '')
                     OR ISNULL (DST.[AuthorSuffix], '') <> ISNULL (SRC.[AuthorSuffix], '')
                     OR ISNULL (DST.[AuthorTelephoneCell], '') <> ISNULL (SRC.[AuthorTelephoneCell], '')
                     OR ISNULL (DST.[AuthorTelephoneFax], '') <> ISNULL (SRC.[AuthorTelephoneFax], '')
                     OR ISNULL (DST.[AuthorTelephoneOffice], '') <> ISNULL (SRC.[AuthorTelephoneOffice], '')
                     OR ISNULL (DST.[AuthorTwitterHandle], '') <> ISNULL (SRC.[AuthorTwitterHandle], '')
                     OR ISNULL (DST.[AuthorTwitterPage], '') <> ISNULL (SRC.[AuthorTwitterPage], '')
                     OR ISNULL (DST.[AuthorWebsite], '') <> ISNULL (SRC.[AuthorWebsite], '')
                     OR ISNULL (DST.[AuthorZip], '') <> ISNULL (SRC.[AuthorZip], '')

                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.[SubmissionStatus] = ISNULL (SRC.[SubmissionStatus], '')
,DST.[SubmissionDateAdded] = ISNULL (SRC.[SubmissionDateAdded], '')
,DST.[SubmissionDateCompleted] = ISNULL (SRC.[SubmissionDateCompleted], '')
,DST.[SubmissionTypeID] = ISNULL (SRC.[SubmissionTypeID], '')
,DST.[SubmissionCategory] = ISNULL (SRC.[SubmissionCategory], '')
,DST.[SubmissionTopic] = ISNULL (SRC.[SubmissionTopic], '')
,DST.[SubmissionTitle] = ISNULL (SRC.[SubmissionTitle], '')
,DST.[SubmissionOAR] = ISNULL (SRC.[SubmissionOAR], '')
,DST.[SubmissionReviewCount] = ISNULL (SRC.[SubmissionReviewCount], '')
,DST.[SubmissionFinalDecision] = ISNULL (SRC.[SubmissionFinalDecision], '')
,DST.[SubmissionLearningObjective1] = ISNULL (SRC.[SubmissionLearningObjective1], '')
,DST.[SubmissionLearningObjective2] = ISNULL (SRC.[SubmissionLearningObjective2], '')
,DST.[SubmissionLearningObjective3] = ISNULL (SRC.[SubmissionLearningObjective3], '')
,DST.[SubmissionLearningObjective4] = ISNULL (SRC.[SubmissionLearningObjective4], '')
,DST.[SubmissionLearningObjective5] = ISNULL (SRC.[SubmissionLearningObjective5], '')
,DST.[SubmissionLearningObjective6] = ISNULL (SRC.[SubmissionLearningObjective6], '')
,DST.[SubmissionLearningObjective7] = ISNULL (SRC.[SubmissionLearningObjective7], '')
,DST.[SubmissionLearningObjective8] = ISNULL (SRC.[SubmissionLearningObjective8], '')
,DST.[SubmissionLearningObjective9] = ISNULL (SRC.[SubmissionLearningObjective9], '')
,DST.[SubmissionLearningObjective10] = ISNULL (SRC.[SubmissionLearningObjective10], '')
,DST.[SubmissionTrackName] = ISNULL (SRC.[SubmissionTrackName], '')
,DST.[SubmissionSessionName] = ISNULL (SRC.[SubmissionSessionName], '')
,DST.[SubmissionSessionTimeStart] = ISNULL (SRC.[SubmissionSessionTimeStart], '')
,DST.[SubmissionSessionTimeEnd] = ISNULL (SRC.[SubmissionSessionTimeEnd], '')
,DST.[SubmissionPresentationDate] = ISNULL (SRC.[SubmissionPresentationDate], '')
,DST.[SubmissionPresentationTimeStart] = ISNULL (SRC.[SubmissionPresentationTimeStart], '')
,DST.[SubmissionPresentationTimeEnd] = ISNULL (SRC.[SubmissionPresentationTimeEnd], '')
,DST.[SubmissionRoom] = ISNULL (SRC.[SubmissionRoom], '')
,DST.[SubmissionCustomLongField1] = ISNULL (SRC.[SubmissionCustomLongField1], '')
,DST.[SubmissionCustomLongField2] = ISNULL (SRC.[SubmissionCustomLongField2], '')
,DST.[SubmissionCustomLongField3] = ISNULL (SRC.[SubmissionCustomLongField3], '')
,DST.[SubmissionCustomLongField4] = ISNULL (SRC.[SubmissionCustomLongField4], '')
,DST.[SubmissionCustomLongField5] = ISNULL (SRC.[SubmissionCustomLongField5], '')
,DST.[SubmissionCustomLongField6] = ISNULL (SRC.[SubmissionCustomLongField6], '')
,DST.[SubmissionCustomLongField7] = ISNULL (SRC.[SubmissionCustomLongField7], '')
,DST.[SubmissionCustomLongField8] = ISNULL (SRC.[SubmissionCustomLongField8], '')
,DST.[SubmissionCustomLongField9] = ISNULL (SRC.[SubmissionCustomLongField9], '')
,DST.[SubmissionCustomLongField10] = ISNULL (SRC.[SubmissionCustomLongField10], '')
,DST.[SubmissionCustomLongField11] = ISNULL (SRC.[SubmissionCustomLongField11], '')
,DST.[SubmissionCustomLongField12] = ISNULL (SRC.[SubmissionCustomLongField12], '')
,DST.[SubmissionCustomLongField13] = ISNULL (SRC.[SubmissionCustomLongField13], '')
,DST.[SubmissionCustomLongField14] = ISNULL (SRC.[SubmissionCustomLongField14], '')
,DST.[SubmissionCustomLongField15] = ISNULL (SRC.[SubmissionCustomLongField15], '')
,DST.[SubmissionCustomLongField16] = ISNULL (SRC.[SubmissionCustomLongField16], '')
,DST.[SubmissionCustomLongField17] = ISNULL (SRC.[SubmissionCustomLongField17], '')
,DST.[SubmissionCustomLongField18] = ISNULL (SRC.[SubmissionCustomLongField18], '')
,DST.[SubmissionCustomLongField19] = ISNULL (SRC.[SubmissionCustomLongField19], '')
,DST.[SubmissionCustomLongField20] = ISNULL (SRC.[SubmissionCustomLongField20], '')
,DST.[SubmissionCustomLongField21] = ISNULL (SRC.[SubmissionCustomLongField21], '')
,DST.[SubmissionCustomLongField22] = ISNULL (SRC.[SubmissionCustomLongField22], '')
,DST.[SubmissionCustomLongField23] = ISNULL (SRC.[SubmissionCustomLongField23], '')
,DST.[SubmissionCustomLongField24] = ISNULL (SRC.[SubmissionCustomLongField24], '')
,DST.[SubmissionCustomLongField25] = ISNULL (SRC.[SubmissionCustomLongField25], '')
,DST.[SubmissionCustomLongField26] = ISNULL (SRC.[SubmissionCustomLongField26], '')
,DST.[SubmissionCustomLongField27] = ISNULL (SRC.[SubmissionCustomLongField27], '')
,DST.[SubmissionCustomLongField28] = ISNULL (SRC.[SubmissionCustomLongField28], '')
,DST.[SubmissionCustomLongField29] = ISNULL (SRC.[SubmissionCustomLongField29], '')
,DST.[SubmissionCustomLongField30] = ISNULL (SRC.[SubmissionCustomLongField30], '')
,DST.[SubmissionCustomLongField31] = ISNULL (SRC.[SubmissionCustomLongField31], '')
,DST.[SubmissionCustomLongField32] = ISNULL (SRC.[SubmissionCustomLongField32], '')
,DST.[SubmissionCustomLongField33] = ISNULL (SRC.[SubmissionCustomLongField33], '')
,DST.[SubmissionCustomLongField34] = ISNULL (SRC.[SubmissionCustomLongField34], '')
,DST.[SubmissionCustomLongField35] = ISNULL (SRC.[SubmissionCustomLongField35], '')
,DST.[SubmissionCustomLongField36] = ISNULL (SRC.[SubmissionCustomLongField36], '')
,DST.[SubmissionCustomLongField37] = ISNULL (SRC.[SubmissionCustomLongField37], '')
,DST.[SubmissionCustomLongField38] = ISNULL (SRC.[SubmissionCustomLongField38], '')
,DST.[SubmissionCustomLongField39] = ISNULL (SRC.[SubmissionCustomLongField39], '')
,DST.[SubmissionCustomLongField40] = ISNULL (SRC.[SubmissionCustomLongField40], '')
,DST.[SubmissionCustomLongField41] = ISNULL (SRC.[SubmissionCustomLongField41], '')
,DST.[SubmissionCustomLongField42] = ISNULL (SRC.[SubmissionCustomLongField42], '')
,DST.[SubmissionCustomLongField43] = ISNULL (SRC.[SubmissionCustomLongField43], '')
,DST.[SubmissionCustomLongField44] = ISNULL (SRC.[SubmissionCustomLongField44], '')
,DST.[SubmissionCustomLongField45] = ISNULL (SRC.[SubmissionCustomLongField45], '')
,DST.[SubmissionCustomLongField46] = ISNULL (SRC.[SubmissionCustomLongField46], '')
,DST.[SubmissionCustomLongField47] = ISNULL (SRC.[SubmissionCustomLongField47], '')
,DST.[SubmissionCustomLongField48] = ISNULL (SRC.[SubmissionCustomLongField48], '')
,DST.[SubmissionCustomLongField49] = ISNULL (SRC.[SubmissionCustomLongField49], '')
,DST.[SubmissionCustomLongField50] = ISNULL (SRC.[SubmissionCustomLongField50], '')
,DST.[SubmissionCustomLongField51] = ISNULL (SRC.[SubmissionCustomLongField51], '')
,DST.[SubmissionCustomLongField52] = ISNULL (SRC.[SubmissionCustomLongField52], '')
,DST.[SubmissionCustomLongField53] = ISNULL (SRC.[SubmissionCustomLongField53], '')
,DST.[SubmissionCustomLongField54] = ISNULL (SRC.[SubmissionCustomLongField54], '')
,DST.[SubmissionCustomLongField55] = ISNULL (SRC.[SubmissionCustomLongField55], '')
,DST.[SubmissionCustomLongField56] = ISNULL (SRC.[SubmissionCustomLongField56], '')
,DST.[SubmissionCustomLongField57] = ISNULL (SRC.[SubmissionCustomLongField57], '')
,DST.[SubmissionCustomLongField58] = ISNULL (SRC.[SubmissionCustomLongField58], '')
,DST.[SubmissionCustomLongField59] = ISNULL (SRC.[SubmissionCustomLongField59], '')
,DST.[SubmissionCustomLongField60] = ISNULL (SRC.[SubmissionCustomLongField60], '')
,DST.[SubmissionCustomLongField61] = ISNULL (SRC.[SubmissionCustomLongField61], '')
,DST.[SubmissionCustomLongField62] = ISNULL (SRC.[SubmissionCustomLongField62], '')
,DST.[SubmissionCustomLongField63] = ISNULL (SRC.[SubmissionCustomLongField63], '')
,DST.[SubmissionCustomLongField64] = ISNULL (SRC.[SubmissionCustomLongField64], '')
,DST.[SubmissionCustomLongField65] = ISNULL (SRC.[SubmissionCustomLongField65], '')
,DST.[SubmissionCustomLongField66] = ISNULL (SRC.[SubmissionCustomLongField66], '')
,DST.[SubmissionCustomLongField67] = ISNULL (SRC.[SubmissionCustomLongField67], '')
,DST.[SubmissionCustomLongField68] = ISNULL (SRC.[SubmissionCustomLongField68], '')
,DST.[SubmissionCustomLongField69] = ISNULL (SRC.[SubmissionCustomLongField69], '')
,DST.[SubmissionCustomLongField70] = ISNULL (SRC.[SubmissionCustomLongField70], '')
,DST.[SubmissionCustomLongField71] = ISNULL (SRC.[SubmissionCustomLongField71], '')
,DST.[SubmissionCustomLongField72] = ISNULL (SRC.[SubmissionCustomLongField72], '')
,DST.[SubmissionCustomLongField73] = ISNULL (SRC.[SubmissionCustomLongField73], '')
,DST.[SubmissionCustomLongField74] = ISNULL (SRC.[SubmissionCustomLongField74], '')
,DST.[SubmissionCustomLongField75] = ISNULL (SRC.[SubmissionCustomLongField75], '')
,DST.[SubmissionCustomLongField76] = ISNULL (SRC.[SubmissionCustomLongField76], '')
,DST.[SubmissionCustomLongField77] = ISNULL (SRC.[SubmissionCustomLongField77], '')
,DST.[SubmissionCustomLongField78] = ISNULL (SRC.[SubmissionCustomLongField78], '')
,DST.[SubmissionCustomLongField79] = ISNULL (SRC.[SubmissionCustomLongField79], '')
,DST.[SubmissionCustomLongField80] = ISNULL (SRC.[SubmissionCustomLongField80], '')
,DST.[SubmissionCustomLongField81] = ISNULL (SRC.[SubmissionCustomLongField81], '')
,DST.[SubmissionCustomLongField82] = ISNULL (SRC.[SubmissionCustomLongField82], '')
,DST.[SubmissionCustomLongField83] = ISNULL (SRC.[SubmissionCustomLongField83], '')
,DST.[SubmissionCustomLongField84] = ISNULL (SRC.[SubmissionCustomLongField84], '')
,DST.[SubmissionCustomLongField85] = ISNULL (SRC.[SubmissionCustomLongField85], '')
,DST.[SubmissionCustomLongField86] = ISNULL (SRC.[SubmissionCustomLongField86], '')
,DST.[SubmissionCustomLongField87] = ISNULL (SRC.[SubmissionCustomLongField87], '')
,DST.[SubmissionCustomLongField88] = ISNULL (SRC.[SubmissionCustomLongField88], '')
,DST.[SubmissionCustomLongField89] = ISNULL (SRC.[SubmissionCustomLongField89], '')
,DST.[SubmissionCustomLongField90] = ISNULL (SRC.[SubmissionCustomLongField90], '')
,DST.[SubmissionCustomLongField91] = ISNULL (SRC.[SubmissionCustomLongField91], '')
,DST.[SubmissionCustomLongField92] = ISNULL (SRC.[SubmissionCustomLongField92], '')
,DST.[SubmissionCustomLongField93] = ISNULL (SRC.[SubmissionCustomLongField93], '')
,DST.[SubmissionCustomLongField94] = ISNULL (SRC.[SubmissionCustomLongField94], '')
,DST.[SubmissionCustomLongField95] = ISNULL (SRC.[SubmissionCustomLongField95], '')
,DST.[SubmissionCustomLongField96] = ISNULL (SRC.[SubmissionCustomLongField96], '')
,DST.[SubmissionCustomLongField97] = ISNULL (SRC.[SubmissionCustomLongField97], '')
,DST.[SubmissionCustomLongField98] = ISNULL (SRC.[SubmissionCustomLongField98], '')
,DST.[SubmissionCustomLongField99] = ISNULL (SRC.[SubmissionCustomLongField99], '')
,DST.[SubmissionCustomLongField100] = ISNULL (SRC.[SubmissionCustomLongField100], '')
,DST.[SubmissionCustomShortField1] = ISNULL (SRC.[SubmissionCustomShortField1], '')
,DST.[SubmissionCustomShortField2] = ISNULL (SRC.[SubmissionCustomShortField2], '')
,DST.[SubmissionCustomShortField3] = ISNULL (SRC.[SubmissionCustomShortField3], '')
,DST.[SubmissionCustomShortField4] = ISNULL (SRC.[SubmissionCustomShortField4], '')
,DST.[SubmissionCustomShortField5] = ISNULL (SRC.[SubmissionCustomShortField5], '')
,DST.[SubmissionCustomShortField6] = ISNULL (SRC.[SubmissionCustomShortField6], '')
,DST.[SubmissionCustomShortField7] = ISNULL (SRC.[SubmissionCustomShortField7], '')
,DST.[SubmissionCustomShortField8] = ISNULL (SRC.[SubmissionCustomShortField8], '')
,DST.[SubmissionCustomShortField9] = ISNULL (SRC.[SubmissionCustomShortField9], '')
,DST.[SubmissionCustomShortField10] = ISNULL (SRC.[SubmissionCustomShortField10], '')
,DST.[SubmissionCustomShortField11] = ISNULL (SRC.[SubmissionCustomShortField11], '')
,DST.[SubmissionCustomShortField12] = ISNULL (SRC.[SubmissionCustomShortField12], '')
,DST.[SubmissionCustomShortField13] = ISNULL (SRC.[SubmissionCustomShortField13], '')
,DST.[SubmissionCustomShortField14] = ISNULL (SRC.[SubmissionCustomShortField14], '')
,DST.[SubmissionCustomShortField15] = ISNULL (SRC.[SubmissionCustomShortField15], '')
,DST.[SubmissionCustomShortField16] = ISNULL (SRC.[SubmissionCustomShortField16], '')
,DST.[SubmissionCustomShortField17] = ISNULL (SRC.[SubmissionCustomShortField17], '')
,DST.[SubmissionCustomShortField18] = ISNULL (SRC.[SubmissionCustomShortField18], '')
,DST.[SubmissionCustomShortField19] = ISNULL (SRC.[SubmissionCustomShortField19], '')
,DST.[SubmissionCustomShortField20] = ISNULL (SRC.[SubmissionCustomShortField20], '')
,DST.[SubmissionCustomShortField21] = ISNULL (SRC.[SubmissionCustomShortField21], '')
,DST.[SubmissionCustomShortField22] = ISNULL (SRC.[SubmissionCustomShortField22], '')
,DST.[SubmissionCustomShortField23] = ISNULL (SRC.[SubmissionCustomShortField23], '')
,DST.[SubmissionCustomShortField24] = ISNULL (SRC.[SubmissionCustomShortField24], '')
,DST.[SubmissionCustomShortField25] = ISNULL (SRC.[SubmissionCustomShortField25], '')
,DST.[SubmissionCustomShortField26] = ISNULL (SRC.[SubmissionCustomShortField26], '')
,DST.[SubmissionCustomShortField27] = ISNULL (SRC.[SubmissionCustomShortField27], '')
,DST.[SubmissionCustomShortField28] = ISNULL (SRC.[SubmissionCustomShortField28], '')
,DST.[SubmissionCustomShortField29] = ISNULL (SRC.[SubmissionCustomShortField29], '')
,DST.[SubmissionCustomShortField30] = ISNULL (SRC.[SubmissionCustomShortField30], '')
,DST.[SubmissionCustomShortField31] = ISNULL (SRC.[SubmissionCustomShortField31], '')
,DST.[SubmissionCustomShortField32] = ISNULL (SRC.[SubmissionCustomShortField32], '')
,DST.[SubmissionCustomShortField33] = ISNULL (SRC.[SubmissionCustomShortField33], '')
,DST.[SubmissionCustomShortField34] = ISNULL (SRC.[SubmissionCustomShortField34], '')
,DST.[SubmissionCustomShortField35] = ISNULL (SRC.[SubmissionCustomShortField35], '')
,DST.[SubmissionCustomShortField36] = ISNULL (SRC.[SubmissionCustomShortField36], '')
,DST.[SubmissionCustomShortField37] = ISNULL (SRC.[SubmissionCustomShortField37], '')
,DST.[SubmissionCustomShortField38] = ISNULL (SRC.[SubmissionCustomShortField38], '')
,DST.[SubmissionCustomShortField39] = ISNULL (SRC.[SubmissionCustomShortField39], '')
,DST.[SubmissionCustomShortField40] = ISNULL (SRC.[SubmissionCustomShortField40], '')
,DST.[SubmissionCustomShortField41] = ISNULL (SRC.[SubmissionCustomShortField41], '')
,DST.[SubmissionCustomShortField42] = ISNULL (SRC.[SubmissionCustomShortField42], '')
,DST.[SubmissionCustomShortField43] = ISNULL (SRC.[SubmissionCustomShortField43], '')
,DST.[SubmissionCustomShortField44] = ISNULL (SRC.[SubmissionCustomShortField44], '')
,DST.[SubmissionCustomShortField45] = ISNULL (SRC.[SubmissionCustomShortField45], '')
,DST.[SubmissionCustomShortField46] = ISNULL (SRC.[SubmissionCustomShortField46], '')
,DST.[SubmissionCustomShortField47] = ISNULL (SRC.[SubmissionCustomShortField47], '')
,DST.[SubmissionCustomShortField48] = ISNULL (SRC.[SubmissionCustomShortField48], '')
,DST.[SubmissionCustomShortField49] = ISNULL (SRC.[SubmissionCustomShortField49], '')
,DST.[SubmissionCustomShortField50] = ISNULL (SRC.[SubmissionCustomShortField50], '')
,DST.[SubmissionCustomShortField51] = ISNULL (SRC.[SubmissionCustomShortField51], '')
,DST.[SubmissionCustomShortField52] = ISNULL (SRC.[SubmissionCustomShortField52], '')
,DST.[SubmissionCustomShortField53] = ISNULL (SRC.[SubmissionCustomShortField53], '')
,DST.[SubmissionCustomShortField54] = ISNULL (SRC.[SubmissionCustomShortField54], '')
,DST.[SubmissionCustomShortField55] = ISNULL (SRC.[SubmissionCustomShortField55], '')
,DST.[SubmissionCustomShortField56] = ISNULL (SRC.[SubmissionCustomShortField56], '')
,DST.[SubmissionCustomShortField57] = ISNULL (SRC.[SubmissionCustomShortField57], '')
,DST.[SubmissionCustomShortField58] = ISNULL (SRC.[SubmissionCustomShortField58], '')
,DST.[SubmissionCustomShortField59] = ISNULL (SRC.[SubmissionCustomShortField59], '')
,DST.[SubmissionCustomShortField60] = ISNULL (SRC.[SubmissionCustomShortField60], '')
,DST.[SubmissionCustomShortField61] = ISNULL (SRC.[SubmissionCustomShortField61], '')
,DST.[SubmissionCustomShortField62] = ISNULL (SRC.[SubmissionCustomShortField62], '')
,DST.[SubmissionCustomShortField63] = ISNULL (SRC.[SubmissionCustomShortField63], '')
,DST.[SubmissionCustomShortField64] = ISNULL (SRC.[SubmissionCustomShortField64], '')
,DST.[SubmissionCustomShortField65] = ISNULL (SRC.[SubmissionCustomShortField65], '')
,DST.[SubmissionCustomShortField66] = ISNULL (SRC.[SubmissionCustomShortField66], '')
,DST.[SubmissionCustomShortField67] = ISNULL (SRC.[SubmissionCustomShortField67], '')
,DST.[SubmissionCustomShortField68] = ISNULL (SRC.[SubmissionCustomShortField68], '')
,DST.[SubmissionCustomShortField69] = ISNULL (SRC.[SubmissionCustomShortField69], '')
,DST.[SubmissionCustomShortField70] = ISNULL (SRC.[SubmissionCustomShortField70], '')
,DST.[SubmissionCustomShortField71] = ISNULL (SRC.[SubmissionCustomShortField71], '')
,DST.[SubmissionCustomShortField72] = ISNULL (SRC.[SubmissionCustomShortField72], '')
,DST.[SubmissionCustomShortField73] = ISNULL (SRC.[SubmissionCustomShortField73], '')
,DST.[SubmissionCustomShortField74] = ISNULL (SRC.[SubmissionCustomShortField74], '')
,DST.[SubmissionCustomShortField75] = ISNULL (SRC.[SubmissionCustomShortField75], '')
,DST.[SubmissionCustomShortField76] = ISNULL (SRC.[SubmissionCustomShortField76], '')
,DST.[SubmissionCustomShortField77] = ISNULL (SRC.[SubmissionCustomShortField77], '')
,DST.[SubmissionCustomShortField78] = ISNULL (SRC.[SubmissionCustomShortField78], '')
,DST.[SubmissionCustomShortField79] = ISNULL (SRC.[SubmissionCustomShortField79], '')
,DST.[SubmissionCustomShortField80] = ISNULL (SRC.[SubmissionCustomShortField80], '')
,DST.[SubmissionCustomShortField81] = ISNULL (SRC.[SubmissionCustomShortField81], '')
,DST.[SubmissionCustomShortField82] = ISNULL (SRC.[SubmissionCustomShortField82], '')
,DST.[SubmissionCustomShortField83] = ISNULL (SRC.[SubmissionCustomShortField83], '')
,DST.[SubmissionCustomShortField84] = ISNULL (SRC.[SubmissionCustomShortField84], '')
,DST.[SubmissionCustomShortField85] = ISNULL (SRC.[SubmissionCustomShortField85], '')
,DST.[SubmissionCustomShortField86] = ISNULL (SRC.[SubmissionCustomShortField86], '')
,DST.[SubmissionCustomShortField87] = ISNULL (SRC.[SubmissionCustomShortField87], '')
,DST.[SubmissionCustomShortField88] = ISNULL (SRC.[SubmissionCustomShortField88], '')
,DST.[SubmissionCustomShortField89] = ISNULL (SRC.[SubmissionCustomShortField89], '')
,DST.[SubmissionCustomShortField90] = ISNULL (SRC.[SubmissionCustomShortField90], '')
,DST.[SubmissionCustomShortField91] = ISNULL (SRC.[SubmissionCustomShortField91], '')
,DST.[SubmissionCustomShortField92] = ISNULL (SRC.[SubmissionCustomShortField92], '')
,DST.[SubmissionCustomShortField93] = ISNULL (SRC.[SubmissionCustomShortField93], '')
,DST.[SubmissionCustomShortField94] = ISNULL (SRC.[SubmissionCustomShortField94], '')
,DST.[SubmissionCustomShortField95] = ISNULL (SRC.[SubmissionCustomShortField95], '')
,DST.[SubmissionCustomShortField96] = ISNULL (SRC.[SubmissionCustomShortField96], '')
,DST.[SubmissionCustomShortField97] = ISNULL (SRC.[SubmissionCustomShortField97], '')
,DST.[SubmissionCustomShortField98] = ISNULL (SRC.[SubmissionCustomShortField98], '')
,DST.[SubmissionCustomShortField99] = ISNULL (SRC.[SubmissionCustomShortField99], '')
,DST.[SubmissionCustomShortField100] = ISNULL (SRC.[SubmissionCustomShortField100], '')
,DST.[SubmissionIDexternal] = ISNULL (SRC.[SubmissionIDexternal], '')
,DST.[SubmissionNumber] = ISNULL (SRC.[SubmissionNumber], '')
,DST.[AuthorAddress1] = ISNULL (SRC.[AuthorAddress1], '')
,DST.[AuthorAddress2] = ISNULL (SRC.[AuthorAddress2], '')
,DST.[AuthorAddress3] = ISNULL (SRC.[AuthorAddress3], '')
,DST.[AuthorAssistantEmail] = ISNULL (SRC.[AuthorAssistantEmail], '')
,DST.[AuthorAssistantName] = ISNULL (SRC.[AuthorAssistantName], '')
,DST.[AuthorAssistantTelephone] = ISNULL (SRC.[AuthorAssistantTelephone], '')
,DST.[AuthorAssociationMemberNumber] = ISNULL (SRC.[AuthorAssociationMemberNumber], '')
,DST.[AuthorAssociationMemberYesNo] = ISNULL (SRC.[AuthorAssociationMemberYesNo], '')
,DST.[AuthorBiographyText] = ISNULL (SRC.[AuthorBiographyText], '')
,DST.[AuthorBiosketchText] = ISNULL (SRC.[AuthorBiosketchText], '')
,DST.[AuthorCity] = ISNULL (SRC.[AuthorCity], '')
,DST.[AuthorCountry] = ISNULL (SRC.[AuthorCountry], '')
,DST.[AuthorCredentials] = ISNULL (SRC.[AuthorCredentials], '')
,DST.[AuthorCustomField1] = ISNULL (SRC.[AuthorCustomField1], '')
,DST.[AuthorCustomField10] = ISNULL (SRC.[AuthorCustomField10], '')
,DST.[AuthorCustomField2] = ISNULL (SRC.[AuthorCustomField2], '')
,DST.[AuthorCustomField3] = ISNULL (SRC.[AuthorCustomField3], '')
,DST.[AuthorCustomField4] = ISNULL (SRC.[AuthorCustomField4], '')
,DST.[AuthorCustomField5] = ISNULL (SRC.[AuthorCustomField5], '')
,DST.[AuthorCustomField6] = ISNULL (SRC.[AuthorCustomField6], '')
,DST.[AuthorCustomField7] = ISNULL (SRC.[AuthorCustomField7], '')
,DST.[AuthorCustomField8] = ISNULL (SRC.[AuthorCustomField8], '')
,DST.[AuthorCustomField9] = ISNULL (SRC.[AuthorCustomField9], '')
,DST.[AuthorDepartment] = ISNULL (SRC.[AuthorDepartment], '')
,DST.[AuthorEmail] = ISNULL (SRC.[AuthorEmail], '')
,DST.[AuthorEmailAlternative] = ISNULL (SRC.[AuthorEmailAlternative], '')
,DST.[AuthorFacebook] = ISNULL (SRC.[AuthorFacebook], '')
,DST.[AuthorFirstName] = ISNULL (SRC.[AuthorFirstName], '')
,DST.[AuthorGooglePlus] = ISNULL (SRC.[AuthorGooglePlus], '')
,DST.[AuthorID] = ISNULL (SRC.[AuthorID], '')
,DST.[AuthorInstagram] = ISNULL (SRC.[AuthorInstagram], '')
,DST.[AuthorKey] = ISNULL (SRC.[AuthorKey], '')
,DST.[AuthorLastName] = ISNULL (SRC.[AuthorLastName], '')
,DST.[AuthorLinkedIn] = ISNULL (SRC.[AuthorLinkedIn], '')
,DST.[AuthorMemberFlag] = ISNULL (SRC.[AuthorMemberFlag], '')
,DST.[AuthorMemberLogins] = ISNULL (SRC.[AuthorMemberLogins], '')
,DST.[AuthorMiddleInitial] = ISNULL (SRC.[AuthorMiddleInitial], '')
,DST.[AuthorOrganization] = ISNULL (SRC.[AuthorOrganization], '')
,DST.[AuthorPastAttendeeYesNo] = ISNULL (SRC.[AuthorPastAttendeeYesNo], '')
,DST.[AuthorPastConferencesWhere] = ISNULL (SRC.[AuthorPastConferencesWhere], '')
,DST.[AuthorPastConferencesYesNo] = ISNULL (SRC.[AuthorPastConferencesYesNo], '')
,DST.[AuthorPhotoExtension] = ISNULL (SRC.[AuthorPhotoExtension], '')
,DST.[AuthorPhotoFileName] = ISNULL (SRC.[AuthorPhotoFileName], '')
,DST.[AuthorPhotoHeight] = ISNULL (SRC.[AuthorPhotoHeight], '')
,DST.[AuthorPhotoLocation] = ISNULL (SRC.[AuthorPhotoLocation], '')
,DST.[AuthorPhotoOriginalName] = ISNULL (SRC.[AuthorPhotoOriginalName], '')
,DST.[AuthorPhotoSize] = ISNULL (SRC.[AuthorPhotoSize], '')
,DST.[AuthorPhotoWidth] = ISNULL (SRC.[AuthorPhotoWidth], '')
,DST.[AuthorPosition] = ISNULL (SRC.[AuthorPosition], '')
,DST.[AuthorPrefix] = ISNULL (SRC.[AuthorPrefix], '')
,DST.[AuthorRegisteredFlag] = ISNULL (SRC.[AuthorRegisteredFlag], '')
,DST.[AuthorRegisteredNumber] = ISNULL (SRC.[AuthorRegisteredNumber], '')
,DST.[AuthorState] = ISNULL (SRC.[AuthorState], '')
,DST.[AuthorSuffix] = ISNULL (SRC.[AuthorSuffix], '')
,DST.[AuthorTelephoneCell] = ISNULL (SRC.[AuthorTelephoneCell], '')
,DST.[AuthorTelephoneFax] = ISNULL (SRC.[AuthorTelephoneFax], '')
,DST.[AuthorTelephoneOffice] = ISNULL (SRC.[AuthorTelephoneOffice], '')
,DST.[AuthorTwitterHandle] = ISNULL (SRC.[AuthorTwitterHandle], '')
,DST.[AuthorTwitterPage] = ISNULL (SRC.[AuthorTwitterPage], '')
,DST.[AuthorWebsite] = ISNULL (SRC.[AuthorWebsite], '')
,DST.[AuthorZip] = ISNULL (SRC.[AuthorZip], '')
											

					 
WHEN NOT MATCHED
       THEN
              INSERT (
			  [submissionid]
  	,[SubmitterID]
,[SubmissionStatus]
,[SubmissionDateAdded]
,[SubmissionDateCompleted]
,[SubmissionTypeID]
,[SubmissionCategory]
,[SubmissionTopic]
,[SubmissionTitle]
,[SubmissionOAR]
,[SubmissionReviewCount]
,[SubmissionFinalDecision]
,[SubmissionLearningObjective1]
,[SubmissionLearningObjective2]
,[SubmissionLearningObjective3]
,[SubmissionLearningObjective4]
,[SubmissionLearningObjective5]
,[SubmissionLearningObjective6]
,[SubmissionLearningObjective7]
,[SubmissionLearningObjective8]
,[SubmissionLearningObjective9]
,[SubmissionLearningObjective10]
,[SubmissionTrackName]
,[SubmissionSessionName]
,[SubmissionSessionTimeStart]
,[SubmissionSessionTimeEnd]
,[SubmissionPresentationDate]
,[SubmissionPresentationTimeStart]
,[SubmissionPresentationTimeEnd]
,[SubmissionRoom]
,[SubmissionCustomLongField1]
,[SubmissionCustomLongField2]
,[SubmissionCustomLongField3]
,[SubmissionCustomLongField4]
,[SubmissionCustomLongField5]
,[SubmissionCustomLongField6]
,[SubmissionCustomLongField7]
,[SubmissionCustomLongField8]
,[SubmissionCustomLongField9]
,[SubmissionCustomLongField10]
,[SubmissionCustomLongField11]
,[SubmissionCustomLongField12]
,[SubmissionCustomLongField13]
,[SubmissionCustomLongField14]
,[SubmissionCustomLongField15]
,[SubmissionCustomLongField16]
,[SubmissionCustomLongField17]
,[SubmissionCustomLongField18]
,[SubmissionCustomLongField19]
,[SubmissionCustomLongField20]
,[SubmissionCustomLongField21]
,[SubmissionCustomLongField22]
,[SubmissionCustomLongField23]
,[SubmissionCustomLongField24]
,[SubmissionCustomLongField25]
,[SubmissionCustomLongField26]
,[SubmissionCustomLongField27]
,[SubmissionCustomLongField28]
,[SubmissionCustomLongField29]
,[SubmissionCustomLongField30]
,[SubmissionCustomLongField31]
,[SubmissionCustomLongField32]
,[SubmissionCustomLongField33]
,[SubmissionCustomLongField34]
,[SubmissionCustomLongField35]
,[SubmissionCustomLongField36]
,[SubmissionCustomLongField37]
,[SubmissionCustomLongField38]
,[SubmissionCustomLongField39]
,[SubmissionCustomLongField40]
,[SubmissionCustomLongField41]
,[SubmissionCustomLongField42]
,[SubmissionCustomLongField43]
,[SubmissionCustomLongField44]
,[SubmissionCustomLongField45]
,[SubmissionCustomLongField46]
,[SubmissionCustomLongField47]
,[SubmissionCustomLongField48]
,[SubmissionCustomLongField49]
,[SubmissionCustomLongField50]
,[SubmissionCustomLongField51]
,[SubmissionCustomLongField52]
,[SubmissionCustomLongField53]
,[SubmissionCustomLongField54]
,[SubmissionCustomLongField55]
,[SubmissionCustomLongField56]
,[SubmissionCustomLongField57]
,[SubmissionCustomLongField58]
,[SubmissionCustomLongField59]
,[SubmissionCustomLongField60]
,[SubmissionCustomLongField61]
,[SubmissionCustomLongField62]
,[SubmissionCustomLongField63]
,[SubmissionCustomLongField64]
,[SubmissionCustomLongField65]
,[SubmissionCustomLongField66]
,[SubmissionCustomLongField67]
,[SubmissionCustomLongField68]
,[SubmissionCustomLongField69]
,[SubmissionCustomLongField70]
,[SubmissionCustomLongField71]
,[SubmissionCustomLongField72]
,[SubmissionCustomLongField73]
,[SubmissionCustomLongField74]
,[SubmissionCustomLongField75]
,[SubmissionCustomLongField76]
,[SubmissionCustomLongField77]
,[SubmissionCustomLongField78]
,[SubmissionCustomLongField79]
,[SubmissionCustomLongField80]
,[SubmissionCustomLongField81]
,[SubmissionCustomLongField82]
,[SubmissionCustomLongField83]
,[SubmissionCustomLongField84]
,[SubmissionCustomLongField85]
,[SubmissionCustomLongField86]
,[SubmissionCustomLongField87]
,[SubmissionCustomLongField88]
,[SubmissionCustomLongField89]
,[SubmissionCustomLongField90]
,[SubmissionCustomLongField91]
,[SubmissionCustomLongField92]
,[SubmissionCustomLongField93]
,[SubmissionCustomLongField94]
,[SubmissionCustomLongField95]
,[SubmissionCustomLongField96]
,[SubmissionCustomLongField97]
,[SubmissionCustomLongField98]
,[SubmissionCustomLongField99]
,[SubmissionCustomLongField100]
,[SubmissionCustomShortField1]
,[SubmissionCustomShortField2]
,[SubmissionCustomShortField3]
,[SubmissionCustomShortField4]
,[SubmissionCustomShortField5]
,[SubmissionCustomShortField6]
,[SubmissionCustomShortField7]
,[SubmissionCustomShortField8]
,[SubmissionCustomShortField9]
,[SubmissionCustomShortField10]
,[SubmissionCustomShortField11]
,[SubmissionCustomShortField12]
,[SubmissionCustomShortField13]
,[SubmissionCustomShortField14]
,[SubmissionCustomShortField15]
,[SubmissionCustomShortField16]
,[SubmissionCustomShortField17]
,[SubmissionCustomShortField18]
,[SubmissionCustomShortField19]
,[SubmissionCustomShortField20]
,[SubmissionCustomShortField21]
,[SubmissionCustomShortField22]
,[SubmissionCustomShortField23]
,[SubmissionCustomShortField24]
,[SubmissionCustomShortField25]
,[SubmissionCustomShortField26]
,[SubmissionCustomShortField27]
,[SubmissionCustomShortField28]
,[SubmissionCustomShortField29]
,[SubmissionCustomShortField30]
,[SubmissionCustomShortField31]
,[SubmissionCustomShortField32]
,[SubmissionCustomShortField33]
,[SubmissionCustomShortField34]
,[SubmissionCustomShortField35]
,[SubmissionCustomShortField36]
,[SubmissionCustomShortField37]
,[SubmissionCustomShortField38]
,[SubmissionCustomShortField39]
,[SubmissionCustomShortField40]
,[SubmissionCustomShortField41]
,[SubmissionCustomShortField42]
,[SubmissionCustomShortField43]
,[SubmissionCustomShortField44]
,[SubmissionCustomShortField45]
,[SubmissionCustomShortField46]
,[SubmissionCustomShortField47]
,[SubmissionCustomShortField48]
,[SubmissionCustomShortField49]
,[SubmissionCustomShortField50]
,[SubmissionCustomShortField51]
,[SubmissionCustomShortField52]
,[SubmissionCustomShortField53]
,[SubmissionCustomShortField54]
,[SubmissionCustomShortField55]
,[SubmissionCustomShortField56]
,[SubmissionCustomShortField57]
,[SubmissionCustomShortField58]
,[SubmissionCustomShortField59]
,[SubmissionCustomShortField60]
,[SubmissionCustomShortField61]
,[SubmissionCustomShortField62]
,[SubmissionCustomShortField63]
,[SubmissionCustomShortField64]
,[SubmissionCustomShortField65]
,[SubmissionCustomShortField66]
,[SubmissionCustomShortField67]
,[SubmissionCustomShortField68]
,[SubmissionCustomShortField69]
,[SubmissionCustomShortField70]
,[SubmissionCustomShortField71]
,[SubmissionCustomShortField72]
,[SubmissionCustomShortField73]
,[SubmissionCustomShortField74]
,[SubmissionCustomShortField75]
,[SubmissionCustomShortField76]
,[SubmissionCustomShortField77]
,[SubmissionCustomShortField78]
,[SubmissionCustomShortField79]
,[SubmissionCustomShortField80]
,[SubmissionCustomShortField81]
,[SubmissionCustomShortField82]
,[SubmissionCustomShortField83]
,[SubmissionCustomShortField84]
,[SubmissionCustomShortField85]
,[SubmissionCustomShortField86]
,[SubmissionCustomShortField87]
,[SubmissionCustomShortField88]
,[SubmissionCustomShortField89]
,[SubmissionCustomShortField90]
,[SubmissionCustomShortField91]
,[SubmissionCustomShortField92]
,[SubmissionCustomShortField93]
,[SubmissionCustomShortField94]
,[SubmissionCustomShortField95]
,[SubmissionCustomShortField96]
,[SubmissionCustomShortField97]
,[SubmissionCustomShortField98]
,[SubmissionCustomShortField99]
,[SubmissionCustomShortField100]
,[SubmissionIDexternal]
,[SubmissionNumber]
,[AuthorAddress1]
,[AuthorAddress2]
,[AuthorAddress3]
,[AuthorAssistantEmail]
,[AuthorAssistantName]
,[AuthorAssistantTelephone]
,[AuthorAssociationMemberNumber]
,[AuthorAssociationMemberYesNo]
,[AuthorBiographyText]
,[AuthorBiosketchText]
,[AuthorCity]
,[AuthorCountry]
,[AuthorCredentials]
,[AuthorCustomField1]
,[AuthorCustomField10]
,[AuthorCustomField2]
,[AuthorCustomField3]
,[AuthorCustomField4]
,[AuthorCustomField5]
,[AuthorCustomField6]
,[AuthorCustomField7]
,[AuthorCustomField8]
,[AuthorCustomField9]
,[AuthorDepartment]
,[AuthorEmail]
,[AuthorEmailAlternative]
,[AuthorFacebook]
,[AuthorFirstName]
,[AuthorGooglePlus]
,[AuthorID]
,[AuthorInstagram]
,[AuthorKey]
,[AuthorLastName]
,[AuthorLinkedIn]
,[AuthorMemberFlag]
,[AuthorMemberLogins]
,[AuthorMiddleInitial]
,[AuthorOrganization]
,[AuthorPastAttendeeYesNo]
,[AuthorPastConferencesWhere]
,[AuthorPastConferencesYesNo]
,[AuthorPhotoExtension]
,[AuthorPhotoFileName]
,[AuthorPhotoHeight]
,[AuthorPhotoLocation]
,[AuthorPhotoOriginalName]
,[AuthorPhotoSize]
,[AuthorPhotoWidth]
,[AuthorPosition]
,[AuthorPrefix]
,[AuthorRegisteredFlag]
,[AuthorRegisteredNumber]
,[AuthorState]
,[AuthorSuffix]
,[AuthorTelephoneCell]
,[AuthorTelephoneFax]
,[AuthorTelephoneOffice]
,[AuthorTwitterHandle]
,[AuthorTwitterPage]
,[AuthorWebsite]
,[AuthorZip]
	

)
          

              VALUES (
SRC.[submissionid]
,SRC.[SubmitterID]
,SRC.[SubmissionStatus]
,SRC.[SubmissionDateAdded]
,SRC.[SubmissionDateCompleted]
,SRC.[SubmissionTypeID]
,SRC.[SubmissionCategory]
,SRC.[SubmissionTopic]
,SRC.[SubmissionTitle]
,SRC.[SubmissionOAR]
,SRC.[SubmissionReviewCount]
,SRC.[SubmissionFinalDecision]
,SRC.[SubmissionLearningObjective1]
,SRC.[SubmissionLearningObjective2]
,SRC.[SubmissionLearningObjective3]
,SRC.[SubmissionLearningObjective4]
,SRC.[SubmissionLearningObjective5]
,SRC.[SubmissionLearningObjective6]
,SRC.[SubmissionLearningObjective7]
,SRC.[SubmissionLearningObjective8]
,SRC.[SubmissionLearningObjective9]
,SRC.[SubmissionLearningObjective10]
,SRC.[SubmissionTrackName]
,SRC.[SubmissionSessionName]
,SRC.[SubmissionSessionTimeStart]
,SRC.[SubmissionSessionTimeEnd]
,SRC.[SubmissionPresentationDate]
,SRC.[SubmissionPresentationTimeStart]
,SRC.[SubmissionPresentationTimeEnd]
,SRC.[SubmissionRoom]
,SRC.[SubmissionCustomLongField1]
,SRC.[SubmissionCustomLongField2]
,SRC.[SubmissionCustomLongField3]
,SRC.[SubmissionCustomLongField4]
,SRC.[SubmissionCustomLongField5]
,SRC.[SubmissionCustomLongField6]
,SRC.[SubmissionCustomLongField7]
,SRC.[SubmissionCustomLongField8]
,SRC.[SubmissionCustomLongField9]
,SRC.[SubmissionCustomLongField10]
,SRC.[SubmissionCustomLongField11]
,SRC.[SubmissionCustomLongField12]
,SRC.[SubmissionCustomLongField13]
,SRC.[SubmissionCustomLongField14]
,SRC.[SubmissionCustomLongField15]
,SRC.[SubmissionCustomLongField16]
,SRC.[SubmissionCustomLongField17]
,SRC.[SubmissionCustomLongField18]
,SRC.[SubmissionCustomLongField19]
,SRC.[SubmissionCustomLongField20]
,SRC.[SubmissionCustomLongField21]
,SRC.[SubmissionCustomLongField22]
,SRC.[SubmissionCustomLongField23]
,SRC.[SubmissionCustomLongField24]
,SRC.[SubmissionCustomLongField25]
,SRC.[SubmissionCustomLongField26]
,SRC.[SubmissionCustomLongField27]
,SRC.[SubmissionCustomLongField28]
,SRC.[SubmissionCustomLongField29]
,SRC.[SubmissionCustomLongField30]
,SRC.[SubmissionCustomLongField31]
,SRC.[SubmissionCustomLongField32]
,SRC.[SubmissionCustomLongField33]
,SRC.[SubmissionCustomLongField34]
,SRC.[SubmissionCustomLongField35]
,SRC.[SubmissionCustomLongField36]
,SRC.[SubmissionCustomLongField37]
,SRC.[SubmissionCustomLongField38]
,SRC.[SubmissionCustomLongField39]
,SRC.[SubmissionCustomLongField40]
,SRC.[SubmissionCustomLongField41]
,SRC.[SubmissionCustomLongField42]
,SRC.[SubmissionCustomLongField43]
,SRC.[SubmissionCustomLongField44]
,SRC.[SubmissionCustomLongField45]
,SRC.[SubmissionCustomLongField46]
,SRC.[SubmissionCustomLongField47]
,SRC.[SubmissionCustomLongField48]
,SRC.[SubmissionCustomLongField49]
,SRC.[SubmissionCustomLongField50]
,SRC.[SubmissionCustomLongField51]
,SRC.[SubmissionCustomLongField52]
,SRC.[SubmissionCustomLongField53]
,SRC.[SubmissionCustomLongField54]
,SRC.[SubmissionCustomLongField55]
,SRC.[SubmissionCustomLongField56]
,SRC.[SubmissionCustomLongField57]
,SRC.[SubmissionCustomLongField58]
,SRC.[SubmissionCustomLongField59]
,SRC.[SubmissionCustomLongField60]
,SRC.[SubmissionCustomLongField61]
,SRC.[SubmissionCustomLongField62]
,SRC.[SubmissionCustomLongField63]
,SRC.[SubmissionCustomLongField64]
,SRC.[SubmissionCustomLongField65]
,SRC.[SubmissionCustomLongField66]
,SRC.[SubmissionCustomLongField67]
,SRC.[SubmissionCustomLongField68]
,SRC.[SubmissionCustomLongField69]
,SRC.[SubmissionCustomLongField70]
,SRC.[SubmissionCustomLongField71]
,SRC.[SubmissionCustomLongField72]
,SRC.[SubmissionCustomLongField73]
,SRC.[SubmissionCustomLongField74]
,SRC.[SubmissionCustomLongField75]
,SRC.[SubmissionCustomLongField76]
,SRC.[SubmissionCustomLongField77]
,SRC.[SubmissionCustomLongField78]
,SRC.[SubmissionCustomLongField79]
,SRC.[SubmissionCustomLongField80]
,SRC.[SubmissionCustomLongField81]
,SRC.[SubmissionCustomLongField82]
,SRC.[SubmissionCustomLongField83]
,SRC.[SubmissionCustomLongField84]
,SRC.[SubmissionCustomLongField85]
,SRC.[SubmissionCustomLongField86]
,SRC.[SubmissionCustomLongField87]
,SRC.[SubmissionCustomLongField88]
,SRC.[SubmissionCustomLongField89]
,SRC.[SubmissionCustomLongField90]
,SRC.[SubmissionCustomLongField91]
,SRC.[SubmissionCustomLongField92]
,SRC.[SubmissionCustomLongField93]
,SRC.[SubmissionCustomLongField94]
,SRC.[SubmissionCustomLongField95]
,SRC.[SubmissionCustomLongField96]
,SRC.[SubmissionCustomLongField97]
,SRC.[SubmissionCustomLongField98]
,SRC.[SubmissionCustomLongField99]
,SRC.[SubmissionCustomLongField100]
,SRC.[SubmissionCustomShortField1]
,SRC.[SubmissionCustomShortField2]
,SRC.[SubmissionCustomShortField3]
,SRC.[SubmissionCustomShortField4]
,SRC.[SubmissionCustomShortField5]
,SRC.[SubmissionCustomShortField6]
,SRC.[SubmissionCustomShortField7]
,SRC.[SubmissionCustomShortField8]
,SRC.[SubmissionCustomShortField9]
,SRC.[SubmissionCustomShortField10]
,SRC.[SubmissionCustomShortField11]
,SRC.[SubmissionCustomShortField12]
,SRC.[SubmissionCustomShortField13]
,SRC.[SubmissionCustomShortField14]
,SRC.[SubmissionCustomShortField15]
,SRC.[SubmissionCustomShortField16]
,SRC.[SubmissionCustomShortField17]
,SRC.[SubmissionCustomShortField18]
,SRC.[SubmissionCustomShortField19]
,SRC.[SubmissionCustomShortField20]
,SRC.[SubmissionCustomShortField21]
,SRC.[SubmissionCustomShortField22]
,SRC.[SubmissionCustomShortField23]
,SRC.[SubmissionCustomShortField24]
,SRC.[SubmissionCustomShortField25]
,SRC.[SubmissionCustomShortField26]
,SRC.[SubmissionCustomShortField27]
,SRC.[SubmissionCustomShortField28]
,SRC.[SubmissionCustomShortField29]
,SRC.[SubmissionCustomShortField30]
,SRC.[SubmissionCustomShortField31]
,SRC.[SubmissionCustomShortField32]
,SRC.[SubmissionCustomShortField33]
,SRC.[SubmissionCustomShortField34]
,SRC.[SubmissionCustomShortField35]
,SRC.[SubmissionCustomShortField36]
,SRC.[SubmissionCustomShortField37]
,SRC.[SubmissionCustomShortField38]
,SRC.[SubmissionCustomShortField39]
,SRC.[SubmissionCustomShortField40]
,SRC.[SubmissionCustomShortField41]
,SRC.[SubmissionCustomShortField42]
,SRC.[SubmissionCustomShortField43]
,SRC.[SubmissionCustomShortField44]
,SRC.[SubmissionCustomShortField45]
,SRC.[SubmissionCustomShortField46]
,SRC.[SubmissionCustomShortField47]
,SRC.[SubmissionCustomShortField48]
,SRC.[SubmissionCustomShortField49]
,SRC.[SubmissionCustomShortField50]
,SRC.[SubmissionCustomShortField51]
,SRC.[SubmissionCustomShortField52]
,SRC.[SubmissionCustomShortField53]
,SRC.[SubmissionCustomShortField54]
,SRC.[SubmissionCustomShortField55]
,SRC.[SubmissionCustomShortField56]
,SRC.[SubmissionCustomShortField57]
,SRC.[SubmissionCustomShortField58]
,SRC.[SubmissionCustomShortField59]
,SRC.[SubmissionCustomShortField60]
,SRC.[SubmissionCustomShortField61]
,SRC.[SubmissionCustomShortField62]
,SRC.[SubmissionCustomShortField63]
,SRC.[SubmissionCustomShortField64]
,SRC.[SubmissionCustomShortField65]
,SRC.[SubmissionCustomShortField66]
,SRC.[SubmissionCustomShortField67]
,SRC.[SubmissionCustomShortField68]
,SRC.[SubmissionCustomShortField69]
,SRC.[SubmissionCustomShortField70]
,SRC.[SubmissionCustomShortField71]
,SRC.[SubmissionCustomShortField72]
,SRC.[SubmissionCustomShortField73]
,SRC.[SubmissionCustomShortField74]
,SRC.[SubmissionCustomShortField75]
,SRC.[SubmissionCustomShortField76]
,SRC.[SubmissionCustomShortField77]
,SRC.[SubmissionCustomShortField78]
,SRC.[SubmissionCustomShortField79]
,SRC.[SubmissionCustomShortField80]
,SRC.[SubmissionCustomShortField81]
,SRC.[SubmissionCustomShortField82]
,SRC.[SubmissionCustomShortField83]
,SRC.[SubmissionCustomShortField84]
,SRC.[SubmissionCustomShortField85]
,SRC.[SubmissionCustomShortField86]
,SRC.[SubmissionCustomShortField87]
,SRC.[SubmissionCustomShortField88]
,SRC.[SubmissionCustomShortField89]
,SRC.[SubmissionCustomShortField90]
,SRC.[SubmissionCustomShortField91]
,SRC.[SubmissionCustomShortField92]
,SRC.[SubmissionCustomShortField93]
,SRC.[SubmissionCustomShortField94]
,SRC.[SubmissionCustomShortField95]
,SRC.[SubmissionCustomShortField96]
,SRC.[SubmissionCustomShortField97]
,SRC.[SubmissionCustomShortField98]
,SRC.[SubmissionCustomShortField99]
,SRC.[SubmissionCustomShortField100]
,SRC.[SubmissionIDexternal]
,SRC.[SubmissionNumber]
,SRC.[AuthorAddress1]
,SRC.[AuthorAddress2]
,SRC.[AuthorAddress3]
,SRC.[AuthorAssistantEmail]
,SRC.[AuthorAssistantName]
,SRC.[AuthorAssistantTelephone]
,SRC.[AuthorAssociationMemberNumber]
,SRC.[AuthorAssociationMemberYesNo]
,SRC.[AuthorBiographyText]
,SRC.[AuthorBiosketchText]
,SRC.[AuthorCity]
,SRC.[AuthorCountry]
,SRC.[AuthorCredentials]
,SRC.[AuthorCustomField1]
,SRC.[AuthorCustomField10]
,SRC.[AuthorCustomField2]
,SRC.[AuthorCustomField3]
,SRC.[AuthorCustomField4]
,SRC.[AuthorCustomField5]
,SRC.[AuthorCustomField6]
,SRC.[AuthorCustomField7]
,SRC.[AuthorCustomField8]
,SRC.[AuthorCustomField9]
,SRC.[AuthorDepartment]
,SRC.[AuthorEmail]
,SRC.[AuthorEmailAlternative]
,SRC.[AuthorFacebook]
,SRC.[AuthorFirstName]
,SRC.[AuthorGooglePlus]
,SRC.[AuthorID]
,SRC.[AuthorInstagram]
,SRC.[AuthorKey]
,SRC.[AuthorLastName]
,SRC.[AuthorLinkedIn]
,SRC.[AuthorMemberFlag]
,SRC.[AuthorMemberLogins]
,SRC.[AuthorMiddleInitial]
,SRC.[AuthorOrganization]
,SRC.[AuthorPastAttendeeYesNo]
,SRC.[AuthorPastConferencesWhere]
,SRC.[AuthorPastConferencesYesNo]
,SRC.[AuthorPhotoExtension]
,SRC.[AuthorPhotoFileName]
,SRC.[AuthorPhotoHeight]
,SRC.[AuthorPhotoLocation]
,SRC.[AuthorPhotoOriginalName]
,SRC.[AuthorPhotoSize]
,SRC.[AuthorPhotoWidth]
,SRC.[AuthorPosition]
,SRC.[AuthorPrefix]
,SRC.[AuthorRegisteredFlag]
,SRC.[AuthorRegisteredNumber]
,SRC.[AuthorState]
,SRC.[AuthorSuffix]
,SRC.[AuthorTelephoneCell]
,SRC.[AuthorTelephoneFax]
,SRC.[AuthorTelephoneOffice]
,SRC.[AuthorTwitterHandle]
,SRC.[AuthorTwitterPage]
,SRC.[AuthorWebsite]
,SRC.[AuthorZip]
	

                     )
			

OUTPUT $ACTION AS action
       ,inserted.SUBMITTERID
       ,deleted.SUBMITTERID
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
	   ,'tmp.cadmium_submissions'
       ,'stg.cadmium_submissions'
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'INSERT'
                     GROUP BY action
                     ) X
              )
       ,(
              SELECT action_Count
              FROM (
                     SELECT action
                           ,count(*) AS action_count
                     FROM #audittemp
                     WHERE action = 'UPDATE'
                     GROUP BY action
                     ) X
					 )
			 ,(select count(*) as RowsRead
				from tmp.cadmium_submissions
				) 
       ,getdate()
       )


			  Truncate TABLE #audittemp
/**************************************************table**********************/

COMMIT 
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
	ROLLBACK
	END
SET @ErrorMessage  = ERROR_MESSAGE();
    SET @ErrorSeverity = ERROR_SEVERITY();
    SET @ErrorState    = ERROR_STATE();
    THROW;
END CATCH;
END