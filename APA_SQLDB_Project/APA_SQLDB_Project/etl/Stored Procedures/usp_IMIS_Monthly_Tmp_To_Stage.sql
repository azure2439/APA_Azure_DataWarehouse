



CREATE procedure [etl].[usp_IMIS_Monthly_Tmp_To_Stage]
@PipelineName varchar(60) = 'ssms'
as


DECLARE @Yesterday DATETIME = DATEADD(dd, - 1, GETDATE())
DECLARE @Today DATETIME = GETDATE()

IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
       Truncate TABLE #audittemp

CREATE TABLE #audittemp (
       action NVARCHAR(20)
       ,inserted_id varchar(80)
       ,deleted_id varchar(80)
       );

DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @ErrorSeverity NVARCHAR(MAX)

BEGIN TRY
DECLARE @ErrorState tinyint




/***********************************************DIRECT_DEBIT**********************************************/



--IF OBJECT_ID('tempdb..#audittemp') IS NOT NULL
--       Truncate TABLE #audittemp

--CREATE TABLE #audittemp (
--       action NVARCHAR(20)
--       ,inserted_id varchar(30)
--       ,deleted_id varchar(30)
--       );

MERGE stg.imis_Direct_Debit AS DST
USING tmp.imis_Direct_Debit AS SRC
       ON DST.ID = SRC.ID 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.AUTO_DEBIT, '') <> ISNULL(SRC.AUTO_DEBIT, '')
                     OR ISNULL(DST.[START_DATE], '') <> ISNULL(SRC.[START_DATE], '')
                     OR ISNULL(DST.ACCOUNT_NUM, '') <> ISNULL(SRC.ACCOUNT_NUM, '')
                     OR ISNULL(DST.ABA_ROUTING, '') <> ISNULL(SRC.ABA_ROUTING, '')
                     OR ISNULL(DST.MONTHLY_PAYMENT, '') <> ISNULL(SRC.MONTHLY_PAYMENT, '')
                     OR ISNULL(DST.BANK_NAME, '') <> ISNULL(SRC.BANK_NAME , '')
                     OR ISNULL(DST.REMAIN_DEBIT, '') <> ISNULL(SRC.REMAIN_DEBIT, '')
                     OR ISNULL(DST.TOTAL_DEBIT, '') <> ISNULL(SRC.TOTAL_DEBIT , '')
					 OR ISNULL(DST.NUM_DEBIT, '') <> ISNULL(SRC.NUM_DEBIT , '')
                     OR ISNULL(DST.REMAIN_TIMES, '') <> ISNULL(SRC.REMAIN_TIMES , '')
					 OR ISNULL(DST.RUN_DATE, '') <> ISNULL(SRC.RUN_DATE , '')
					 OR ISNULL(DST.AT_CHECKING, '') <> ISNULL(SRC.AT_CHECKING , '')
					 OR ISNULL(DST.AT_SAVING, '') <> ISNULL(SRC.AT_SAVING, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
       ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                     )

              VALUES (
       SRC.ID
      ,SRC.AUTO_DEBIT
      ,SRC.[START_DATE]
      ,SRC.ACCOUNT_NUM
      ,SRC.ABA_ROUTING
      ,SRC.MONTHLY_PAYMENT
      ,SRC.BANK_NAME
      ,SRC.REMAIN_DEBIT
      ,SRC.TOTAL_DEBIT
      ,SRC.NUM_DEBIT
      ,SRC.REMAIN_TIMES
      ,SRC.RUN_DATE
      ,SRC.AT_CHECKING
      ,SRC.AT_SAVING
      ,SRC.TIME_STAMP
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ID
       ,deleted.ID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
       '@PipelineName'
	   ,'tmp.imis_Direct_Debit'
       ,'stg.imis_Direct_Debit'
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
				from tmp.imis_Direct_Debit
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Direct_Debit (
ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,TIME_STAMP
      ,IsActive
      ,StartDate
                 )
select  
                     ID
      ,AUTO_DEBIT
      ,[START_DATE]
      ,ACCOUNT_NUM
      ,ABA_ROUTING
      ,MONTHLY_PAYMENT
      ,BANK_NAME
      ,REMAIN_DEBIT
      ,TOTAL_DEBIT
      ,NUM_DEBIT
      ,REMAIN_TIMES
      ,RUN_DATE
      ,AT_CHECKING
      ,AT_SAVING
      ,cast(TIME_STAMP as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Direct_Debit
WHERE ID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp







/**************************************************ContactMain******************************/
MERGE stg.imis_ContactMain AS DST
USING tmp.imis_ContactMain AS SRC
       ON DST.ContactKey = SRC.ContactKey
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.CONTACTSTATUSCODE, '') <> ISNULL(SRC.CONTACTSTATUSCODE, '')
                     OR ISNULL(DST.FULLNAME, '') <> ISNULL(SRC.FULLNAME, '')
                     OR ISNULL(DST.SORTNAME, '') <> ISNULL(SRC.SORTNAME, '')
                     OR ISNULL(DST.ISINSTITUTE, '') <> ISNULL(SRC.ISINSTITUTE, '')
                     OR ISNULL(DST.TAXIDNUMBER, '') <> ISNULL(SRC.TAXIDNUMBER, '')
					 OR ISNULL(DST.NOSOLICITATIONFLAG, '') <> ISNULL(SRC.NOSOLICITATIONFLAG, '')
					 OR ISNULL(DST.SYNCCONTACTID, '') <> ISNULL(SRC.SYNCCONTACTID, '')
					 OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
					 OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
					 OR ISNULL(DST.ISIDEDITABLE, '') <> ISNULL(SRC.ISIDEDITABLE, '')
					 OR ISNULL(DST.ID, '') <> ISNULL(SRC.ID, '')
					 OR ISNULL(DST.PREFERREDADDRESSCATEGORYCODE, '') <> ISNULL(SRC.PREFERREDADDRESSCATEGORYCODE, '')
					 OR ISNULL(DST.ISSORTNAMEOVERRIDDEN, '') <> ISNULL(SRC.ISSORTNAMEOVERRIDDEN, '')
					 OR ISNULL(DST.PRIMARYMEMBERSHIPGROUPKEY, '') <> ISNULL(SRC.PRIMARYMEMBERSHIPGROUPKEY, '')
					 OR ISNULL(DST.MAJORKEY, '') <> ISNULL(SRC.MAJORKEY, '')
					 OR ISNULL(DST.ACCESSKEY, '') <> ISNULL(SRC.ACCESSKEY, '')
					 OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
					 OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
					 OR ISNULL(DST.TEXTONLYEMAILFLAG, '') <> ISNULL(SRC.TEXTONLYEMAILFLAG, '')
					 OR ISNULL(DST.CONTACTTYPEKEY, '') <> ISNULL(SRC.CONTACTTYPEKEY, '')
					 OR ISNULL(DST.OPTOUTFLAG, '') <> ISNULL(SRC.OPTOUTFLAG, '')
					 OR ISNULL(DST.MARKEDFORDELETEON, '') <> ISNULL(SRC.MARKEDFORDELETEON, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ContactKey]
      ,[ContactStatusCode]
      ,[FullName]
      ,[SortName]
      ,[IsInstitute]
      ,[TaxIDNumber]
      ,[NoSolicitationFlag]
      ,[SyncContactID]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,[IsIDEditable]
      ,[ID]
      ,[PreferredAddressCategoryCode]
      ,[IsSortNameOverridden]
      ,[PrimaryMembershipGroupKey]
      ,[MajorKey]
      ,[AccessKey]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[TextOnlyEmailFlag]
      ,[ContactTypeKey]
      ,[OptOutFlag]
      ,[MarkedForDeleteOn]
      ,[IsActive]
      ,[StartDate]
 
          
                     )
              VALUES (
                   SRC.[ContactKey]
      ,SRC.[ContactStatusCode]
      ,SRC.[FullName]
      ,SRC.[SortName]
      ,SRC.[IsInstitute]
      ,SRC.[TaxIDNumber]
      ,SRC.[NoSolicitationFlag]
      ,SRC.[SyncContactID]
      ,SRC.[UpdatedOn]
      ,SRC.[UpdatedByUserKey]
      ,SRC.[IsIDEditable]
      ,SRC.[ID]
      ,SRC.[PreferredAddressCategoryCode]
      ,SRC.[IsSortNameOverridden]
      ,SRC.[PrimaryMembershipGroupKey]
      ,SRC.[MajorKey]
      ,SRC.[AccessKey]
      ,SRC.[CreatedByUserKey]
      ,SRC.[CreatedOn]
      ,SRC.[TextOnlyEmailFlag]
      ,SRC.[ContactTypeKey]
      ,SRC.[OptOutFlag]
      ,SRC.[MarkedForDeleteOn]
	  ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.contactkey
       ,deleted.contactkey
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
		,'tmp.imis_ContactMain'       
       ,'stg.imis_ContactMain'
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
		,
		(select count(*) as RowsRead
		from tmp.imis_ContactMain
		)
		,getdate()
       )

INSERT INTO stg.imis_ContactMain (
[ContactKey]
      ,[ContactStatusCode]
      ,[FullName]
      ,[SortName]
      ,[IsInstitute]
      ,[TaxIDNumber]
      ,[NoSolicitationFlag]
      ,[SyncContactID]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,[IsIDEditable]
      ,[ID]
      ,[PreferredAddressCategoryCode]
      ,[IsSortNameOverridden]
      ,[PrimaryMembershipGroupKey]
      ,[MajorKey]
      ,[AccessKey]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[TextOnlyEmailFlag]
      ,[ContactTypeKey]
      ,[OptOutFlag]
      ,[MarkedForDeleteOn]
      ,[IsActive]
      ,[StartDate]
 
                 )
select  
                    [ContactKey]
      ,[ContactStatusCode]
      ,[FullName]
      ,[SortName]
      ,[IsInstitute]
      ,[TaxIDNumber]
      ,[NoSolicitationFlag]
      ,[SyncContactID]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,[IsIDEditable]
      ,[ID]
      ,[PreferredAddressCategoryCode]
      ,[IsSortNameOverridden]
      ,[PrimaryMembershipGroupKey]
      ,[MajorKey]
      ,[AccessKey]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[TextOnlyEmailFlag]
      ,[ContactTypeKey]
      ,[OptOutFlag]
      ,[MarkedForDeleteOn]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_ContactMain
WHERE ContactKey IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )

			  Truncate TABLE #audittemp



/*************************************************USER MAIN*******************************/
MERGE stg.imis_UserMain AS DST
USING tmp.imis_UserMain AS SRC
       ON DST.USERKEY = SRC.USERKEY 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.CONTACTMASTER, '') <> ISNULL(SRC.CONTACTMASTER, '')
                     OR ISNULL(DST.USERID, '') <> ISNULL(SRC.USERID, '')
                     OR ISNULL(DST.ISDISABLED, '') <> ISNULL(SRC.ISDISABLED, '')
                     OR ISNULL(DST.EFFECTIVEDATE, '') <> ISNULL(SRC.EFFECTIVEDATE, '')
                     OR ISNULL(DST.EXPIRATIONDATE, '') <> ISNULL(SRC.EXPIRATIONDATE, '')
                     OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
                     OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
                     OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
                     OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
					 OR ISNULL(DST.MARKEDFORDELETEON, '') <> ISNULL(SRC.MARKEDFORDELETEON, '')
					 OR ISNULL(DST.DEFAULTDEPARTMENTGROUPKEY, '') <> ISNULL(SRC.DEFAULTDEPARTMENTGROUPKEY, '')
					 OR ISNULL(DST.DEFAULTPERSPECTIVEKEY, '') <> ISNULL(SRC.DEFAULTPERSPECTIVEKEY, '')
					 OR ISNULL(DST.PROVIDERKEY, '') <> ISNULL(SRC.PROVIDERKEY, '')
					 OR ISNULL(DST.MULTIFACTORINFO, '') <> ISNULL(SRC.MULTIFACTORINFO, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [UserKey]
      ,[ContactMaster]
      ,[UserId]
      ,[IsDisabled]
      ,[EffectiveDate]
      ,[ExpirationDate]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[MarkedForDeleteOn]
      ,[DefaultDepartmentGroupKey]
      ,[DefaultPerspectiveKey]
      ,[ProviderKey]
      ,[MultiFactorInfo]
      ,[IsActive]
      ,[StartDate]

          
                     )
              VALUES (
                    SRC.[UserKey]
      ,SRC.[ContactMaster]
      ,SRC.[UserId]
      ,SRC.[IsDisabled]
      ,SRC.[EffectiveDate]
      ,SRC.[ExpirationDate]
      ,SRC.[UpdatedByUserKey]
      ,SRC.[UpdatedOn]
      ,SRC.[CreatedByUserKey]
      ,SRC.[CreatedOn]
      ,SRC.[MarkedForDeleteOn]
      ,SRC.[DefaultDepartmentGroupKey]
      ,SRC.[DefaultPerspectiveKey]
      ,SRC.[ProviderKey]
      ,SRC.[MultiFactorInfo]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.USERKEY
       ,deleted.USERKEY
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@Pipelinename'
	   ,'tmp.imis_UserMain'
       ,'stg.imis_UserMain'
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
				from tmp.imis_UserMain
				)
       ,getdate()
       )

INSERT INTO stg.imis_UserMain (
[UserKey]
      ,[ContactMaster]
      ,[UserId]
      ,[IsDisabled]
      ,[EffectiveDate]
      ,[ExpirationDate]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[MarkedForDeleteOn]
      ,[DefaultDepartmentGroupKey]
      ,[DefaultPerspectiveKey]
      ,[ProviderKey]
      ,[MultiFactorInfo]
      ,[IsActive]
      ,[StartDate]

                 )
select  
                    [UserKey]
      ,[ContactMaster]
      ,[UserId]
      ,[IsDisabled]
      ,[EffectiveDate]
      ,[ExpirationDate]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[MarkedForDeleteOn]
      ,[DefaultDepartmentGroupKey]
      ,[DefaultPerspectiveKey]
      ,[ProviderKey]
      ,[MultiFactorInfo]
      ,1
      ,@Today
           
         
FROM 
tmp.imis_UserMain
WHERE userkey IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp




/*************************************************Csi_MergedRecords********************************/
MERGE stg.imis_Csi_MergedRecords AS DST
USING tmp.imis_Csi_MergedRecords AS SRC
       ON DST.DuplicateID = SRC.DuplicateID and
	   DST.MergeToID = SRC.MergeToID
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.USERNAME, '') <> ISNULL(SRC.USERNAME, '')
                     OR ISNULL(DST.DATEOFMERGE, '') <> ISNULL(SRC.DATEOFMERGE, '')
          
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [DuplicateID]
      ,[MergeToID]
      ,[UserName]
      ,[DateOfMerge]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     src.[DuplicateID]
      ,src.[MergeToID]
      ,src.[UserName]
      ,src.[DateOfMerge]
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.DuplicateID
       ,deleted.DuplicateID
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_Csi_MergedRecords'
       ,'stg.imis_Csi_MergedRecords'
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
				from tmp.imis_Csi_MergedRecords
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Csi_MergedRecords (
[DuplicateID]
      ,[MergeToID]
      ,[UserName]
      ,[DateOfMerge]
      ,[IsActive]
      ,[StartDate]

                 )
select  
                   [DuplicateID]
      ,[MergeToID]
      ,[UserName]
      ,[DateOfMerge]
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_Csi_MergedRecords
WHERE DuplicateID IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

/**********************************************GroupMember*********************************/
MERGE stg.imis_GroupMember AS DST
USING tmp.imis_GroupMember AS SRC
       ON DST.GroupMemberKey = SRC.GroupMemberKey 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.GROUPKEY, '') <> ISNULL(SRC.GROUPKEY, '')
                     OR ISNULL(DST.MEMBERCONTACTKEY, '') <> ISNULL(SRC.MEMBERCONTACTKEY, '')
                     OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
                     OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
                     OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
                     OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
                     OR ISNULL(DST.DROPDATE, '') <> ISNULL(SRC.DROPDATE, '')
                     OR ISNULL(DST.JOINDATE, '') <> ISNULL(SRC.JOINDATE, '')
                     OR ISNULL(DST.[MarkedForDeleteOn], '') <> ISNULL(SRC.[MarkedForDeleteOn], '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [GroupMemberKey]
      ,[GroupKey]
      ,[MemberContactKey]
      ,[IsActive]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[DropDate]
      ,[JoinDate]
      ,[MarkedForDeleteOn]
      ,[StartDate]
          
                     )
              VALUES (
                      src.[GroupMemberKey]
      ,src.[GroupKey]
      ,src.[MemberContactKey]
      ,1
      ,src.[CreatedByUserKey]
      ,src.[CreatedOn]
      ,src.[UpdatedByUserKey]
      ,src.[UpdatedOn]
      ,src.[DropDate]
      ,src.[JoinDate]
      ,src.[MarkedForDeleteOn]
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.GroupMemberKey
       ,deleted.GroupMemberKey
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_GroupMember'
       ,'stg.imis_GroupMember'
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
				from tmp.imis_GroupMember
				)
       ,getdate()
       )

INSERT INTO stg.imis_GroupMember (
 [GroupMemberKey]
      ,[GroupKey]
      ,[MemberContactKey]
      ,[IsActive]
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[DropDate]
      ,[JoinDate]
      ,[MarkedForDeleteOn]
      ,[StartDate]
                 )
select  
                     [GroupMemberKey]
      ,[GroupKey]
      ,[MemberContactKey]
      ,1
      ,[CreatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedByUserKey]
      ,[UpdatedOn]
      ,[DropDate]
      ,[JoinDate]
      ,[MarkedForDeleteOn]
      ,@Today
           
         
FROM 
tmp.imis_GroupMember
WHERE GroupMemberKey IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp



/********************************************GroupTypeRef***********************************************/
MERGE stg.imis_GroupTypeRef AS DST
USING tmp.imis_GroupTypeRef AS SRC
       ON DST.GroupTypeKey = SRC.GroupTypeKey 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.GROUPTYPENAME, '') <> ISNULL(SRC.GROUPTYPENAME, '')
                     OR ISNULL(DST.ISSYSTEM, '') <> ISNULL(SRC.ISSYSTEM, '')
                     OR ISNULL(DST.ISPAYMENTREQUIRED, '') <> ISNULL(SRC.ISPAYMENTREQUIRED, '')
                     OR ISNULL(DST.ISDATELIMITED, '') <> ISNULL(SRC.ISDATELIMITED, '')
                     OR ISNULL(DST.GROUPMEMBERBRANCHNAME, '') <> ISNULL(SRC.GROUPMEMBERBRANCHNAME, '')
                     OR ISNULL(DST.ISINVITATIONONLY, '') <> ISNULL(SRC.ISINVITATIONONLY, '')
                     OR ISNULL(DST.DEFAULTGROUPSTATUSCODE, '') <> ISNULL(SRC.DEFAULTGROUPSTATUSCODE, '')
                     OR ISNULL(DST.ISSIMPLEGROUP, '') <> ISNULL(SRC.ISSIMPLEGROUP, '')
                     OR ISNULL(DST.MEMBERQUERYFOLDERKEY, '') <> ISNULL(SRC.MEMBERQUERYFOLDERKEY, '')
					 OR ISNULL(DST.INHERITROLESFLAG, '') <> ISNULL(SRC.INHERITROLESFLAG, '')
					 OR ISNULL(DST.ISSINGLEROLE, '') <> ISNULL(SRC.ISSINGLEROLE, '')
					 OR ISNULL(DST.GROUPTYPEDESC, '') <> ISNULL(SRC.GROUPTYPEDESC, '')
					 OR ISNULL(DST.CREATEDBYUSERKEY, '') <> ISNULL(SRC.CREATEDBYUSERKEY, '')
					 OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
					 OR ISNULL(DST.CREATEDON, '') <> ISNULL(SRC.CREATEDON, '')
					 OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
					 OR ISNULL(DST.LANDINGPAGECONTENTKEY, '') <> ISNULL(SRC.LANDINGPAGECONTENTKEY, '')
					 OR ISNULL(DST.ISRELATIONSHIPGROUP, '') <> ISNULL(SRC.ISRELATIONSHIPGROUP, '')
					 OR ISNULL(DST.EXTENDACTIVEMEMBERSHIPTERMFLAG, '') <> ISNULL(SRC.EXTENDACTIVEMEMBERSHIPTERMFLAG, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [GroupTypeKey]
      ,[GroupTypeName]
      ,[IsSystem]
      ,[IsPaymentRequired]
      ,[IsDateLimited]
      ,[GroupMemberBranchName]
      ,[IsInvitationOnly]
      ,[DefaultGroupStatusCode]
      ,[IsSimpleGroup]
      ,[MemberQueryFolderKey]
      ,[InheritRolesFlag]
      ,[IsSingleRole]
      ,[GroupTypeDesc]
      ,[CreatedByUserKey]
      ,[UpdatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedOn]
      ,[LandingPageContentKey]
      ,[IsRelationshipGroup]
      ,[ExtendActiveMembershipTermFlag]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                     src.[GroupTypeKey]
      ,src.[GroupTypeName]
      ,src.[IsSystem]
      ,src.[IsPaymentRequired]
      ,src.[IsDateLimited]
      ,src.[GroupMemberBranchName]
      ,src.[IsInvitationOnly]
      ,src.[DefaultGroupStatusCode]
      ,src.[IsSimpleGroup]
      ,src.[MemberQueryFolderKey]
      ,src.[InheritRolesFlag]
      ,src.[IsSingleRole]
      ,src.[GroupTypeDesc]
      ,src.[CreatedByUserKey]
      ,src.[UpdatedByUserKey]
      ,src.[CreatedOn]
      ,src.[UpdatedOn]
      ,src.[LandingPageContentKey]
      ,src.[IsRelationshipGroup]
      ,src.[ExtendActiveMembershipTermFlag]
                 ,1
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.GroupTypeKey
       ,deleted.GroupTypeKey
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_GroupTypeRef'
       ,'stg.imis_GroupTypeRef'
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
				from tmp.imis_GroupTypeRef
				) 
       ,getdate()
       )

INSERT INTO stg.imis_GroupTypeRef (
[GroupTypeKey]
      ,[GroupTypeName]
      ,[IsSystem]
      ,[IsPaymentRequired]
      ,[IsDateLimited]
      ,[GroupMemberBranchName]
      ,[IsInvitationOnly]
      ,[DefaultGroupStatusCode]
      ,[IsSimpleGroup]
      ,[MemberQueryFolderKey]
      ,[InheritRolesFlag]
      ,[IsSingleRole]
      ,[GroupTypeDesc]
      ,[CreatedByUserKey]
      ,[UpdatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedOn]
      ,[LandingPageContentKey]
      ,[IsRelationshipGroup]
      ,[ExtendActiveMembershipTermFlag]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                  [GroupTypeKey]
      ,[GroupTypeName]
      ,[IsSystem]
      ,[IsPaymentRequired]
      ,[IsDateLimited]
      ,[GroupMemberBranchName]
      ,[IsInvitationOnly]
      ,[DefaultGroupStatusCode]
      ,[IsSimpleGroup]
      ,[MemberQueryFolderKey]
      ,[InheritRolesFlag]
      ,[IsSingleRole]
      ,[GroupTypeDesc]
      ,[CreatedByUserKey]
      ,[UpdatedByUserKey]
      ,[CreatedOn]
      ,[UpdatedOn]
      ,[LandingPageContentKey]
      ,[IsRelationshipGroup]
      ,[ExtendActiveMembershipTermFlag]
                 ,1
                 ,@Today
           
         
FROM 
tmp.imis_GroupTypeRef
WHERE GroupTypeKey IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

/********************************PostalCodeRef*****************************/
MERGE stg.imis_PostalCodeRef AS DST
USING tmp.imis_PostalCodeRef AS SRC
       ON DST.PostalCodeKey = SRC.PostalCodeKey 
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.POSTALCODE, '') <> ISNULL(SRC.POSTALCODE, '')
                     OR ISNULL(DST.COUNTRYCODE, '') <> ISNULL(SRC.COUNTRYCODE, '')
                     OR ISNULL(DST.CITY, '') <> ISNULL(SRC.CITY, '')
                     OR ISNULL(DST.STATEPROVINCECODE, '') <> ISNULL(SRC.STATEPROVINCECODE, '')
                     OR ISNULL(DST.REGION, '') <> ISNULL(SRC.REGION, '')
                     OR ISNULL(DST.COUNTY, '') <> ISNULL(SRC.COUNTY, '')
                     OR ISNULL(DST.COUNTYFIPS, '') <> ISNULL(SRC.COUNTYFIPS, '')
                     OR ISNULL(DST.ISHANDMODIFIED, '') <> ISNULL(SRC.ISHANDMODIFIED, '')
                     OR ISNULL(DST.ISHANDENTERED, '') <> ISNULL(SRC.ISHANDENTERED, '')
					 OR ISNULL(DST.CHAPTERGROUPKEY, '') <> ISNULL(SRC.CHAPTERGROUPKEY, '')
					 OR ISNULL(DST.UPDATEDON, '') <> ISNULL(SRC.UPDATEDON, '')
					 OR ISNULL(DST.UPDATEDBYUSERKEY, '') <> ISNULL(SRC.UPDATEDBYUSERKEY, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                     [PostalCode]
      ,[CountryCode]
      ,[City]
      ,[StateProvinceCode]
      ,[Region]
      ,[County]
      ,[CountyFIPS]
      ,[IsHandModified]
      ,[IsHandEntered]
      ,[ChapterGroupKey]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,[IsActive]
      ,[PostalCodeKey]
      ,[StartDate]
          
                     )
              VALUES (
                      src.[PostalCode]
      ,src.[CountryCode]
      ,src.[City]
      ,src.[StateProvinceCode]
      ,src.[Region]
      ,src.[County]
      ,src.[CountyFIPS]
      ,src.[IsHandModified]
      ,src.[IsHandEntered]
      ,src.[ChapterGroupKey]
      ,src.[UpdatedOn]
      ,src.[UpdatedByUserKey]
      ,1
      ,src.[PostalCodeKey]
                 ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.postalcodekey
       ,deleted.postalcodekey
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_PostalCodeRef'
       ,'stg.imis_PostalCodeRef'
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
				from tmp.imis_PostalCodeRef
				) 
       ,getdate()
       )

INSERT INTO stg.imis_PostalCodeRef (
 [PostalCode]
      ,[CountryCode]
      ,[City]
      ,[StateProvinceCode]
      ,[Region]
      ,[County]
      ,[CountyFIPS]
      ,[IsHandModified]
      ,[IsHandEntered]
      ,[ChapterGroupKey]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,[IsActive]
      ,[PostalCodeKey]
      ,[StartDate]
                 )
select  
                   [PostalCode]
      ,[CountryCode]
      ,[City]
      ,[StateProvinceCode]
      ,[Region]
      ,[County]
      ,[CountyFIPS]
      ,[IsHandModified]
      ,[IsHandEntered]
      ,[ChapterGroupKey]
      ,[UpdatedOn]
      ,[UpdatedByUserKey]
      ,1
      ,[PostalCodeKey]
                 ,@Today
           
         
FROM 
tmp.imis_PostalCodeRef
WHERE PostalCodeKey IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp

/********************************OrderMeet*****************************/
MERGE stg.imis_Order_Meet AS DST
USING tmp.imis_Order_Meet AS SRC
       ON DST.ORDER_NUMBER = SRC.ORDER_NUMBER
WHEN MATCHED
              AND DST.IsActive = 1
              AND (
                   
                     ISNULL(DST.MEETING, '') <> ISNULL(SRC.MEETING, '')
                     OR ISNULL(DST.REGISTRANT_CLASS, '') <> ISNULL(SRC.REGISTRANT_CLASS, '')
                     OR ISNULL(DST.ARRIVAL, '') <> ISNULL(SRC.ARRIVAL, '')
                     OR ISNULL(DST.DEPARTURE, '') <> ISNULL(SRC.DEPARTURE, '')
                     OR ISNULL(DST.HOTEL, '') <> ISNULL(SRC.HOTEL, '')
                     OR ISNULL(DST.LODGING_INSTRUCTIONS, '') <> ISNULL(SRC.LODGING_INSTRUCTIONS, '')
                     OR ISNULL(DST.BOOTH, '') <> ISNULL(SRC.BOOTH, '')
                     OR ISNULL(DST.GUEST_FIRST, '') <> ISNULL(SRC.GUEST_FIRST, '')
                     OR ISNULL(DST.GUEST_MIDDLE, '') <> ISNULL(SRC.GUEST_MIDDLE, '')
					 OR ISNULL(DST.GUEST_LAST, '') <> ISNULL(SRC.GUEST_LAST, '')
					 OR ISNULL(DST.GUEST_IS_SPOUSE, '') <> ISNULL(SRC.GUEST_IS_SPOUSE, '')
					 OR ISNULL(DST.ADDITIONAL_BADGES, '') <> ISNULL(SRC.ADDITIONAL_BADGES, '')
                     OR ISNULL(DST.DELEGATE, '') <> ISNULL(SRC.DELEGATE, '')
					 OR ISNULL(DST.UF_1, '') <> ISNULL(SRC.UF_1, '')
					 OR ISNULL(DST.UF_2, '') <> ISNULL(SRC.UF_2, '')
					 OR ISNULL(DST.UF_3, '') <> ISNULL(SRC.UF_3, '')
					 OR ISNULL(DST.UF_4, '') <> ISNULL(SRC.UF_4, '')
					 OR ISNULL(DST.UF_5, '') <> ISNULL(SRC.UF_5, '')
					 OR ISNULL(DST.UF_6, '') <> ISNULL(SRC.UF_6, '')
					 OR ISNULL(DST.UF_7, '') <> ISNULL(SRC.UF_7, '')
					 OR ISNULL(DST.UF_8, '') <> ISNULL(SRC.UF_8, '')
					 OR ISNULL(DST.SHARE_STATUS, '') <> ISNULL(SRC.SHARE_STATUS, '')
					 OR ISNULL(DST.SHARE_ORDER_NUMBER, 0) <> ISNULL(SRC.SHARE_ORDER_NUMBER, 0)
					 OR ISNULL(DST.ROOM_TYPE, '') <> ISNULL(SRC.ROOM_TYPE, '')
					 OR ISNULL(DST.ROOM_QUANTITY, '') <> ISNULL(SRC.ROOM_QUANTITY, '')
					 OR ISNULL(DST.ROOM_CONFIRM, '') <> ISNULL(SRC.ROOM_CONFIRM, '')
					 OR ISNULL(DST.UF_9, '') <> ISNULL(SRC.UF_9, '')
					 OR ISNULL(DST.UF_10, '') <> ISNULL(SRC.UF_10, '')
					 OR ISNULL(DST.ARRIVAL_TIME, '') <> ISNULL(SRC.ARRIVAL_TIME, '')
					 OR ISNULL(DST.DEPARTURE_TIME, '') <> ISNULL(SRC.DEPARTURE_TIME, '')
					 OR ISNULL(DST.COMP_REGISTRATIONS, '') <> ISNULL(SRC.COMP_REGISTRATIONS, '')
					 OR ISNULL(DST.COMP_REG_SOURCE, '') <> ISNULL(SRC.COMP_REG_SOURCE, '')
					 OR ISNULL(DST.TOTAL_SQUARE_FEET, 0) <> ISNULL(SRC.TOTAL_SQUARE_FEET, 0)
					 OR ISNULL(DST.COMP_REGISTRATIONS_USED, '') <> ISNULL(SRC.COMP_REGISTRATIONS_USED, '')
					 OR ISNULL(DST.PARENT_ORDER_NUMBER, 0) <> ISNULL(SRC.PARENT_ORDER_NUMBER, 0)
					 OR ISNULL(DST.REGISTERED_BY_ID, '') <> ISNULL(SRC.REGISTERED_BY_ID, '')
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET DST.isActive = 0
                           ,DST.EndDate = @Yesterday
WHEN NOT MATCHED
       THEN
              INSERT (
                    [ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
          
                     )
              VALUES (
                      SRC.[ORDER_NUMBER]
      ,SRC.[MEETING]
      ,SRC.[REGISTRANT_CLASS]
      ,SRC.[ARRIVAL]
      ,SRC.[DEPARTURE]
      ,SRC.[HOTEL]
      ,SRC.[LODGING_INSTRUCTIONS]
      ,SRC.[BOOTH]
      ,SRC.[GUEST_FIRST]
      ,SRC.[GUEST_MIDDLE]
      ,SRC.[GUEST_LAST]
      ,SRC.[GUEST_IS_SPOUSE]
      ,SRC.[ADDITIONAL_BADGES]
      ,SRC.[DELEGATE]
      ,SRC.[UF_1]
      ,SRC.[UF_2]
      ,SRC.[UF_3]
      ,SRC.[UF_4]
      ,SRC.[UF_5]
      ,SRC.[UF_6]
      ,SRC.[UF_7]
      ,SRC.[UF_8]
      ,SRC.[SHARE_STATUS]
      ,SRC.[SHARE_ORDER_NUMBER]
      ,SRC.[ROOM_TYPE]
      ,SRC.[ROOM_QUANTITY]
      ,SRC.[ROOM_CONFIRM]
      ,SRC.[UF_9]
      ,SRC.[UF_10]
      ,SRC.[ARRIVAL_TIME]
      ,SRC.[DEPARTURE_TIME]
      ,SRC.[COMP_REGISTRATIONS]
      ,SRC.[COMP_REG_SOURCE]
      ,SRC.[TOTAL_SQUARE_FEET]
      ,SRC.[COMP_REGISTRATIONS_USED]
      ,SRC.[PARENT_ORDER_NUMBER]
      ,SRC.[REGISTERED_BY_ID]
      ,SRC.[TIME_STAMP]
      ,1
      ,@Today
                     ) 

OUTPUT $ACTION AS action
       ,inserted.ORDER_NUMBER
       ,deleted.ORDER_NUMBER
INTO #audittemp;



INSERT INTO etl.executionlog
VALUES (
		'@PipelineName'
	   ,'tmp.imis_Order_Meet'
       ,'stg.imis_Order_Meet'
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
				from tmp.imis_Order_Meet
				) 
       ,getdate()
       )

INSERT INTO stg.imis_Order_Meet (
 [ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
      ,[TIME_STAMP]
      ,[IsActive]
      ,[StartDate]
                 )
select  
                   [ORDER_NUMBER]
      ,[MEETING]
      ,[REGISTRANT_CLASS]
      ,[ARRIVAL]
      ,[DEPARTURE]
      ,[HOTEL]
      ,[LODGING_INSTRUCTIONS]
      ,[BOOTH]
      ,[GUEST_FIRST]
      ,[GUEST_MIDDLE]
      ,[GUEST_LAST]
      ,[GUEST_IS_SPOUSE]
      ,[ADDITIONAL_BADGES]
      ,[DELEGATE]
      ,[UF_1]
      ,[UF_2]
      ,[UF_3]
      ,[UF_4]
      ,[UF_5]
      ,[UF_6]
      ,[UF_7]
      ,[UF_8]
      ,[SHARE_STATUS]
      ,[SHARE_ORDER_NUMBER]
      ,[ROOM_TYPE]
      ,[ROOM_QUANTITY]
      ,[ROOM_CONFIRM]
      ,[UF_9]
      ,[UF_10]
      ,[ARRIVAL_TIME]
      ,[DEPARTURE_TIME]
      ,[COMP_REGISTRATIONS]
      ,[COMP_REG_SOURCE]
      ,[TOTAL_SQUARE_FEET]
      ,[COMP_REGISTRATIONS_USED]
      ,[PARENT_ORDER_NUMBER]
      ,[REGISTERED_BY_ID]
      ,cast([TIME_STAMP] as bigint)
      ,1
      ,@Today
           
         
FROM 
tmp.imis_Order_Meet
WHERE order_number IN (
              SELECT inserted_ID
              FROM #audittemp
              WHERE action = 'UPDATE'
              )


			  Truncate TABLE #audittemp


/**************************************************table**********************/
END TRY
BEGIN CATCH
SET @ErrorMessage  = ERROR_MESSAGE();
    SET @ErrorSeverity = ERROR_SEVERITY();
    SET @ErrorState    = ERROR_STATE();
    THROW;
END CATCH;
