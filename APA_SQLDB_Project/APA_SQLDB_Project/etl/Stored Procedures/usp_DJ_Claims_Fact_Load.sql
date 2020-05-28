CREATE procedure [etl].[usp_DJ_Claims_Fact_Load]
@PipelineName varchar(60) 
as

/*******************************************************************************************************************************
 Description:   This Stored Procedure loads active new or updated records into the claims fact table. Records are loaded 
				through a process of connecting the staging fact table to the rpt schema through joins to the dimensions. These
				joins allow us to get transform the raw data into keys. 
				

Added By:		Morgan Diestler on 4/30/2020				
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



/************truncate tmp.rptFact_Claims *********************/
truncate table tmp.rptFact_Claims 

/***************Insert Keys into temp rpt from tmp fact**************************/
  insert into tmp.rptFact_Claims (ID_Claim, LogStatusKey, CMPeriodKey, ProductIDKey, FlagKey, MemberIDKey, Credits,
Law_Credits, Ethics_Credits, SubmittedDateKey, MemberTypeKey, ChapterKey, SalaryKey, RaceKey, StatusKey, MemberStatusKey,
Addressnum1key, Addressnum2key)
select distinct A.ID_Claim, B.LogStatusKey, C.CMPeriodKey, E.ClaimsProductKey, D.FlagKey, F.MemberIDKey, A.Credits, A.Law_Credits,
A.Ethics_Credits, O.Date_Key, G.MemberTypeIDKey, K.ChapterKey, H.SalaryIDKey, I.RaceIDKey, J.StatusKey, 
L.MemberStatus_Key, M.Address_Key, N.Address_Key
from tmp.fact_claims A
left join rpt.dimClaimLogStatus B on
A.Log_Status = B.Log_Status
left join rpt.dimCMPeriod C on
A.CM_Period_Code = C.CM_PeriodCode
left join rpt.dimClaimsFlags D on
A.Verified = D.Verified and A.Is_Author = D.Is_Author and A.Is_CarryOver = D.Is_CarryOver and A.Is_Deleted = D.Is_Deleted
and A.Is_Probono = D.Is_Probono and A.Is_Speaker = D.Is_Speaker and A.Self_Reported = D.Self_Reported
left join rpt.dimClaimsProduct E on
A.Product_Major = E.ClaimsProduct_Major and A.Product_Minor = E.ClaimsProduct_Minor
left join rpt.dimMember F on
A.Member_ID = F.Member_ID and F.IsActive = 1
left join rpt.dimMemberType G on a.member_type = G.member_type and G.isactive = 1
left join rpt.dimSalary H on a.salary_range = H.SalaryID and H.isactive = 1
left join rpt.dimRace I on a.Race_ID = I.RaceID and I.Origin = a.Origin and I.isactive = 1
left join rpt.dimStatus J on a.status =J.status and J.Category = a.Category and J.Gender = a.Gender  and J.isactive = 1
left join rpt.dimChapter K on a.Primary_Chapter = K.Chapter_Minor and K.isactive = 1
left join rpt.dimMember_Status L on a.member_status = L.member_status and L.isactive = 1
left join rpt.dimAddress M on a.address_num1 = M.address_num and M.isactive = 1
left join rpt.dimAddress N on a.address_num2 = N.address_num and N.isactive = 1
left join rpt.dimDate O on convert(date,A.submitted_time) = O.Date
where A.Member_ID is not null




/**************Merge Tmp to Rpt Fact**********************/




Merge rpt.tblFactClaims DST
Using tmp.rptFact_Claims as SRC
on DST.Id_Claim = SRC.ID_Claim
WHEN MATCHED
THEN UPDATE
SET  DST.LogStatusKey = SRC.LogStatusKey
	,DST.CMPeriodKey  = SRC.CMPeriodKey
	,DST.ProductIDKey = SRC.ProductIDKey
	,DST.FlagKey = SRC.FlagKey
	,DST.Credits = SRC.Credits
	,DST.Law_Credits = SRC.Law_Credits
	,DST.Ethics_Credits = SRC.Ethics_Credits
	,DST.SubmittedDateKey = SRC.SubmittedDateKey

	WHEN NOT MATCHED THEN
	Insert (ID_Claim, LogStatusKey, CMPeriodKey, ProductIDKey, FlagKey, MemberIDKey, Credits,
Law_Credits, Ethics_Credits, SubmittedDateKey, MemberTypeKey, ChapterKey, SalaryKey, RaceKey, StatusKey, MemberStatusKey,
Addressnum1key, Addressnum2key)
	VALUES (SRC.ID_Claim, SRC.LogStatusKey, SRC.CMPeriodKey, SRC.ProductIDKey, SRC.FlagKey, SRC.MemberIDKey, SRC.Credits,
SRC.Law_Credits, SRC.Ethics_Credits, SRC.SubmittedDateKey, SRC.MemberTypeKey, SRC.ChapterKey, SRC.SalaryKey, SRC.RaceKey, SRC.StatusKey, SRC.MemberStatusKey,
SRC.Addressnum1key,SRC.Addressnum2key)

OUTPUT $ACTION AS action
       ,inserted.Id_Claim
       ,deleted.Id_Claim
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
       ,'rpt.tblFactClaims'
       ,'tmp.rptFact_Claims'
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
                           from tmp.rptfact_registrations
                           ) 
       ,getdate()
       )


                       Truncate TABLE #audittemp




/***************Merge Tmp to Staging Fact - Natural Keys *************************/
Merge Stg.Fact_Claims as DST
Using tmp.Fact_Claims as SRC
ON DST.ID_Claim = SRC.ID_Claim
WHEN MATCHED AND
     (    ISNULL(DST.Log_Status,'') <> ISNULL(SRC.Log_Status,'')
	 OR   ISNULL(DST.CM_Period_Code,'')  <> ISNULL(SRC.CM_Period_Code,'')
	 OR   ISNULL(DST.Member_ID,'') <> ISNULL(SRC.Member_ID,'')
	 OR   ISNULL(DST.Product_Major,'') <> ISNULL(SRC.Product_Major,'')
	 OR   ISNULL(DST.Product_Minor,'') <> ISNULL(SRC.Product_Minor,'')
	 OR   ISNULL(DST.Verified,'') <> ISNULL(SRC.Verified,'')
	 OR   ISNULL(DST.Credits,'') <> ISNULL(SRC.Credits,'')
	 OR   ISNULL(DST.Law_Credits,'') <> ISNULL(SRC.Law_Credits,'')
	 OR   ISNULL(DST.Ethics_Credits,'') <> ISNULL(SRC.Ethics_Credits,'')
	 OR   ISNULL(DST.Is_Speaker,'') <> ISNULL(SRC.Is_Speaker,'')
	 OR   ISNULL(DST.Is_Author,'') <> ISNULL(SRC.Is_Author,'')
	 OR   ISNULL(DST.Self_Reported,'') <> ISNULL(SRC.Self_Reported,'')
	 OR   ISNULL(DST.Is_CarryOver,'') <> ISNULL(SRC.Is_CarryOver,'')
	 OR   ISNULL(DST.Is_Probono,'') <> ISNULL(SRC.Is_Probono,'')
	 OR   ISNULL(DST.Submitted_Time,'') <> ISNULL(SRC.Submitted_Time,'')
	 )

	 THEN UPDATE SET
	       DST.Log_Status  = ISNULL(SRC.Log_Status,'')
	  ,DST.CM_Period_Code  = ISNULL(SRC.CM_Period_Code,'')
	  ,DST.Member_ID  = ISNULL(SRC.Member_ID,'')
	  ,DST.Product_Major  = ISNULL(SRC.Product_Major,'')
	  ,DST.Product_Minor  = ISNULL(SRC.Product_Minor,'')
	  ,DST.Verified  = ISNULL(SRC.Verified,'')
	  ,DST.Credits  = ISNULL(SRC.Credits,'')
	  ,DST.Law_Credits  = ISNULL(SRC.Law_Credits,'')
	  ,DST.Ethics_Credits  = ISNULL(SRC.Ethics_Credits,'')
	  ,DST.Is_Speaker  = ISNULL(SRC.Is_Speaker,'')
	  ,DST.Is_Author  = ISNULL(SRC.Is_Author,'')
	  ,DST.Self_Reported  = ISNULL(SRC.Self_Reported,'')
	  ,DST.Is_CarryOver  = ISNULL(SRC.Is_CarryOver,'')
	  ,DST.Is_Probono  = ISNULL(SRC.Is_Probono,'')
	  ,DST.Submitted_Time  = ISNULL(SRC.Submitted_Time,'')

	  WHEN NOT MATCHED THEN
	  INSERT(ID_Claim
  ,Log_Status  
  ,CM_Period_Code  
  ,Member_ID  
  ,Product_Major  
  ,Product_Minor  
  ,Verified  
  ,Credits  
  ,Law_Credits  
  ,Ethics_Credits  
  ,Is_Speaker  
  ,Is_Author  
  ,Self_Reported  
  ,Is_CarryOver  
  ,Is_Probono  
  ,Is_Deleted
  ,Submitted_Time
  ,Member_Type
  ,Salary_Range
  ,Race_ID
  ,Origin
  ,Primary_Chapter
  ,Gender
  ,Status
  ,Category
  ,Member_Status
  ,Address_Num1
  ,Address_Num2
  ) 

    VALUES
	 (SRC.ID_Claim
 ,SRC.Log_Status
 ,SRC.CM_Period_Code
 ,SRC.Member_ID
 ,SRC.Product_Major
 ,SRC.Product_Minor
 ,SRC.Verified
 ,SRC.Credits
 ,SRC.Law_Credits
 ,SRC.Ethics_Credits
 ,SRC.Is_Speaker
 ,SRC.Is_Author
 ,SRC.Self_Reported
 ,SRC.Is_CarryOver
 ,SRC.Is_Probono
 ,0
 ,SRC.Submitted_Time
 ,SRC.Member_Type
  ,SRC.Salary_Range
  ,SRC.Race_ID
  ,SRC.Origin
  ,SRC.Primary_Chapter
  ,SRC.Gender
  ,SRC.Status
  ,SRC.Category
  ,SRC.Member_Status
  ,SRC.Address_Num1
  ,SRC.Address_Num2
 )
                     

OUTPUT $ACTION AS action
       ,inserted.ID_Claim
       ,deleted.ID_Claim
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        @PipelineName
       ,'tmp.Fact_Claims'
       ,'stg.Fact_Claims'
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
                           from tmp.Fact_Registrations
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