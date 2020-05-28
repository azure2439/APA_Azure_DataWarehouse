CREATE procedure [etl].[usp_IMIS_Registration_Fact_Load]
@PipelineName varchar(60) 
as

/*******************************************************************************************************************************
 Description:   This Stored Procedure loads active new or updated records into the registrations fact table. Records are loaded 
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



/************truncate tmp.rptfact_registrations*********************/
truncate table tmp.rptfact_registrations

/***************Insert Keys into temp rpt from tmp fact**************************/
Insert into tmp.rptfact_registrations (seqn, MemberIDKey, ProductCodeKey, Order_Number, Line_Number, Quantity_Ordered, Quantity_Shipped, Quantity_Backordered, Unit_Price, MemberTypeIDKey, SalaryIDKey, RaceIDKey, StatusKey, RegistrantStatusKey, RegistrationClassKey, ChapterKey, MemberStatus_Key, Address_Num1_Key, Address_Num2_Key, Member_ID)
select distinct a.seqn, b.MemberIDKey, c.ProductCodeKey, a.Order_Number, a.Line_Number, a.Quantity_Ordered, a.Quantity_Shipped, a.Quantity_Backordered, a.Unit_Price, d.MemberTypeIDKey, e.SalaryIDKey, f.RaceIDKey, g.StatusKey, h.RegistrantStatusKey, i.RegistrantClassKey, j.ChapterKey, k.MemberStatus_Key, l.Address_Key as address_num1_key, m.Address_Key as address_num2_key, a.Member_ID
from tmp.fact_registrations a
left join rpt.dimMember b on a.member_id = b.member_id and b.isactive = 1
left join rpt.dimProductCode c on a.product_code = c.product_code and c.isactive = 1
--left join rpt.dimProductCode c on a.product_code = c.product_code and c.isactive = 1
left join rpt.dimMemberType d on a.member_type = d.member_type and d.isactive = 1
left join rpt.dimSalary e on a.salary_range = e.SalaryID and e.isactive = 1
left join rpt.dimRace f on a.Race_ID = f.RaceID and f.Origin = a.Origin and f.isactive = 1
left join rpt.dimStatus g on a.status = g.status and g.Category = a.Category and g.Gender = a.Gender  and g.isactive = 1
left join rpt.dimRegistrantStatus h on a.registrant_status = h.registrant_status and h.isactive = 1
left join rpt.dimRegistrantClass i on a.registrant_class = i.registrant_class and i.isactive = 1
left join rpt.dimChapter j on a.Primary_Chapter = j.Chapter_Minor and j.isactive = 1
left join rpt.dimMember_Status k on a.member_status = k.member_status and k.isactive = 1
left join rpt.dimAddress l on a.address_num1 = l.address_num and l.isactive = 1
left join rpt.dimAddress m on a.address_num2 = m.address_num and m.isactive = 1




/**************Merge Tmp to Rpt Fact**********************/




MERGE rpt.tblfactregistrations as DST
USING tmp.rptfact_registrations as SRC
on DST.SEQN = SRC.SEQN
		  and DST.Member_ID  = SRC.Member_ID
          and DST.ORDER_NUMBER = SRC.ORDER_NUMBER
          and DST.LINE_NUMBER = SRC.LINE_NUMBER
       WHEN MATCHED
       THEN UPDATE 
       SET DST.QUANTITY_ORDERED = ISNULL(SRC.QUANTITY_ORDERED, 0)
       ,DST.QUANTITY_SHIPPED = ISNULL(SRC.QUANTITY_SHIPPED, 0)
       ,DST.QUANTITY_BACKORDERED = ISNULL(SRC.QUANTITY_BACKORDERED, 0)
       ,DST.UNIT_PRICE = ISNULL(SRC.UNIT_PRICE, '')
	   ,DST.RegistrantStatusKey = SRC.RegistrantStatusKey
	   ,DST.RegistrationClassKey = SRC.RegistrationClassKey
	   ,DST.MemberStatus_Key = SRC.MemberStatus_Key
	   

WHEN NOT MATCHED 
then insert (
seqn, 
MemberIDKey,
ProductCodeKey, 
order_Number,
Line_Number,
Quantity_Ordered,
Quantity_Shipped, 
Quantity_Backordered, 
Unit_Price, 
MemberTypeIDKey, 
SalaryIDKey, 
RaceIDKey, 
StatusKey, 
RegistrantStatusKey, 
RegistrationClassKey, 
ChapterKey, 
MemberStatus_Key, 
Address_Num1_Key, 
Address_Num2_Key
)
values (
src.seqn, 
src.MemberIDKey,
src.ProductCodeKey, 
src.order_Number,
src.Line_Number,
src.Quantity_Ordered,
src.Quantity_Shipped, 
src.Quantity_Backordered, 
src.Unit_Price, 
src.MemberTypeIDKey, 
src.SalaryIDKey, 
src.RaceIDKey, 
src.StatusKey, 
src.RegistrantStatusKey, 
src.RegistrationClassKey, 
src.ChapterKey, 
src.MemberStatus_Key, 
src.Address_Num1_Key, 
src.Address_Num2_Key
)

OUTPUT $ACTION AS action
       ,inserted.SEQN
       ,deleted.SEQN
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'RegistrationsFact_InitialLoad'
       ,'rpt.tbl_fact_registrations'
       ,'tmp.rptfact_registrations'
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
MERGE stg.Fact_Registrations AS DST
USING  tmp.fact_registrations AS SRC
       ON DST.SEQN = SRC.SEQN
		  and DST.Member_ID = SRC.Member_ID	
          and DST.ORDER_NUMBER = SRC.ORDER_NUMBER
          and DST.LINE_NUMBER = SRC.LINE_NUMBER
WHEN MATCHED
             
              AND (
                     
                        ISNULL(DST.PRODUCT_MAJOR, '') <> ISNULL(SRC.PRODUCT_MAJOR, '')
                     OR ISNULL(DST.PRODUCT_CODE, '') <> ISNULL(SRC.PRODUCT_CODE, '')
                     OR ISNULL(DST.QUANTITY_ORDERED, 0) <> ISNULL(SRC.QUANTITY_ORDERED, 0)
                     OR ISNULL(DST.QUANTITY_SHIPPED, 0) <> ISNULL(SRC.QUANTITY_SHIPPED, 0)
                     OR ISNULL(DST.QUANTITY_BACKORDERED, 0) <> ISNULL(SRC.QUANTITY_BACKORDERED, 0)
                     OR ISNULL(DST.UNIT_PRICE, '') <> ISNULL(SRC.UNIT_PRICE, '')
                     OR ISNULL(DST.REGISTRANT_CLASS, '') <> ISNULL(SRC.REGISTRANT_CLASS, '')
                     OR ISNULL(DST.REGISTRANT_STATUS, '') <> ISNULL(SRC.REGISTRANT_STATUS, '')
                     OR ISNULL(DST.FUNCTION_TYPE, '') <> ISNULL(SRC.FUNCTION_TYPE, '')
                     OR ISNULL(DST.SOURCE_TABLENAME, '') <> ISNULL(SRC.SOURCE_TABLENAME, '')
                     
                     )
              -- Update statement for a changed dimension record, to flag as no longer active, only insert fields they want to track 
              THEN
                     UPDATE
                     SET 
                      DST.PRODUCT_MAJOR = ISNULL(SRC.PRODUCT_MAJOR, '')
                     ,DST.PRODUCT_CODE = ISNULL(SRC.PRODUCT_CODE, '')
                     ,DST.QUANTITY_ORDERED = ISNULL(SRC.QUANTITY_ORDERED, 0)
                     ,DST.QUANTITY_SHIPPED = ISNULL(SRC.QUANTITY_SHIPPED, 0)
                     ,DST.QUANTITY_BACKORDERED = ISNULL(SRC.QUANTITY_BACKORDERED, 0)
                     ,DST.UNIT_PRICE = ISNULL(SRC.UNIT_PRICE, '')
                     ,DST.REGISTRANT_CLASS = ISNULL(SRC.REGISTRANT_CLASS, '')
                     ,DST.REGISTRANT_STATUS = ISNULL(SRC.REGISTRANT_STATUS, '')
                     ,DST.FUNCTION_TYPE = ISNULL(SRC.FUNCTION_TYPE, '')
                     ,DST.SOURCE_TABLENAME = ISNULL(SRC.SOURCE_TABLENAME, '')
WHEN NOT MATCHED
       THEN
              INSERT (
                     [Seqn]
      ,[Member_ID]
      ,[Product_Major]
      ,[Product_code]
      ,[Order_Number]
      ,[Line_Number]
      ,[Quantity_Ordered]
      ,[Quantity_Shipped]
      ,[Quantity_Backordered]
      ,[Unit_Price]
      ,[Registrant_Class]
      ,[Registrant_Status]
      ,[Function_Type]
      ,[Member_Type]
      ,[Salary_Range]
      ,[Race_ID]
      ,[Origin]
      ,[Primary_Chapter]
      ,[Gender]
      ,[Status]
      ,[Category]
      ,[Member_Status]
      ,[Address_Num1]
      ,[Address_Num2]
      ,[Source_TableName]
          
                     )
              VALUES (
                   SRC.[Seqn]
      ,SRC.[Member_ID]
      ,SRC.[Product_Major]
      ,SRC.[Product_code]
      ,SRC.[Order_Number]
      ,SRC.[Line_Number]
      ,SRC.[Quantity_Ordered]
      ,SRC.[Quantity_Shipped]
      ,SRC.[Quantity_Backordered]
      ,SRC.[Unit_Price]
      ,SRC.[Registrant_Class]
      ,SRC.[Registrant_Status]
      ,SRC.[Function_Type]
      ,SRC.[Member_Type]
      ,SRC.[Salary_Range]
      ,SRC.[Race_ID]
      ,SRC.[Origin]
      ,SRC.[Primary_Chapter]
      ,SRC.[Gender]
      ,SRC.[Status]
      ,SRC.[Category]
      ,SRC.[Member_Status]
      ,SRC.[Address_Num1]
      ,SRC.[Address_Num2]
      ,SRC.[Source_TableName]
                     )
                     

OUTPUT $ACTION AS action
       ,inserted.SEQN
       ,deleted.SEQN
INTO #audittemp;



INSERT INTO etl.executionlog (pipeline_name, source_table, target_table, insert_count, update_count, rows_read, datetimestamp)
VALUES (
        'stg_Fact_Registrations_Initial_Load'
       ,'tmp.Fact_Registrations'
       ,'stg.Fact_Registrations'
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