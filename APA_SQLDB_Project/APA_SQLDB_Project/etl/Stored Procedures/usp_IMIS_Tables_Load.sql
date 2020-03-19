



/*******************************************************************************************************************************
 Description:   This Stored Procedure checks to see what tables need to be refreshed on a daily / weekly / monthly basis. 
				It triggers the pipeline to incrementally bring in data on whatever basis the refresh frequency is set to and 
				what source system to pull from.

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE procedure [etl].[usp_IMIS_Tables_Load] (@sourcetype varchar(60), @sourcesystem varchar(30), @refreshfrequency varchar(30))
as

BEGIN

BEGIN TRY

BEGIN TRAN

(
select Source, Target, LastModified,Max_SourceTimestamp, Target_Raw from cfg.Data_Source
where source_type = @sourcetype
and SourceSystemName = @sourcesystem
and refreshfrequency = @refreshfrequency
and Active  = 1
)


COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END
