




/*******************************************************************************************************************************
 Description:   This Stored Procedure counts the number of records that were inserted vs. updated and from which source table 
				these records were coming from. 
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/

CREATE PROCEDURE [etl].[zzz_usp_IMIS_Log_Table_Counts] (
	@RowsRead INT
	,@RowsCopied INT
	,@Name NVARCHAR(60)
	,@SourceTable NVARCHAR(60)
	,@TargetTable NVARCHAR(60)
	)
AS

BEGIN

BEGIN TRY

BEGIN TRAN

SELECT pipeline_name
	,source_table
	,target_table
	,insert_count
	,update_count
	,datetimestamp
FROM [etl].[executionlog]
WHERE pipeline_name = @Name
	AND source_table = @SourceTable
	AND target_table = @TargetTable
	AND insert_count = @RowsCopied
	AND update_count = @RowsRead;


COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END
