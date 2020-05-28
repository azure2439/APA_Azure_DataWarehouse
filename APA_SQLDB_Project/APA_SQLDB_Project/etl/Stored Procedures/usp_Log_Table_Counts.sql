


CREATE PROCEDURE [etl].[usp_Log_Table_Counts]
 (
	 @RowsRead INT
	,@RowsCopied INT
	,@Name NVARCHAR(60)
	,@SourceTable NVARCHAR(60)
	,@TargetTable NVARCHAR(60)
	,@RowsUpdated INT
	)

AS
INSERT INTO etl.executionlog (pipeline_name
	,source_table
	,target_table
	,insert_count
	,update_count
	,rows_read
	)
VALUES (@Name
	    ,@SourceTable
	    ,@TargetTable
        ,@RowsCopied
		,@rowsUpdated
        ,@RowsRead);