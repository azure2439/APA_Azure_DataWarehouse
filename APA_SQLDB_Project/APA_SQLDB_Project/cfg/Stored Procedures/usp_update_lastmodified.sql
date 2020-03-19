

/*******************************************************************************************************************************
 Description:   This Stored Procedure updates the last modified date in the config table with the max timestamp value from the 
				source table. Updating the last modified with this values allows for an incremental load of data, meaning we 
				are only bringing over the new or updated records.

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/


CREATE procedure [cfg].[usp_update_lastmodified] (
@SourceSystemName varchar(60),
@RefreshFrequency varchar(60)
)
as

BEGIN

BEGIN TRY

BEGIN TRAN

update [cfg].[Data_Source]
set lastmodified = max_sourcetimestamp
where sourcesystemname = @sourcesystemname and
refreshfrequency = @refreshfrequency

COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END
