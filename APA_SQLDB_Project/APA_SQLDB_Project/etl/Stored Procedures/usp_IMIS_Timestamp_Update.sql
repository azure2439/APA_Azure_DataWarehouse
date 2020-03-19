



/*******************************************************************************************************************************
 Description:   This Stored Procedure updates the max timestamp field in the config. table equal to the most recent last
				modified date. This allows for records to be brought over on an incremental basis.
				

Added By:		Morgan Diestler on 2/28/2020				
*******************************************************************************************************************************/

CREATE procedure [etl].[usp_IMIS_Timestamp_Update] @Target varchar(60), @LastModified varchar(30)
as

BEGIN

BEGIN TRY

BEGIN TRAN

SET NOCOUNT ON;

update cfg.Data_Source
set Max_SourceTimestamp = @LastModified
where Source = @Target

COMMIT TRAN

END TRY

BEGIN CATCH

IF(@@TRANCOUNT > 0)

ROLLBACK TRAN;

THROW;

END CATCH

END
