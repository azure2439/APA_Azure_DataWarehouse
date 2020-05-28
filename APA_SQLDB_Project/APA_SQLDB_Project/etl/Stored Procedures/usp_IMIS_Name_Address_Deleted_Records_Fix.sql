/*

This procedure corrects deleted Name_Address records from iMIS.
If the address does not exist in the tmp table, it was deleted.

Set all preferred options to 0.
Set an end date.

*/
CREATE procedure [etl].[usp_IMIS_Name_Address_Deleted_Records_Fix]
AS

BEGIN

UPDATE stg.imis_Name_Address
SET IsActive = 0,
PREFERRED_MAIL = 0,
PREFERRED_BILL = 0,
PREFERRED_SHIP = 0,
EndDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
WHERE ADDRESS_NUM NOT IN (
SELECT ADDRESS_NUM
FROM tmp.imis_Name_Address
)
AND IsActive = 1

END