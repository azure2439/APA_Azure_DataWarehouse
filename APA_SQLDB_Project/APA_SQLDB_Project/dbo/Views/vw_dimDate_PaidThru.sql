CREATE VIEW vw_dimDate_PaidThru AS
SELECT 
[Date_Key]
      ,[date]
      ,[Day]
      ,[FiscalMonth]
      ,[FiscalQuarter]
      ,[FiscalYear]
      ,[CalendarMonth]
      ,[CalendarQuarter]
      ,[CalendarYear]
      ,[LastUpdatedBy]
      ,[LastModified]
FROM [rpt].[dimDate]
