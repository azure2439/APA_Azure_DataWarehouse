CREATE TABLE [dbo].[zzz_dimdate] (
    [Date_Key]        INT          IDENTITY (1, 1) NOT NULL,
    [date]            DATE         NULL,
    [Day]             INT          NULL,
    [FiscalMonth]     INT          NULL,
    [FiscalQuarter]   INT          NULL,
    [FiscalYear]      INT          NULL,
    [CalendarMonth]   INT          NULL,
    [CalendarQuarter] INT          NULL,
    [CalendarYear]    INT          NULL,
    [LastUpdatedBy]   VARCHAR (40) NULL,
    [LastModified]    DATETIME     NULL
);

