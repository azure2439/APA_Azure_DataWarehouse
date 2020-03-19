CREATE TABLE [rpt].[dimDate] (
    [Date_Key]        INT          IDENTITY (1, 1) NOT NULL,
    [date]            DATE         NULL,
    [Day]             INT          NULL,
    [FiscalMonth]     INT          NULL,
    [FiscalQuarter]   INT          NULL,
    [FiscalYear]      INT          NULL,
    [CalendarMonth]   INT          NULL,
    [CalendarQuarter] INT          NULL,
    [CalendarYear]    INT          NULL,
    [LastUpdatedBy]   VARCHAR (40) CONSTRAINT [DF_dimDate_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME     CONSTRAINT [DF_dimDate_LastModified] DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([Date_Key] ASC)
);

