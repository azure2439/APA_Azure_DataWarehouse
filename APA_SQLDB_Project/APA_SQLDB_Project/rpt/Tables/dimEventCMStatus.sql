CREATE TABLE [rpt].[dimEventCMStatus] (
    [EventCMStatusIDKey] INT           IDENTITY (1, 1) NOT NULL,
    [EventCMStatusCode]  VARCHAR (50)  NOT NULL,
    [Title]              VARCHAR (200) NOT NULL,
    [LastUpdatedBy]      VARCHAR (40)  CONSTRAINT [DF_dimEventCMStatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME      CONSTRAINT [DF_dimEventCMStatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT           CONSTRAINT [DF_dimEventCMStatus_IsActive] DEFAULT ((1)) NULL,
    [StartDate]          DATETIME      CONSTRAINT [DF_dimEventCMStatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME      CONSTRAINT [DF_dimEventCMStatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([EventCMStatusIDKey] ASC)
);

