CREATE TABLE [rpt].[dimRegistrantStatus] (
    [RegistrantStatusKey] INT          IDENTITY (1, 1) NOT NULL,
    [Registrant_Status]   VARCHAR (5)  NULL,
    [StartDate]           DATETIME     CONSTRAINT [DF_dimRstatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]             DATETIME     CONSTRAINT [DF_dimRstatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [LastUpdatedBy]       VARCHAR (40) CONSTRAINT [DF_dimRstatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME     CONSTRAINT [DF_dimRstatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]            BIT          CONSTRAINT [DF_dimRstatus_IsActive] DEFAULT ((1)) NULL,
    PRIMARY KEY CLUSTERED ([RegistrantStatusKey] ASC)
);

