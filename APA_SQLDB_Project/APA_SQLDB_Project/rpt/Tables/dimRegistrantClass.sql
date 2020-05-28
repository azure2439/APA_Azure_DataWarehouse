CREATE TABLE [rpt].[dimRegistrantClass] (
    [RegistrantClassKey] INT          IDENTITY (1, 1) NOT NULL,
    [Registrant_Class]   VARCHAR (10) NULL,
    [StartDate]          DATETIME     CONSTRAINT [DF_dimRclass_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME     CONSTRAINT [DF_dimRclass_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [LastUpdatedBy]      VARCHAR (40) CONSTRAINT [DF_dimRclass_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME     CONSTRAINT [DF_dimRclass_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT          CONSTRAINT [DF_dimRclass_IsActive] DEFAULT ((1)) NULL,
    PRIMARY KEY CLUSTERED ([RegistrantClassKey] ASC)
);

