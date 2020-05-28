CREATE TABLE [rpt].[dimContentStatus] (
    [ContentStatusKey] INT           IDENTITY (1, 1) NOT NULL,
    [Status]           VARCHAR (5)   NULL,
    [Title]            VARCHAR (100) NULL,
    [LastUpdatedBy]    VARCHAR (40)  CONSTRAINT [DF_dimContentStatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]     DATETIME      CONSTRAINT [DF_dimContentStatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]         BIT           CONSTRAINT [DF_dimContentStatus_IsActive] DEFAULT ((1)) NULL,
    [StartDate]        DATETIME      CONSTRAINT [DF_dimContentStatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]          DATETIME      CONSTRAINT [DF_dimContentStatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([ContentStatusKey] ASC)
);

