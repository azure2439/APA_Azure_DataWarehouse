CREATE TABLE [rpt].[dimSalary] (
    [SalaryIDKey]   INT           IDENTITY (1, 1) NOT NULL,
    [SalaryID]      VARCHAR (30)  NULL,
    [Description]   VARCHAR (255) NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [DF_dimSalary_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [DF_dimSalary_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT           CONSTRAINT [DF_dimSalary_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME      CONSTRAINT [DF_dimSalary_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME      CONSTRAINT [DF_dimSalary_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([SalaryIDKey] ASC)
);

