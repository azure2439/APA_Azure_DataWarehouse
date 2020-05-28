CREATE TABLE [rpt].[dimAICPExamRegistrantType] (
    [RegistrantTypeIDKey] INT           IDENTITY (1, 1) NOT NULL,
    [Registrant_Type]     VARCHAR (200) NULL,
    [Title]               VARCHAR (200) NULL,
    [LastUpdatedBy]       VARCHAR (40)  CONSTRAINT [DF_dimAICPExamRegistrantType_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME      CONSTRAINT [DF_dimAICPExamRegistrantType_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]            BIT           CONSTRAINT [DF_dimAICPExamRegistrantType_IsActive] DEFAULT ((1)) NULL,
    [StartDate]           DATETIME      CONSTRAINT [DF_dimAICPExamRegistrantType_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]             DATETIME      CONSTRAINT [DF_dimAICPExamRegistrantType_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([RegistrantTypeIDKey] ASC)
);

