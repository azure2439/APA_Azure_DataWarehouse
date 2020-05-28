CREATE TABLE [stg].[imis_Meet_Reg_Class] (
    [REGISTRANT_CLASS]  VARCHAR (5)  CONSTRAINT [DF_Meet_Reg_Class_REGISTRANT_CLASS] DEFAULT ('') NOT NULL,
    [DESCRIPTION]       VARCHAR (20) CONSTRAINT [DF_Meet_Reg_Class_DESCRIPTION] DEFAULT ('') NOT NULL,
    [LONG_DESCRIPTION]  VARCHAR (40) CONSTRAINT [DF_Meet_Reg_Class_LONG_DESCRIPTION] DEFAULT ('') NOT NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_Meet_Reg_Class_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_Meet_Reg_Class_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_Meet_Reg_Class_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_Meet_Reg_Class_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Meet_Reg_Class] PRIMARY KEY CLUSTERED ([REGISTRANT_CLASS] ASC)
);

