CREATE TABLE [stg].[imis_Gen_Tables] (
    [TABLE_NAME]           VARCHAR (30)  CONSTRAINT [DF_Gen_Tables_TABLE_NAME] DEFAULT ('') NOT NULL,
    [CODE]                 VARCHAR (60)  CONSTRAINT [DF_Gen_Tables_CODE] DEFAULT ('') NOT NULL,
    [SUBSTITUTE]           VARCHAR (255) CONSTRAINT [DF_Gen_Tables_SUBSTITUTE] DEFAULT ('') NOT NULL,
    [UPPER_CODE]           VARCHAR (60)  CONSTRAINT [DF_Gen_Tables_UPPER_CODE] DEFAULT ('') NOT NULL,
    [DESCRIPTION]          VARCHAR (255) CONSTRAINT [DF_Gen_Tables_DESCRIPTION] DEFAULT ('') NOT NULL,
    [OBSOLETE_DESCRIPTION] VARCHAR (255) CONSTRAINT [DF_Gen_Tables_OBSOLETE_DESCRIPTION] DEFAULT ('') NOT NULL,
    [NCODE]                NVARCHAR (60) NOT NULL,
    [TIME_STAMP]           BIGINT        NULL,
    [Id_Identity]          INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]        VARCHAR (40)  CONSTRAINT [df_Gen_Tables_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]         DATETIME      CONSTRAINT [df_Gen_Tables_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]             BIT           NULL,
    [StartDate]            DATETIME      CONSTRAINT [df_Gen_Tables_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]              DATETIME      CONSTRAINT [df_Gen_Tables_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Gen_Tables] PRIMARY KEY CLUSTERED ([Id_Identity] ASC)
);

