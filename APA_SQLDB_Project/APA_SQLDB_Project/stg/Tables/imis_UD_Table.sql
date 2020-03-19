CREATE TABLE [stg].[imis_UD_Table] (
    [TABLE_NAME]               VARCHAR (30)  CONSTRAINT [DF_UD_Table_TABLE_NAME] DEFAULT ('') NOT NULL,
    [DESCRIPTION]              VARCHAR (60)  CONSTRAINT [DF_UD_Table_DESCRIPTION] DEFAULT ('') NOT NULL,
    [APPLICATION]              VARCHAR (15)  CONSTRAINT [DF_UD_Table_APPLICATION] DEFAULT ('') NOT NULL,
    [EXTERNAL_FLAG]            BIT           CONSTRAINT [DF_UD_Table_EXTERNAL_FLAG] DEFAULT ((0)) NOT NULL,
    [LINK_VIA]                 VARCHAR (30)  CONSTRAINT [DF_UD_Table_LINK_VIA] DEFAULT ('') NOT NULL,
    [ALLOW_MULTIPLE_INSTANCES] BIT           CONSTRAINT [DF_UD_Table_ALLOW_MULTIPLE_INSTANCES] DEFAULT ((0)) NOT NULL,
    [REQUIRED]                 BIT           CONSTRAINT [DF_UD_Table_REQUIRED] DEFAULT ((0)) NOT NULL,
    [EDIT_TYPES]               VARCHAR (100) CONSTRAINT [DF_UD_Table_EDIT_TYPES] DEFAULT ('') NOT NULL,
    [NAME_ALL_TABLE]           BIT           CONSTRAINT [DF_UD_Table_NAME_ALL_TABLE] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]               BIGINT        NULL,
    [Id_Identitycolumn]        INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]            VARCHAR (40)  CONSTRAINT [df_udLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]             DATETIME      CONSTRAINT [df_udLastModified] DEFAULT (getdate()) NULL,
    [IsActive]                 BIT           NULL,
    [StartDate]                DATETIME      CONSTRAINT [df_udStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                  DATETIME      CONSTRAINT [df_udEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_UD_Table] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

