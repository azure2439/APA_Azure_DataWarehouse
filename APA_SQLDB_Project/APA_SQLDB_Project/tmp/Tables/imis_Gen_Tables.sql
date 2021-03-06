﻿CREATE TABLE [tmp].[imis_Gen_Tables] (
    [TABLE_NAME]           VARCHAR (30)  CONSTRAINT [DF_Gen_Tables_TABLE_NAME] DEFAULT ('') NOT NULL,
    [CODE]                 VARCHAR (60)  CONSTRAINT [DF_Gen_Tables_CODE] DEFAULT ('') NOT NULL,
    [SUBSTITUTE]           VARCHAR (255) CONSTRAINT [DF_Gen_Tables_SUBSTITUTE] DEFAULT ('') NOT NULL,
    [UPPER_CODE]           VARCHAR (60)  CONSTRAINT [DF_Gen_Tables_UPPER_CODE] DEFAULT ('') NOT NULL,
    [DESCRIPTION]          VARCHAR (255) CONSTRAINT [DF_Gen_Tables_DESCRIPTION] DEFAULT ('') NOT NULL,
    [OBSOLETE_DESCRIPTION] VARCHAR (255) CONSTRAINT [DF_Gen_Tables_OBSOLETE_DESCRIPTION] DEFAULT ('') NOT NULL,
    [NCODE]                NVARCHAR (60) NOT NULL,
    [TIME_STAMP]           ROWVERSION    NULL,
    CONSTRAINT [PK_Gen_Tables] PRIMARY KEY CLUSTERED ([TABLE_NAME] ASC, [CODE] ASC)
);

