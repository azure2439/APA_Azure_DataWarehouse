﻿CREATE TABLE [tmp].[imis_PAS_Demographics] (
    [ID]              VARCHAR (10) NOT NULL,
    [PAS_TYPE]        VARCHAR (20) CONSTRAINT [DF__PAS_Demog__PAS_T__641105FB] DEFAULT ('') NOT NULL,
    [POP_CITY_COUNTY] VARCHAR (20) CONSTRAINT [DF__PAS_Demog__POP_C__65052A34] DEFAULT ('') NOT NULL,
    [POP_REG_STATE]   VARCHAR (20) CONSTRAINT [DF__PAS_Demog__POP_R__65F94E6D] DEFAULT ('') NOT NULL,
    [TIME_STAMP]      ROWVERSION   NULL,
    CONSTRAINT [pkPAS_DemographicsID] PRIMARY KEY CLUSTERED ([ID] ASC)
);

