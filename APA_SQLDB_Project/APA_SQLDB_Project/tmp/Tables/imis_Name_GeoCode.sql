﻿CREATE TABLE [tmp].[imis_Name_GeoCode] (
    [ID]          VARCHAR (10) NOT NULL,
    [SEQN]        INT          DEFAULT ((0)) NOT NULL,
    [LONGITUDE]   FLOAT (53)   DEFAULT ((0)) NOT NULL,
    [LATITUDE]    FLOAT (53)   DEFAULT ((0)) NOT NULL,
    [ADDRESS_NUM] INT          DEFAULT ((0)) NOT NULL,
    [CHANGED]     VARCHAR (24) DEFAULT ('') NOT NULL,
    [Status_Code] VARCHAR (10) DEFAULT ('') NOT NULL,
    [TIME_STAMP]  ROWVERSION   NULL,
    CONSTRAINT [PK_Name_GeoCode] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

