﻿CREATE TABLE [tmp].[imis_Custom_Event_Registration] (
    [ID]             VARCHAR (10) NOT NULL,
    [SEQN]           INT          DEFAULT ((0)) NOT NULL,
    [MEETING]        VARCHAR (10) DEFAULT ('') NOT NULL,
    [ADDRESS_1]      VARCHAR (40) DEFAULT ('') NOT NULL,
    [ADDRESS_2]      VARCHAR (40) DEFAULT ('') NOT NULL,
    [CITY]           VARCHAR (40) DEFAULT ('') NOT NULL,
    [STATE_PROVINCE] VARCHAR (15) DEFAULT ('') NOT NULL,
    [COUNTRY]        VARCHAR (25) DEFAULT ('') NOT NULL,
    [ZIP]            VARCHAR (10) DEFAULT ('') NOT NULL,
    [BADGE_NAME]     VARCHAR (20) DEFAULT ('') NOT NULL,
    [BADGE_COMPANY]  VARCHAR (80) DEFAULT ('') NOT NULL,
    [BADGE_LOCATION] VARCHAR (60) DEFAULT ('') NOT NULL,
    [TIME_STAMP]     ROWVERSION   NULL,
    CONSTRAINT [PK_Custom_Event_Registration] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

