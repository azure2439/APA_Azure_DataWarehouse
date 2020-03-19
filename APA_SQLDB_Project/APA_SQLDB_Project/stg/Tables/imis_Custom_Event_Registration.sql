CREATE TABLE [stg].[imis_Custom_Event_Registration] (
    [ID]                VARCHAR (10) DEFAULT ('') NOT NULL,
    [SEQN]              INT          DEFAULT ((0)) NOT NULL,
    [MEETING]           VARCHAR (10) DEFAULT ('') NOT NULL,
    [ADDRESS_1]         VARCHAR (40) DEFAULT ('') NOT NULL,
    [ADDRESS_2]         VARCHAR (40) DEFAULT ('') NOT NULL,
    [CITY]              VARCHAR (40) DEFAULT ('') NOT NULL,
    [STATE_PROVINCE]    VARCHAR (15) DEFAULT ('') NOT NULL,
    [COUNTRY]           VARCHAR (25) DEFAULT ('') NOT NULL,
    [ZIP]               VARCHAR (10) DEFAULT ('') NOT NULL,
    [BADGE_NAME]        VARCHAR (20) DEFAULT ('') NOT NULL,
    [BADGE_COMPANY]     VARCHAR (80) DEFAULT ('') NOT NULL,
    [BADGE_LOCATION]    VARCHAR (60) DEFAULT ('') NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_eventLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_eventLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_eventStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_eventEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Custom_Event_Registration] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

