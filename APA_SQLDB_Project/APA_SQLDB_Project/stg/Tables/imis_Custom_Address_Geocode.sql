CREATE TABLE [stg].[imis_Custom_Address_Geocode] (
    [ID]                  VARCHAR (10)  DEFAULT ('') NOT NULL,
    [SEQN]                INT           DEFAULT ((0)) NOT NULL,
    [ADDRESS_NUM]         INT           DEFAULT ((0)) NOT NULL,
    [VOTERVOICE_CHECKSUM] VARCHAR (255) DEFAULT ('') NOT NULL,
    [LONGITUDE]           FLOAT (53)    DEFAULT ((0)) NOT NULL,
    [LATITUDE]            FLOAT (53)    DEFAULT ((0)) NOT NULL,
    [WEAK_COORDINATES]    INT           DEFAULT ((0)) NOT NULL,
    [US_CONGRESS]         VARCHAR (100) DEFAULT ('') NOT NULL,
    [STATE_SENATE]        VARCHAR (100) DEFAULT ('') NOT NULL,
    [STATE_HOUSE]         VARCHAR (100) DEFAULT ('') NOT NULL,
    [CHANGED]             BIT           DEFAULT ((0)) NOT NULL,
    [LAST_UPDATED]        DATETIME      NULL,
    [TIME_STAMP]          BIGINT        NULL,
    [Id_Identitycolumn]   INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]       VARCHAR (40)  CONSTRAINT [df_customgeoLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME      CONSTRAINT [df_customgeoLastModified] DEFAULT (getdate()) NULL,
    [IsActive]            BIT           NULL,
    [StartDate]           DATETIME      CONSTRAINT [df_customgeoStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]             DATETIME      CONSTRAINT [df_customgeoEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Custom_Address_Geocode] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

