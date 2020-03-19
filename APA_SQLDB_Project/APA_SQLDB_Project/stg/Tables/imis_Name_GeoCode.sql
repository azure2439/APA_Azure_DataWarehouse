CREATE TABLE [stg].[imis_Name_GeoCode] (
    [ID]                VARCHAR (10) DEFAULT ('') NOT NULL,
    [SEQN]              INT          DEFAULT ((0)) NOT NULL,
    [LONGITUDE]         FLOAT (53)   DEFAULT ((0)) NOT NULL,
    [LATITUDE]          FLOAT (53)   DEFAULT ((0)) NOT NULL,
    [ADDRESS_NUM]       INT          DEFAULT ((0)) NOT NULL,
    [CHANGED]           VARCHAR (24) DEFAULT ('') NOT NULL,
    [Status_Code]       VARCHAR (10) DEFAULT ('') NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_geocodeLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_geocodeLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_geocodeStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_geocodeEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Name_GeoCode] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

