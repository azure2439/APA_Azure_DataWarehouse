CREATE TABLE [stg].[imis_Custom_Event_Schedule] (
    [ID]                VARCHAR (10) DEFAULT ('') NOT NULL,
    [SEQN]              INT          DEFAULT ((0)) NOT NULL,
    [MEETING]           VARCHAR (10) DEFAULT ('') NOT NULL,
    [REGISTRANT_CLASS]  VARCHAR (10) DEFAULT ('') NOT NULL,
    [PRODUCT_CODE]      VARCHAR (31) DEFAULT ('') NOT NULL,
    [STATUS]            VARCHAR (5)  DEFAULT ('') NOT NULL,
    [UNIT_PRICE]        MONEY        DEFAULT ((0)) NOT NULL,
    [IS_WAITLIST]       BIT          DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_eventschLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_eventschLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_eventschStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_eventschEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Custom_Event_Schedule] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

