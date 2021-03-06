﻿CREATE TABLE [stg].[dj_cm_period] (
    [id]               INT           NOT NULL,
    [code]             VARCHAR (200) NULL,
    [title]            VARCHAR (200) NULL,
    [status]           VARCHAR (5)   NOT NULL,
    [description]      VARCHAR (MAX) NULL,
    [slug]             VARCHAR (50)  NULL,
    [created_time]     DATETIME      NOT NULL,
    [updated_time]     DATETIME      NOT NULL,
    [begin_time]       DATETIME      NULL,
    [end_time]         DATETIME      NULL,
    [grace_end_time]   DATETIME      NULL,
    [created_by_id]    INT           NOT NULL,
    [rollover_from_id] INT           NULL,
    [updated_by_id]    INT           NOT NULL,
    [LastUpdatedBy]    VARCHAR (40)  CONSTRAINT [df_cm_period_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]     DATETIME      CONSTRAINT [df_cm_period_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [cm_period_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

