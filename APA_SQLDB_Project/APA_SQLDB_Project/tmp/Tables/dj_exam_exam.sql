﻿CREATE TABLE [tmp].[dj_exam_exam] (
    [id]                         INT           NOT NULL,
    [code]                       VARCHAR (200) NULL,
    [title]                      VARCHAR (200) NULL,
    [status]                     VARCHAR (5)   NOT NULL,
    [description]                TEXT          NULL,
    [slug]                       VARCHAR (50)  NULL,
    [created_time]               DATETIME      NOT NULL,
    [updated_time]               DATETIME      NOT NULL,
    [start_time]                 DATETIME      NULL,
    [end_time]                   DATETIME      NULL,
    [registration_start_time]    DATETIME      NULL,
    [registration_end_time]      DATETIME      NULL,
    [application_start_time]     DATETIME      NULL,
    [application_end_time]       DATETIME      NULL,
    [application_early_end_time] DATETIME      NULL,
    [is_advanced]                BIT           NOT NULL,
    [created_by_id]              INT           NOT NULL,
    [updated_by_id]              INT           NOT NULL,
    [product_id]                 INT           NULL,
    CONSTRAINT [exam_exam_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

