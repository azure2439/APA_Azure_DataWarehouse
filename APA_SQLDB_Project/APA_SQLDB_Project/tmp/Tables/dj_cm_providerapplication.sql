﻿CREATE TABLE [tmp].[dj_cm_providerapplication] (
    [id]                       INT           NOT NULL,
    [explain_topics]           VARCHAR (MAX) NULL,
    [objectives_status]        VARCHAR (50)  NULL,
    [objectives_example_1]     VARCHAR (MAX) NULL,
    [objectives_example_2]     VARCHAR (MAX) NULL,
    [how_determines_speakers]  VARCHAR (MAX) NULL,
    [evaluates_activities]     BIT           NOT NULL,
    [evaluation_procedures]    VARCHAR (MAX) NULL,
    [agree_keep_records]       BIT           NOT NULL,
    [provider_id]              INT           NOT NULL,
    [status]                   VARCHAR (50)  NOT NULL,
    [submitted_time]           DATETIME      NULL,
    [year]                     INT           NOT NULL,
    [begin_date]               DATE          NULL,
    [end_date]                 DATE          NULL,
    [review_notes]             VARCHAR (MAX) NULL,
    [review_notification_time] DATETIME      NULL,
    [review_status]            VARCHAR (50)  NULL,
    [objectives_example_3]     VARCHAR (MAX) NULL,
    [supporting_upload_1]      VARCHAR (255) NULL,
    [supporting_upload_2]      VARCHAR (255) NULL,
    [supporting_upload_3]      VARCHAR (255) NULL,
    [provider_notes]           VARCHAR (MAX) NULL,
    [updated_time]             DATETIME      NULL,
    [created_time]             DATETIME      NULL,
    CONSTRAINT [cm_providerapplication_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);



