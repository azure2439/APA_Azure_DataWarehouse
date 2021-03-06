﻿CREATE TABLE [tmp].[dj_cm_claim] (
    [id]                  INT            NOT NULL,
    [verified]            BIT            NOT NULL,
    [credits]             NUMERIC (6, 2) NULL,
    [law_credits]         NUMERIC (6, 2) NULL,
    [ethics_credits]      NUMERIC (6, 2) NULL,
    [is_speaker]          BIT            NOT NULL,
    [is_author]           BIT            NOT NULL,
    [self_reported]       BIT            NOT NULL,
    [comment_id]          INT            NULL,
    [contact_id]          INT            NOT NULL,
    [event_id]            INT            NULL,
    [log_id]              INT            NOT NULL,
    [submitted_time]      DATETIME       NOT NULL,
    [begin_time]          VARCHAR (50)   NULL,
    [city]                VARCHAR (40)   NULL,
    [country]             VARCHAR (20)   NULL,
    [description]         VARCHAR (MAX)  NULL,
    [end_time]            VARCHAR (50)   NULL,
    [provider_name]       VARCHAR (80)   NULL,
    [state]               VARCHAR (15)   NULL,
    [title]               VARCHAR (200)  NULL,
    [is_carryover]        BIT            NOT NULL,
    [is_pro_bono]         BIT            NOT NULL,
    [learning_objectives] VARCHAR (MAX)  NULL,
    [timezone]            VARCHAR (50)   NULL,
    [author_type]         VARCHAR (50)   NULL,
    [updated_time]        DATETIME       NULL,
    [created_time]        DATETIME       NULL,
    CONSTRAINT [cm_claim_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);



