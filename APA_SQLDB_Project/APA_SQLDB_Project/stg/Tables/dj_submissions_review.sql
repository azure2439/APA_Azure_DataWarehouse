﻿CREATE TABLE [stg].[dj_submissions_review] (
    [id]               INT          NOT NULL,
    [rating_1]         INT          NULL,
    [rating_2]         INT          NULL,
    [rating_3]         INT          NULL,
    [rating_4]         INT          NULL,
    [comments]         TEXT         NULL,
    [deadline_time]    DATETIME     NULL,
    [review_time]      DATETIME     NULL,
    [review_type]      VARCHAR (50) NOT NULL,
    [contact_id]       INT          NOT NULL,
    [content_id]       INT          NOT NULL,
    [role_id]          INT          NULL,
    [recused]          BIT          NOT NULL,
    [review_round]     INT          NOT NULL,
    [assigned_time]    DATETIME     NULL,
    [custom_bit_1]     VARCHAR (4)  NULL,
    [custom_text_1]    TEXT         NULL,
    [custom_text_2]    TEXT         NULL,
    [review_status]    VARCHAR (50) NOT NULL,
    [updated_time]     DATETIME     NULL,
    [created_time]     DATETIME     NULL,
    [custom_boolean_1] BIT          NULL,
    [LastUpdatedBy]    VARCHAR (40) CONSTRAINT [df_submissions_review_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]     DATETIME     CONSTRAINT [df_submissions_review_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [submissions_review_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

