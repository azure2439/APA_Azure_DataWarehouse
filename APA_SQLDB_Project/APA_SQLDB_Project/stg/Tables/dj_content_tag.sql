﻿CREATE TABLE [stg].[dj_content_tag] (
    [id]            INT            NOT NULL,
    [code]          VARCHAR (200)  NULL,
    [title]         VARCHAR (200)  NULL,
    [status]        VARCHAR (5)    NOT NULL,
    [description]   NVARCHAR (MAX) NULL,
    [slug]          VARCHAR (50)   NULL,
    [created_time]  DATETIME       NOT NULL,
    [updated_time]  DATETIME       NOT NULL,
    [sort_number]   INT            NULL,
    [taxo_term]     VARCHAR (200)  NULL,
    [created_by_id] INT            NOT NULL,
    [parent_id]     INT            NULL,
    [tag_type_id]   INT            NOT NULL,
    [updated_by_id] INT            NOT NULL,
    [LastUpdatedBy] VARCHAR (40)   CONSTRAINT [df_content_tag_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME       CONSTRAINT [df_content_tag_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [contenttag_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

