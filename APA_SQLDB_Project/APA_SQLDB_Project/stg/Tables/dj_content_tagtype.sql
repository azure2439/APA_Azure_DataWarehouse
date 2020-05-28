CREATE TABLE [stg].[dj_content_tagtype] (
    [id]              INT            NOT NULL,
    [code]            VARCHAR (200)  NULL,
    [title]           VARCHAR (200)  NULL,
    [status]          VARCHAR (5)    NOT NULL,
    [description]     NVARCHAR (MAX) NULL,
    [slug]            VARCHAR (50)   NULL,
    [created_time]    DATETIME       NOT NULL,
    [updated_time]    DATETIME       NOT NULL,
    [min_allowed]     INT            NULL,
    [max_allowed]     INT            NULL,
    [created_by_id]   INT            NOT NULL,
    [parent_id]       INT            NULL,
    [updated_by_id]   INT            NOT NULL,
    [submission_only] BIT            NOT NULL,
    [sort_number]     INT            NULL,
    [LastUpdatedBy]   VARCHAR (40)   CONSTRAINT [df_content_tagtype_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME       CONSTRAINT [df_content_tagtyper_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [tagtype_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

