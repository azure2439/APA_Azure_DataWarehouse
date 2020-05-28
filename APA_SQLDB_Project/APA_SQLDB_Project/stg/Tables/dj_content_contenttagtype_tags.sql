CREATE TABLE [stg].[dj_content_contenttagtype_tags] (
    [id]                INT          NOT NULL,
    [contenttagtype_id] INT          NOT NULL,
    [tag_id]            INT          NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_content_contenttagtype_tags_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_content_contenttagtype_tags_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [contenttagtype_tags_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

