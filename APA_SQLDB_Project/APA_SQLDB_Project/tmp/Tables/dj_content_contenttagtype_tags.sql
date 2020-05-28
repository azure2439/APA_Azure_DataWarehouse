CREATE TABLE [tmp].[dj_content_contenttagtype_tags] (
    [id]                INT NOT NULL,
    [contenttagtype_id] INT NOT NULL,
    [tag_id]            INT NOT NULL,
    CONSTRAINT [contenttagtype_tags_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

