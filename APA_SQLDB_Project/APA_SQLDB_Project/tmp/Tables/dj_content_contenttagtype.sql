CREATE TABLE [tmp].[dj_content_contenttagtype] (
    [id]              INT          NOT NULL,
    [content_id]      INT          NOT NULL,
    [tag_type_id]     INT          NOT NULL,
    [publish_status]  VARCHAR (50) NOT NULL,
    [publish_time]    DATETIME     NULL,
    [published_by_id] INT          NULL,
    [published_time]  DATETIME     NULL,
    [publish_uuid]    VARCHAR (36) NULL,
    CONSTRAINT [contenttagtype_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

