CREATE TABLE [tmp].[dj_content_content_permission_groups] (
    [id]           INT      NOT NULL,
    [content_id]   INT      NOT NULL,
    [group_id]     INT      NOT NULL,
    [updated_time] DATETIME NULL,
    CONSTRAINT [content_content_permission_groups_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

