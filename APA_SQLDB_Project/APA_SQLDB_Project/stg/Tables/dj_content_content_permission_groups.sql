CREATE TABLE [stg].[dj_content_content_permission_groups] (
    [id]            INT          NOT NULL,
    [content_id]    INT          NOT NULL,
    [group_id]      INT          NOT NULL,
    [updated_time]  DATETIME     NULL,
    [LastUpdatedBy] VARCHAR (40) CONSTRAINT [df_content_permissions_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME     CONSTRAINT [df_content_permissions_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [content_content_permission_groups_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

