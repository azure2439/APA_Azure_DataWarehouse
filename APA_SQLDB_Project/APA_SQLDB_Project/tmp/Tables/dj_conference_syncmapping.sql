CREATE TABLE [tmp].[dj_conference_syncmapping] (
    [id]           INT      NOT NULL,
    [mapping_id]   INT      NOT NULL,
    [sync_id]      INT      NOT NULL,
    [updated_time] DATETIME NULL,
    [created_time] DATETIME NULL,
    CONSTRAINT [conference_syncmapping_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

