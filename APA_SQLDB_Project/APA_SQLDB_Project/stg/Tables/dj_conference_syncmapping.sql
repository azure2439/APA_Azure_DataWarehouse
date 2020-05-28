CREATE TABLE [stg].[dj_conference_syncmapping] (
    [id]            INT          NOT NULL,
    [mapping_id]    INT          NOT NULL,
    [sync_id]       INT          NOT NULL,
    [updated_time]  DATETIME     NULL,
    [created_time]  DATETIME     NULL,
    [LastUpdatedBy] VARCHAR (40) CONSTRAINT [df_conference_syncmapping_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME     CONSTRAINT [df_conference_syncmapping_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [conference_syncmapping_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

