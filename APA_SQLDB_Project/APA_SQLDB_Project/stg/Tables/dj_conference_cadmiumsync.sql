CREATE TABLE [stg].[dj_conference_cadmiumsync] (
    [id]                         INT           NOT NULL,
    [cadmium_event_key]          VARCHAR (200) NULL,
    [registration_task_id]       VARCHAR (20)  NULL,
    [endpoint]                   VARCHAR (255) NULL,
    [microsite_id]               INT           NULL,
    [parent_landing_master_id]   INT           NULL,
    [activity_tag_type_code]     VARCHAR (200) NULL,
    [division_tag_type_code]     VARCHAR (200) NULL,
    [npc_category_tag_type_code] VARCHAR (200) NULL,
    [track_tag_type_code]        VARCHAR (200) NULL,
    [updated_time]               DATETIME      NULL,
    [created_time]               DATETIME      NULL,
    [LastUpdatedBy]              VARCHAR (40)  CONSTRAINT [df_conference_cadmium_sync_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]               DATETIME      CONSTRAINT [df_conference_cadmium_sync_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [conference_cadmiumsync_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

