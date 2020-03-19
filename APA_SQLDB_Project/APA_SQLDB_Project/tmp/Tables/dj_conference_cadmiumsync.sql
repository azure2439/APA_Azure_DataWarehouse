CREATE TABLE [tmp].[dj_conference_cadmiumsync] (
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
    CONSTRAINT [conference_cadmiumsync_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

