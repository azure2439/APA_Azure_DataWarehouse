CREATE TABLE [tmp].[dj_places_contentplace] (
    [id]                INT          NOT NULL,
    [published_time]    DATETIME     NULL,
    [publish_time]      DATETIME     NULL,
    [publish_status]    VARCHAR (50) NOT NULL,
    [publish_uuid]      VARCHAR (36) NULL,
    [tag_parent_state]  BIT          NOT NULL,
    [tag_parent_region] BIT          NOT NULL,
    [tag_place_data]    BIT          NOT NULL,
    [sort_number]       INT          NULL,
    [content_id]        INT          NOT NULL,
    [place_id]          INT          NOT NULL,
    [published_by_id]   INT          NULL,
    [updated_time]      DATETIME     NULL,
    [created_time]      DATETIME     NULL,
    CONSTRAINT [places_contentplace_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

