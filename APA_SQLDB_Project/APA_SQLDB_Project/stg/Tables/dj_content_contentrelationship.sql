CREATE TABLE [stg].[dj_content_contentrelationship] (
    [id]                        INT          NOT NULL,
    [relationship]              VARCHAR (50) NOT NULL,
    [content_id]                INT          NOT NULL,
    [content_master_related_id] INT          NOT NULL,
    [publish_status]            VARCHAR (50) NOT NULL,
    [publish_time]              DATETIME     NULL,
    [publish_uuid]              VARCHAR (36) NULL,
    [published_by_id]           INT          NULL,
    [published_time]            DATETIME     NULL,
    [updated_time]              DATETIME     NULL,
    [created_time]              DATETIME     NULL,
    [LastUpdatedBy]             VARCHAR (40) CONSTRAINT [df_content_relationship_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]              DATETIME     CONSTRAINT [df_content_relationship_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [content_contentrelationship_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

