CREATE TABLE [stg].[dj_comments_comment] (
    [id]             INT           NOT NULL,
    [comment_type]   VARCHAR (50)  NOT NULL,
    [submitted_time] DATETIME      NOT NULL,
    [commentary]     VARCHAR (MAX) NULL,
    [rating]         INT           NULL,
    [flagged]        BIT           NOT NULL,
    [publish]        BIT           NOT NULL,
    [contact_id]     INT           NOT NULL,
    [content_id]     INT           NULL,
    [contactrole_id] INT           NULL,
    [updated_time]   DATETIME      NULL,
    [created_time]   DATETIME      NULL,
    [LastUpdatedBy]  VARCHAR (40)  CONSTRAINT [df_comment_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME      CONSTRAINT [df_comment_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [comments_comment_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

