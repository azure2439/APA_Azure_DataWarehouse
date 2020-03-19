CREATE TABLE [tmp].[dj_comments_comment] (
    [id]             INT          NOT NULL,
    [comment_type]   VARCHAR (50) NOT NULL,
    [submitted_time] DATETIME     NOT NULL,
    [commentary]     TEXT         NULL,
    [rating]         INT          NULL,
    [flagged]        BIT          NOT NULL,
    [publish]        BIT          NOT NULL,
    [contact_id]     INT          NOT NULL,
    [content_id]     INT          NULL,
    [contactrole_id] INT          NULL,
    [updated_time]   DATETIME     NULL,
    [created_time]   DATETIME     NULL,
    CONSTRAINT [comments_comment_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

