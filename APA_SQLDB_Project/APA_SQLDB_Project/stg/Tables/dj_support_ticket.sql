CREATE TABLE [stg].[dj_support_ticket] (
    [id]             INT           NOT NULL,
    [code]           VARCHAR (200) NULL,
    [title]          VARCHAR (200) NULL,
    [status]         VARCHAR (5)   NOT NULL,
    [description]    TEXT          NULL,
    [slug]           VARCHAR (50)  NULL,
    [created_time]   DATETIME      NOT NULL,
    [updated_time]   DATETIME      NOT NULL,
    [apa_id]         VARCHAR (10)  NULL,
    [full_name]      VARCHAR (100) NULL,
    [email]          VARCHAR (100) NULL,
    [phone]          VARCHAR (20)  NULL,
    [contact_id]     INT           NULL,
    [created_by_id]  INT           NOT NULL,
    [updated_by_id]  INT           NOT NULL,
    [category]       VARCHAR (50)  NOT NULL,
    [ticket_status]  VARCHAR (5)   NOT NULL,
    [staff_comments] TEXT          NULL,
    [LastUpdatedBy]  VARCHAR (40)  CONSTRAINT [df_support_ticket_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME      CONSTRAINT [df_support_ticket_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [support_ticket_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

