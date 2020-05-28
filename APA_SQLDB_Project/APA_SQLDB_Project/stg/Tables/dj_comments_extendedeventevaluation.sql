CREATE TABLE [stg].[dj_comments_extendedeventevaluation] (
    [comment_ptr_id]         INT           NOT NULL,
    [objective_rating]       INT           NULL,
    [value_rating]           INT           NULL,
    [learn_more_choice]      VARCHAR (25)  NULL,
    [commentary_suggestions] VARCHAR (MAX) NULL,
    [commentary_takeaways]   VARCHAR (MAX) NULL,
    [updated_time]           DATETIME      NULL,
    [LastUpdatedBy]          VARCHAR (40)  CONSTRAINT [df_extended_event_eval_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]           DATETIME      CONSTRAINT [df_extended_event_eval_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [comments_extendedeventevaluation_pkey] PRIMARY KEY CLUSTERED ([comment_ptr_id] ASC)
);

