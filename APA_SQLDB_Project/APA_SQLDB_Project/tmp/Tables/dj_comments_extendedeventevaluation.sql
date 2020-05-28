CREATE TABLE [tmp].[dj_comments_extendedeventevaluation] (
    [comment_ptr_id]         INT           NOT NULL,
    [objective_rating]       INT           NULL,
    [value_rating]           INT           NULL,
    [learn_more_choice]      VARCHAR (25)  NULL,
    [commentary_suggestions] VARCHAR (MAX) NULL,
    [commentary_takeaways]   VARCHAR (MAX) NULL,
    [updated_time]           DATETIME      NULL,
    CONSTRAINT [comments_extendedeventevaluation_pkey] PRIMARY KEY CLUSTERED ([comment_ptr_id] ASC)
);



