CREATE TABLE [stg].[dj_django_redirect] (
    [id]            INT           NOT NULL,
    [site_id]       INT           NOT NULL,
    [old_path]      VARCHAR (200) NOT NULL,
    [new_path]      VARCHAR (200) NOT NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [df_redirect_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [df_redirect_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [django_redirect_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

