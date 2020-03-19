CREATE TABLE [tmp].[dj_django_redirect] (
    [id]       INT           NOT NULL,
    [site_id]  INT           NOT NULL,
    [old_path] VARCHAR (200) NOT NULL,
    [new_path] VARCHAR (200) NOT NULL,
    CONSTRAINT [django_redirect_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

