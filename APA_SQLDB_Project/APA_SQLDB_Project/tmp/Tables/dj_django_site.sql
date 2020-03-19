CREATE TABLE [tmp].[dj_django_site] (
    [id]     INT           NOT NULL,
    [domain] VARCHAR (100) NOT NULL,
    [name]   VARCHAR (50)  NOT NULL,
    CONSTRAINT [django_site_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

