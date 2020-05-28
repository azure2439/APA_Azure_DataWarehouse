CREATE TABLE [stg].[dj_django_site] (
    [id]            INT           NOT NULL,
    [domain]        VARCHAR (100) NOT NULL,
    [name]          VARCHAR (50)  NOT NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [df_site_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [df_site_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [django_site_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

