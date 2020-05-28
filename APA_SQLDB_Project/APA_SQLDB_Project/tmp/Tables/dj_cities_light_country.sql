CREATE TABLE [tmp].[dj_cities_light_country] (
    [id]              INT           NOT NULL,
    [name_ascii]      VARCHAR (200) NOT NULL,
    [slug]            VARCHAR (50)  NOT NULL,
    [geoname_id]      INT           NULL,
    [alternate_names] VARCHAR (MAX) NULL,
    [name]            VARCHAR (200) NOT NULL,
    [code2]           VARCHAR (2)   NULL,
    [code3]           VARCHAR (3)   NULL,
    [continent]       VARCHAR (2)   NOT NULL,
    [tld]             VARCHAR (5)   NOT NULL,
    [phone]           VARCHAR (20)  NULL,
    CONSTRAINT [cities_light_country_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);



