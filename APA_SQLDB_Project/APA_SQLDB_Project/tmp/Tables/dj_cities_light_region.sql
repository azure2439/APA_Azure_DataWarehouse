﻿CREATE TABLE [tmp].[dj_cities_light_region] (
    [id]              INT           NOT NULL,
    [name_ascii]      VARCHAR (200) NOT NULL,
    [slug]            VARCHAR (50)  NOT NULL,
    [geoname_id]      INT           NULL,
    [alternate_names] VARCHAR (MAX) NULL,
    [name]            VARCHAR (200) NOT NULL,
    [display_name]    VARCHAR (200) NOT NULL,
    [geoname_code]    VARCHAR (50)  NULL,
    [country_id]      INT           NOT NULL,
    CONSTRAINT [cities_light_region_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);



