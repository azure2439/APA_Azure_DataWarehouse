CREATE TABLE [stg].[dj_cities_light_city] (
    [id]              INT            NOT NULL,
    [name_ascii]      VARCHAR (200)  NOT NULL,
    [slug]            VARCHAR (50)   NOT NULL,
    [geoname_id]      INT            NULL,
    [alternate_names] VARCHAR (MAX)  NULL,
    [name]            VARCHAR (200)  NOT NULL,
    [display_name]    VARCHAR (200)  NOT NULL,
    [search_names]    VARCHAR (MAX)  NULL,
    [latitude]        NUMERIC (8, 5) NULL,
    [longitude]       NUMERIC (8, 5) NULL,
    [region_id]       INT            NULL,
    [country_id]      INT            NOT NULL,
    [population]      BIGINT         NULL,
    [feature_code]    VARCHAR (10)   NULL,
    [timezone]        VARCHAR (40)   NULL,
    [LastUpdatedBy]   VARCHAR (40)   CONSTRAINT [df_Cities_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME       CONSTRAINT [df_Cities_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [cities_light_city_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

