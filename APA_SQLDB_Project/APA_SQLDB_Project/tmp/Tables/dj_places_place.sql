CREATE TABLE [tmp].[dj_places_place] (
    [id]                    INT           NOT NULL,
    [code]                  VARCHAR (200) NULL,
    [title]                 VARCHAR (200) NULL,
    [status]                VARCHAR (5)   NOT NULL,
    [description]           TEXT          NULL,
    [slug]                  VARCHAR (50)  NULL,
    [created_time]          DATETIME      NOT NULL,
    [updated_time]          DATETIME      NOT NULL,
    [place_type]            VARCHAR (50)  NOT NULL,
    [country]               VARCHAR (50)  NULL,
    [state_code]            VARCHAR (15)  NULL,
    [state_name]            VARCHAR (200) NULL,
    [place_descriptor_name] VARCHAR (200) NULL,
    [census_geo_id]         VARCHAR (50)  NULL,
    [un_region_id]          VARCHAR (50)  NULL,
    [created_by_id]         INT           NOT NULL,
    [region_id]             INT           NULL,
    [updated_by_id]         INT           NOT NULL,
    [lsad]                  VARCHAR (5)   NOT NULL,
    CONSTRAINT [places_place_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

