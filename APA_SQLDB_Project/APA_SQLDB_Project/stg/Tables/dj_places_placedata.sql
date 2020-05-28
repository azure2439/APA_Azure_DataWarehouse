CREATE TABLE [stg].[dj_places_placedata] (
    [id]             INT           NOT NULL,
    [priority]       INT           NOT NULL,
    [year]           INT           NOT NULL,
    [source_name]    VARCHAR (200) NULL,
    [population]     INT           NULL,
    [housing_units]  INT           NULL,
    [density]        INT           NULL,
    [latitude]       FLOAT (53)    NULL,
    [longitude]      FLOAT (53)    NULL,
    [land_sq_miles]  FLOAT (53)    NULL,
    [water_sq_miles] FLOAT (53)    NULL,
    [place_id]       INT           NOT NULL,
    [updated_time]   DATETIME      NULL,
    [created_time]   DATETIME      NULL,
    [LastUpdatedBy]  VARCHAR (40)  CONSTRAINT [df_placedata_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME      CONSTRAINT [df_placedata_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [places_placedata_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

