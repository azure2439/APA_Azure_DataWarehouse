CREATE TABLE [stg].[dj_conference_cadmiummapping] (
    [id]            INT           NOT NULL,
    [mapping_type]  VARCHAR (100) NULL,
    [from_string]   VARCHAR (200) NULL,
    [to_string]     VARCHAR (200) NULL,
    [updated_time]  DATETIME      NULL,
    [created_time]  DATETIME      NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [df_conference_cadmium_map_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [df_conference_cadmium_map_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [conference_cadmiummapping_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

