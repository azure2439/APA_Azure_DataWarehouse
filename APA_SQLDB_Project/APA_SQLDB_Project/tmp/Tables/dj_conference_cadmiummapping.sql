CREATE TABLE [tmp].[dj_conference_cadmiummapping] (
    [id]           INT           NOT NULL,
    [mapping_type] VARCHAR (100) NULL,
    [from_string]  VARCHAR (200) NULL,
    [to_string]    VARCHAR (200) NULL,
    [updated_time] DATETIME      NULL,
    [created_time] DATETIME      NULL,
    CONSTRAINT [conference_cadmiummapping_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

