CREATE TABLE [stg].[dj_conference_microsite_program_search_filters] (
    [id]            INT          NOT NULL,
    [microsite_id]  INT          NOT NULL,
    [tagtype_id]    INT          NOT NULL,
    [updated_time]  DATETIME     NULL,
    [LastUpdatedBy] VARCHAR (40) CONSTRAINT [df_conference_microsite_program_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME     CONSTRAINT [df_conference_microsite_program_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [conference_microsite_program_search_filters_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

