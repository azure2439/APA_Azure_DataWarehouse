CREATE TABLE [tmp].[dj_conference_microsite_program_search_filters] (
    [id]           INT      NOT NULL,
    [microsite_id] INT      NOT NULL,
    [tagtype_id]   INT      NOT NULL,
    [updated_time] DATETIME NULL,
    CONSTRAINT [conference_microsite_program_search_filters_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

