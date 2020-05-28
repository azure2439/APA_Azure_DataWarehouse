CREATE TABLE [stg].[dj_pages_landingpage] (
    [content_ptr_id]      INT          NOT NULL,
    [search_query]        TEXT         NULL,
    [sort_field]          VARCHAR (50) NOT NULL,
    [search_max]          INT          NOT NULL,
    [show_search_results] BIT          NOT NULL,
    [LastUpdatedBy]       VARCHAR (40) CONSTRAINT [df_landingpage_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME     CONSTRAINT [df_landingpage_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [pages_landingpage_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

