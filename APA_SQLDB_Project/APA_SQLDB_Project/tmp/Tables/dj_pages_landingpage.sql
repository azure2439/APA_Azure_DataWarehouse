CREATE TABLE [tmp].[dj_pages_landingpage] (
    [content_ptr_id]      INT          NOT NULL,
    [search_query]        TEXT         NULL,
    [sort_field]          VARCHAR (50) NOT NULL,
    [search_max]          INT          NOT NULL,
    [show_search_results] BIT          NOT NULL,
    CONSTRAINT [pages_landingpage_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

