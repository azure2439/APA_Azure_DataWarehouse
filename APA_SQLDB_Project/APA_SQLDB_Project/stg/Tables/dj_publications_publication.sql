CREATE TABLE [stg].[dj_publications_publication] (
    [content_ptr_id]       INT           NOT NULL,
    [page_count]           INT           NULL,
    [table_of_contents]    TEXT          NULL,
    [publication_format]   VARCHAR (50)  NOT NULL,
    [publication_download] VARCHAR (100) NULL,
    [edition]              VARCHAR (100) NULL,
    [LastUpdatedBy]        VARCHAR (40)  CONSTRAINT [df_publications_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]         DATETIME      CONSTRAINT [df_publications_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [publications_publication_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

