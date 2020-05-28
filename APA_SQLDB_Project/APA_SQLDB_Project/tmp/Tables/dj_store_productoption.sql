CREATE TABLE [tmp].[dj_store_productoption] (
    [id]              INT           NOT NULL,
    [code]            VARCHAR (200) NULL,
    [title]           VARCHAR (200) NULL,
    [status]          VARCHAR (5)   NULL,
    [description]     TEXT          NULL,
    [slug]            VARCHAR (50)  NULL,
    [created_time]    DATETIME      NULL,
    [updated_time]    DATETIME      NULL,
    [created_by_id]   INT           NULL,
    [product_id]      INT           NULL,
    [updated_by_id]   INT           NULL,
    [sort_number]     INT           NULL,
    [publish_status]  VARCHAR (50)  NULL,
    [publish_time]    DATETIME      NULL,
    [published_by_id] INT           NULL,
    [published_time]  DATETIME      NULL,
    [publish_uuid]    VARCHAR (36)  NULL,
    [LastUpdatedBy]   VARCHAR (40)  NULL,
    [LastModified]    DATETIME      NULL,
    CONSTRAINT [store_productoption_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

