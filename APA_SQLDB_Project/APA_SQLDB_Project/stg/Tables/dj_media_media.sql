CREATE TABLE [stg].[dj_media_media] (
    [content_ptr_id] INT           NOT NULL,
    [media_format]   VARCHAR (20)  NOT NULL,
    [url_source]     VARCHAR (20)  NOT NULL,
    [file_type]      VARCHAR (20)  NOT NULL,
    [resolution]     INT           NULL,
    [height]         INT           NULL,
    [width]          INT           NULL,
    [image_file]     VARCHAR (100) NULL,
    [uploaded_file]  VARCHAR (100) NULL,
    [LastUpdatedBy]  VARCHAR (40)  CONSTRAINT [df_media_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME      CONSTRAINT [df_media_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [media_media_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

