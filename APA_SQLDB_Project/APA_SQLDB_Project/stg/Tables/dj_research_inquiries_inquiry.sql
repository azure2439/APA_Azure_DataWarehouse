CREATE TABLE [stg].[dj_research_inquiries_inquiry] (
    [content_ptr_id] INT          NOT NULL,
    [response_text]  TEXT         NULL,
    [review_status]  VARCHAR (50) NOT NULL,
    [hours]          INT          NULL,
    [LastUpdatedBy]  VARCHAR (40) CONSTRAINT [df_research_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]   DATETIME     CONSTRAINT [df_research_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [research_inquiries_inquiry_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

