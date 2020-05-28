CREATE TABLE [stg].[dj_knowledgebase_collectionrelationship] (
    [contentrelationship_ptr_id] INT          NOT NULL,
    [updated_time]               DATETIME     NULL,
    [LastUpdatedBy]              VARCHAR (40) CONSTRAINT [df_knowledgebase_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]               DATETIME     CONSTRAINT [df_knowledgebase_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [knowledgebase_collectionrelationship_pkey] PRIMARY KEY CLUSTERED ([contentrelationship_ptr_id] ASC)
);

