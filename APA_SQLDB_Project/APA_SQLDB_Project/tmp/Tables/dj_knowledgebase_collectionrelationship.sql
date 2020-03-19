CREATE TABLE [tmp].[dj_knowledgebase_collectionrelationship] (
    [contentrelationship_ptr_id] INT      NOT NULL,
    [updated_time]               DATETIME NULL,
    CONSTRAINT [knowledgebase_collectionrelationship_pkey] PRIMARY KEY CLUSTERED ([contentrelationship_ptr_id] ASC)
);

