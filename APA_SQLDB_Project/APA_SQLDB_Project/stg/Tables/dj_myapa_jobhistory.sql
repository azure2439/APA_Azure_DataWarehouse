CREATE TABLE [stg].[dj_myapa_jobhistory] (
    [id]            INT           NOT NULL,
    [title]         VARCHAR (200) NULL,
    [company]       VARCHAR (80)  NULL,
    [city]          VARCHAR (100) NULL,
    [state]         VARCHAR (100) NULL,
    [zip_code]      VARCHAR (10)  NULL,
    [country]       VARCHAR (100) NULL,
    [start_date]    DATE          NULL,
    [end_date]      DATE          NULL,
    [is_current]    BIT           NOT NULL,
    [is_part_time]  BIT           NOT NULL,
    [contact_id]    INT           NOT NULL,
    [phone]         VARCHAR (20)  NULL,
    [updated_time]  DATETIME      NULL,
    [created_time]  DATETIME      NULL,
    [LastUpdatedBy] VARCHAR (40)  CONSTRAINT [df_myapa_jobhistory_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME      CONSTRAINT [df_myapa_jobhistory_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [myapa_jobhistory_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

