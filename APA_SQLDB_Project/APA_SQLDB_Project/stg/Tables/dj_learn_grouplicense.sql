CREATE TABLE [stg].[dj_learn_grouplicense] (
    [id]                    INT           NOT NULL,
    [license_code]          VARCHAR (100) NULL,
    [redemption_date]       DATETIME      NULL,
    [purchase_id]           INT           NOT NULL,
    [redemption_contact_id] INT           NULL,
    [updated_time]          DATETIME      NULL,
    [created_time]          DATETIME      NULL,
    [LastUpdatedBy]         VARCHAR (40)  CONSTRAINT [df_group_license_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]          DATETIME      CONSTRAINT [df_group_license_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [learn_grouplicense_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

