CREATE TABLE [tmp].[dj_learn_grouplicense] (
    [id]                    INT           NOT NULL,
    [license_code]          VARCHAR (100) NULL,
    [redemption_date]       DATETIME      NULL,
    [purchase_id]           INT           NOT NULL,
    [redemption_contact_id] INT           NULL,
    [updated_time]          DATETIME      NULL,
    [created_time]          DATETIME      NULL,
    CONSTRAINT [learn_grouplicense_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

