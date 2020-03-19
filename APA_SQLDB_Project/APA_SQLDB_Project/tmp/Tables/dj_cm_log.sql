CREATE TABLE [tmp].[dj_cm_log] (
    [id]                      INT            NOT NULL,
    [status]                  VARCHAR (50)   NOT NULL,
    [is_current]              BIT            NOT NULL,
    [contact_id]              INT            NOT NULL,
    [period_id]               INT            NOT NULL,
    [credits_required]        NUMERIC (6, 2) NOT NULL,
    [ethics_credits_required] NUMERIC (6, 2) NOT NULL,
    [law_credits_required]    NUMERIC (6, 2) NOT NULL,
    [begin_time]              DATETIME       NULL,
    [end_time]                DATETIME       NULL,
    [reinstatement_end_time]  DATETIME       NULL,
    [updated_time]            DATETIME       NULL,
    [created_time]            DATETIME       NULL,
    CONSTRAINT [cm_log_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

