CREATE TABLE [stg].[dj_cm_log] (
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
    [LastUpdatedBy]           VARCHAR (40)   CONSTRAINT [df_cm_log_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]            DATETIME       CONSTRAINT [df_cm_log_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]                BIT            NULL,
    [StartDate]               DATETIME       CONSTRAINT [df_cm_log_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                 DATETIME       CONSTRAINT [df_cm_log_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [cm_log_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

