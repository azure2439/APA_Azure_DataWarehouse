CREATE TABLE [stg].[dj_exam_examregistration] (
    [id]                  INT           NOT NULL,
    [ada_requirement]     VARCHAR (50)  NOT NULL,
    [verified]            BIT           NOT NULL,
    [code_of_ethics]      BIT           NOT NULL,
    [release_information] BIT           NOT NULL,
    [certificate_name]    VARCHAR (100) NOT NULL,
    [gee_eligibility_id]  VARCHAR (50)  NULL,
    [legacy_id]           INT           NULL,
    [exam_id]             INT           NOT NULL,
    [application_id]      INT           NULL,
    [registration_type]   VARCHAR (20)  NOT NULL,
    [purchase_id]         INT           NULL,
    [contact_id]          INT           NOT NULL,
    [is_pass]             BIT           NULL,
    [updated_time]        DATETIME      NULL,
    [created_time]        DATETIME      NULL,
    [LastUpdatedBy]       VARCHAR (40)  CONSTRAINT [df_examregistration_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME      CONSTRAINT [df_examregistration_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [exam_examregistration_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

