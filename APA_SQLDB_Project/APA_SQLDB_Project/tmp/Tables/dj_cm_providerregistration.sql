CREATE TABLE [tmp].[dj_cm_providerregistration] (
    [id]                                  INT          NOT NULL,
    [registration_type]                   VARCHAR (50) NOT NULL,
    [year]                                INT          NOT NULL,
    [provider_id]                         INT          NOT NULL,
    [purchase_id]                         INT          NULL,
    [shared_from_partner_registration_id] INT          NULL,
    [status]                              VARCHAR (50) NOT NULL,
    [is_unlimited]                        BIT          NOT NULL,
    [updated_time]                        DATETIME     NULL,
    [created_time]                        DATETIME     NULL,
    CONSTRAINT [cm_providerregistration_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

