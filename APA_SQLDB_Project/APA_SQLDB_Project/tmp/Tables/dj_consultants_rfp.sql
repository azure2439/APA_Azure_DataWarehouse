CREATE TABLE [tmp].[dj_consultants_rfp] (
    [content_ptr_id]       INT             NOT NULL,
    [user_address_num]     INT             NULL,
    [address1]             VARCHAR (40)    NULL,
    [address2]             VARCHAR (40)    NULL,
    [city]                 VARCHAR (40)    NULL,
    [state]                VARCHAR (100)   NULL,
    [zip_code]             VARCHAR (10)    NULL,
    [country]              VARCHAR (100)   NULL,
    [rfp_type]             VARCHAR (50)    NOT NULL,
    [deadline]             DATE            NULL,
    [email]                VARCHAR (100)   NULL,
    [website]              VARCHAR (255)   NULL,
    [company]              VARCHAR (80)    NULL,
    [latitude]             NUMERIC (10, 6) NULL,
    [longitude]            NUMERIC (10, 6) NULL,
    [voter_voice_checksum] VARCHAR (100)   NULL,
    [zip_code_extension]   VARCHAR (4)     NULL,
    CONSTRAINT [consultants_rfp_pkey] PRIMARY KEY CLUSTERED ([content_ptr_id] ASC)
);

