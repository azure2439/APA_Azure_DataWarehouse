﻿CREATE TABLE [stg].[dj_myapa_contactrole] (
    [id]                   INT             NOT NULL,
    [role_type]            VARCHAR (50)    NOT NULL,
    [sort_number]          INT             NULL,
    [confirmed]            BIT             NOT NULL,
    [invitation_sent]      BIT             NOT NULL,
    [content_rating]       INT             NULL,
    [contact_id]           INT             NULL,
    [content_id]           INT             NOT NULL,
    [permission_av]        VARCHAR (50)    NOT NULL,
    [permission_content]   VARCHAR (50)    NOT NULL,
    [special_status]       VARCHAR (50)    NULL,
    [publish_status]       VARCHAR (50)    NOT NULL,
    [publish_time]         DATETIME        NULL,
    [published_by_id]      INT             NULL,
    [published_time]       DATETIME        NULL,
    [bio]                  VARCHAR (MAX)   NULL,
    [first_name]           VARCHAR (20)    NULL,
    [last_name]            VARCHAR (30)    NULL,
    [middle_name]          VARCHAR (20)    NULL,
    [address1]             VARCHAR (40)    NULL,
    [address2]             VARCHAR (40)    NULL,
    [cell_phone]           VARCHAR (20)    NULL,
    [city]                 VARCHAR (40)    NULL,
    [company]              VARCHAR (80)    NULL,
    [country]              VARCHAR (100)   NULL,
    [email]                VARCHAR (100)   NULL,
    [phone]                VARCHAR (20)    NULL,
    [state]                VARCHAR (100)   NULL,
    [user_address_num]     INT             NULL,
    [zip_code]             VARCHAR (10)    NULL,
    [publish_uuid]         VARCHAR (36)    NULL,
    [external_bio_url]     VARCHAR (255)   NULL,
    [updated_time]         DATETIME        NULL,
    [created_time]         DATETIME        NULL,
    [voter_voice_checksum] VARCHAR (100)   NULL,
    [latitude]             NUMERIC (10, 6) NULL,
    [longitude]            NUMERIC (10, 6) NULL,
    [zip_code_extension]   VARCHAR (4)     NULL,
    [LastUpdatedBy]        VARCHAR (40)    CONSTRAINT [df_myapa_contactrole_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]         DATETIME        CONSTRAINT [df_myapa_contactrole_LastModified] DEFAULT (getdate()) NULL,
    CONSTRAINT [myapa_contactrole_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

