CREATE TABLE [tmp].[dj_myapa_organizationprofile] (
    [id]                       INT            NOT NULL,
    [principals]               VARCHAR (1000) NULL,
    [number_of_staff]          INT            NULL,
    [number_of_planners]       INT            NULL,
    [number_of_aicp_members]   INT            NULL,
    [date_founded]             DATE           NULL,
    [consultant_listing_until] DATETIME       NULL,
    [contact_id]               INT            NOT NULL,
    [image_id]                 INT            NULL,
    [research_inquiry_hours]   INT            NULL,
    [employer_bio]             TEXT           NULL,
    [updated_time]             DATETIME       NULL,
    [created_time]             DATETIME       NULL,
    CONSTRAINT [myapa_organizationprofile_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

