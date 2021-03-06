﻿CREATE TABLE [tmp].[dj_learn_learncourseinfo] (
    [id]                   INT            NOT NULL,
    [run_time]             VARCHAR (20)   NULL,
    [run_time_cm]          NUMERIC (6, 2) NULL,
    [vimeo_id]             INT            NULL,
    [lms_course_id]        VARCHAR (200)  NULL,
    [lms_template]         INT            NULL,
    [lms_product_page_url] VARCHAR (255)  NULL,
    [activity_from_id]     INT            NULL,
    [course_to_id]         INT            NULL,
    [updated_time]         DATETIME       NULL,
    [created_time]         DATETIME       NULL,
    CONSTRAINT [learn_learncourseinfo_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

