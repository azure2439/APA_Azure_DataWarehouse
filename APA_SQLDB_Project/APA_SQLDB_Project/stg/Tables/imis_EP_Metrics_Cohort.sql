CREATE TABLE [stg].[imis_EP_Metrics_Cohort] (
    [ID]                 VARCHAR (10) DEFAULT ('') NOT NULL,
    [COHORT]             INT          DEFAULT ((0)) NOT NULL,
    [COHORT_MEMBER_TYPE] VARCHAR (10) DEFAULT ('') NOT NULL,
    [TIME_STAMP]         BIGINT       NULL,
    [Id_Identitycolumn]  INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]      VARCHAR (40) CONSTRAINT [df_cohortLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME     CONSTRAINT [df_cohortLastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT          NULL,
    [StartDate]          DATETIME     CONSTRAINT [df_cohortStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME     CONSTRAINT [df_cohortEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_EP_Metrics_Cohort] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

