CREATE TABLE [tmp].[imis_EP_Metrics_Cohort] (
    [ID]                 VARCHAR (10) NOT NULL,
    [COHORT]             INT          DEFAULT ((0)) NOT NULL,
    [COHORT_MEMBER_TYPE] VARCHAR (10) DEFAULT ('') NOT NULL,
    [TIME_STAMP]         ROWVERSION   NULL,
    CONSTRAINT [PK_EP_Metrics_Cohort] PRIMARY KEY CLUSTERED ([ID] ASC)
);

