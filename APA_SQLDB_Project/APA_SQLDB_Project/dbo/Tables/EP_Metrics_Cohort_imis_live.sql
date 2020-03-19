CREATE TABLE [dbo].[EP_Metrics_Cohort_imis_live] (
    [ID]                 VARCHAR (10) DEFAULT ('') NOT NULL,
    [COHORT]             INT          DEFAULT ((0)) NOT NULL,
    [COHORT_MEMBER_TYPE] VARCHAR (10) DEFAULT ('') NOT NULL,
    [COHORT_QUARTER]     INT          DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]         ROWVERSION   NULL,
    CONSTRAINT [PK_EP_Metrics_Cohort_imis_live] PRIMARY KEY CLUSTERED ([ID] ASC)
);

