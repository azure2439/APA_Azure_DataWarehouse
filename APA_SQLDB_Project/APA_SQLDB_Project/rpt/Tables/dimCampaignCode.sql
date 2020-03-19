CREATE TABLE [rpt].[dimCampaignCode] (
    [CampaignCodeKey] INT           IDENTITY (1, 1) NOT NULL,
    [ParentID]        VARCHAR (10)  NOT NULL,
    [Campaign_Code]   VARCHAR (10)  NULL,
    [Description]     VARCHAR (255) NULL,
    [LastUpdatedBy]   VARCHAR (40)  CONSTRAINT [DF_dimCampaignCode_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]    DATETIME      CONSTRAINT [DF_dimCampaignCode_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]        BIT           CONSTRAINT [DF_dimCampaignCode_IsActive] DEFAULT ((1)) NULL,
    [StartDate]       DATETIME      CONSTRAINT [DF_dimCampaignCode_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]         DATETIME      CONSTRAINT [DF_dimCampaignCode_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([CampaignCodeKey] ASC)
);

