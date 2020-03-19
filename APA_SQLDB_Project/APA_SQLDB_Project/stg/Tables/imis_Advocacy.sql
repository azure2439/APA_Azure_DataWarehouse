CREATE TABLE [stg].[imis_Advocacy] (
    [ID]                     VARCHAR (10)  DEFAULT ('') NOT NULL,
    [Congressional_District] VARCHAR (5)   DEFAULT ('') NOT NULL,
    [St_House]               VARCHAR (5)   DEFAULT ('') NOT NULL,
    [St_Senate]              VARCHAR (5)   DEFAULT ('') NOT NULL,
    [State_Chair]            BIT           DEFAULT ((0)) NOT NULL,
    [District_Captain]       BIT           DEFAULT ((0)) NOT NULL,
    [Transportation]         BIT           DEFAULT ((0)) NOT NULL,
    [Community_Development]  BIT           DEFAULT ((0)) NOT NULL,
    [Federal_Data]           BIT           DEFAULT ((0)) NOT NULL,
    [Water]                  BIT           DEFAULT ((0)) NOT NULL,
    [Other]                  VARCHAR (255) DEFAULT ('') NOT NULL,
    [GrassRootsMember]       BIT           DEFAULT ((0)) NOT NULL,
    [Join_Date]              DATETIME      NULL,
    [TIME_STAMP]             BIGINT        NULL,
    [Id_Identitycolumn]      INT           IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]          VARCHAR (40)  CONSTRAINT [df_advocacyLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]           DATETIME      CONSTRAINT [df_advocacyLastModified] DEFAULT (getdate()) NULL,
    [IsActive]               BIT           NULL,
    [StartDate]              DATETIME      CONSTRAINT [df_advocacyStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]                DATETIME      CONSTRAINT [df_advocacyEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_Advocacy] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

