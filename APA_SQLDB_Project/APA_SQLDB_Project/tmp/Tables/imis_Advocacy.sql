CREATE TABLE [tmp].[imis_Advocacy] (
    [ID]                     VARCHAR (10)  NOT NULL,
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
    [TIME_STAMP]             ROWVERSION    NULL,
    CONSTRAINT [PK_Advocacy] PRIMARY KEY CLUSTERED ([ID] ASC)
);

