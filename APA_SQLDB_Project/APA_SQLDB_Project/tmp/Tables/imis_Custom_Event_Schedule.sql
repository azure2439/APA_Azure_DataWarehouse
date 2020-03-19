CREATE TABLE [tmp].[imis_Custom_Event_Schedule] (
    [ID]               VARCHAR (10) NOT NULL,
    [SEQN]             INT          DEFAULT ((0)) NOT NULL,
    [MEETING]          VARCHAR (10) DEFAULT ('') NOT NULL,
    [REGISTRANT_CLASS] VARCHAR (10) DEFAULT ('') NOT NULL,
    [PRODUCT_CODE]     VARCHAR (31) DEFAULT ('') NOT NULL,
    [STATUS]           VARCHAR (5)  DEFAULT ('') NOT NULL,
    [UNIT_PRICE]       MONEY        DEFAULT ((0)) NOT NULL,
    [IS_WAITLIST]      BIT          DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]       ROWVERSION   NULL,
    CONSTRAINT [PK_Custom_Event_Schedule] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

