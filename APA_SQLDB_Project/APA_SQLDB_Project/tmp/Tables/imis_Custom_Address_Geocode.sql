CREATE TABLE [tmp].[imis_Custom_Address_Geocode] (
    [ID]                  VARCHAR (10)  DEFAULT ('') NOT NULL,
    [SEQN]                INT           DEFAULT ((0)) NOT NULL,
    [ADDRESS_NUM]         INT           DEFAULT ((0)) NOT NULL,
    [VOTERVOICE_CHECKSUM] VARCHAR (255) DEFAULT ('') NOT NULL,
    [LONGITUDE]           FLOAT (53)    DEFAULT ((0)) NOT NULL,
    [LATITUDE]            FLOAT (53)    DEFAULT ((0)) NOT NULL,
    [WEAK_COORDINATES]    INT           DEFAULT ((0)) NOT NULL,
    [US_CONGRESS]         VARCHAR (100) DEFAULT ('') NOT NULL,
    [STATE_SENATE]        VARCHAR (100) DEFAULT ('') NOT NULL,
    [STATE_HOUSE]         VARCHAR (100) DEFAULT ('') NOT NULL,
    [CHANGED]             BIT           DEFAULT ((0)) NOT NULL,
    [LAST_UPDATED]        DATETIME      NULL,
    [TIME_STAMP]          ROWVERSION    NULL,
    CONSTRAINT [PK_Custom_Address_Geocode] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

