﻿CREATE TABLE [dbo].[Order_Meet_2020Q1] (
    [ORDER_NUMBER]            NUMERIC (15, 2) CONSTRAINT [DF_Order_Meet_2020Q1_ORDER_NUMBER] DEFAULT ((0)) NOT NULL,
    [MEETING]                 VARCHAR (10)    CONSTRAINT [DF_Order_Meet_2020Q1_MEETING] DEFAULT ('') NOT NULL,
    [REGISTRANT_CLASS]        VARCHAR (5)     CONSTRAINT [DF_Order_Meet_2020Q1_REGISTRANT_CLASS] DEFAULT ('') NOT NULL,
    [ARRIVAL]                 DATETIME        NULL,
    [DEPARTURE]               DATETIME        NULL,
    [HOTEL]                   VARCHAR (40)    CONSTRAINT [DF_Order_Meet_2020Q1_HOTEL] DEFAULT ('') NOT NULL,
    [LODGING_INSTRUCTIONS]    VARCHAR (255)   CONSTRAINT [DF_Order_Meet_2020Q1_LODGING_INSTRUCTIONS] DEFAULT ('') NOT NULL,
    [BOOTH]                   VARCHAR (255)   CONSTRAINT [DF_Order_Meet_2020Q1_BOOTH] DEFAULT ('') NOT NULL,
    [GUEST_FIRST]             VARCHAR (20)    CONSTRAINT [DF_Order_Meet_2020Q1_GUEST_FIRST] DEFAULT ('') NOT NULL,
    [GUEST_MIDDLE]            VARCHAR (20)    CONSTRAINT [DF_Order_Meet_2020Q1_GUEST_MIDDLE] DEFAULT ('') NOT NULL,
    [GUEST_LAST]              VARCHAR (30)    CONSTRAINT [DF_Order_Meet_2020Q1_GUEST_LAST] DEFAULT ('') NOT NULL,
    [GUEST_IS_SPOUSE]         BIT             CONSTRAINT [DF_Order_Meet_2020Q1_GUEST_IS_SPOUSE] DEFAULT ((0)) NOT NULL,
    [ADDITIONAL_BADGES]       TEXT            NULL,
    [DELEGATE]                VARCHAR (10)    CONSTRAINT [DF_Order_Meet_2020Q1_DELEGATE] DEFAULT ('') NOT NULL,
    [UF_1]                    BIT             CONSTRAINT [DF_Order_Meet_2020Q1_UF_1] DEFAULT ((0)) NOT NULL,
    [UF_2]                    BIT             CONSTRAINT [DF_Order_Meet_2020Q1_UF_2] DEFAULT ((0)) NOT NULL,
    [UF_3]                    BIT             CONSTRAINT [DF_Order_Meet_2020Q1_UF_3] DEFAULT ((0)) NOT NULL,
    [UF_4]                    BIT             CONSTRAINT [DF_Order_Meet_2020Q1_UF_4] DEFAULT ((0)) NOT NULL,
    [UF_5]                    BIT             CONSTRAINT [DF_Order_Meet_2020Q1_UF_5] DEFAULT ((0)) NOT NULL,
    [UF_6]                    VARCHAR (100)   CONSTRAINT [DF_Order_Meet_2020Q1_UF_6] DEFAULT ('') NOT NULL,
    [UF_7]                    VARCHAR (100)   CONSTRAINT [DF_Order_Meet_2020Q1_UF_7] DEFAULT ('') NOT NULL,
    [UF_8]                    VARCHAR (100)   CONSTRAINT [DF_Order_Meet_2020Q1_UF_8] DEFAULT ('') NOT NULL,
    [SHARE_STATUS]            INT             CONSTRAINT [DF_Order_Meet_2020Q1_SHARE_STATUS] DEFAULT ((0)) NOT NULL,
    [SHARE_ORDER_NUMBER]      NUMERIC (15, 2) CONSTRAINT [DF_Order_Meet_2020Q1_SHARE_ORDER_NUMBER] DEFAULT ((0)) NOT NULL,
    [ROOM_TYPE]               VARCHAR (8)     CONSTRAINT [DF_Order_Meet_2020Q1_ROOM_TYPE] DEFAULT ('') NOT NULL,
    [ROOM_QUANTITY]           INT             CONSTRAINT [DF_Order_Meet_2020Q1_ROOM_QUANTITY] DEFAULT ((0)) NOT NULL,
    [ROOM_CONFIRM]            BIT             CONSTRAINT [DF_Order_Meet_2020Q1_ROOM_CONFIRM] DEFAULT ((0)) NOT NULL,
    [UF_9]                    TEXT            NULL,
    [UF_10]                   TEXT            NULL,
    [ARRIVAL_TIME]            DATETIME        NULL,
    [DEPARTURE_TIME]          DATETIME        NULL,
    [COMP_REGISTRATIONS]      INT             CONSTRAINT [DF_Order_Meet_2020Q1_COMP_REGISTRATIONS] DEFAULT ((0)) NOT NULL,
    [COMP_REG_SOURCE]         DECIMAL (15, 6) NULL,
    [TOTAL_SQUARE_FEET]       NUMERIC (15, 2) CONSTRAINT [DF_Order_Meet_2020Q1_TOTAL_SQUARE_FEET] DEFAULT ((0)) NOT NULL,
    [COMP_REGISTRATIONS_USED] INT             CONSTRAINT [DF_Order_Meet_2020Q1_COMP_REGISTRATIONS_USED] DEFAULT ((0)) NOT NULL,
    [PARENT_ORDER_NUMBER]     NUMERIC (15, 2) NULL,
    [REGISTERED_BY_ID]        VARCHAR (10)    NULL,
    [TIME_STAMP]              ROWVERSION      NULL,
    CONSTRAINT [PK_Order_Meet_2020Q1] PRIMARY KEY NONCLUSTERED ([ORDER_NUMBER] ASC)
);

