﻿CREATE TABLE [tmp].[imis_PostalCodeRef] (
    [PostalCode]        NVARCHAR (20)    NOT NULL,
    [CountryCode]       NCHAR (2)        NOT NULL,
    [City]              NVARCHAR (50)    NULL,
    [StateProvinceCode] NVARCHAR (5)     NULL,
    [Region]            NVARCHAR (50)    NULL,
    [County]            NVARCHAR (50)    NULL,
    [CountyFIPS]        NVARCHAR (5)     NULL,
    [IsHandModified]    BIT              NOT NULL,
    [IsHandEntered]     BIT              NOT NULL,
    [ChapterGroupKey]   UNIQUEIDENTIFIER NULL,
    [UpdatedOn]         DATETIME         NOT NULL,
    [UpdatedByUserKey]  UNIQUEIDENTIFIER NOT NULL,
    [IsActive]          BIT              NOT NULL,
    [PostalCodeKey]     UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT [PK_PostalCodeRef] PRIMARY KEY CLUSTERED ([PostalCodeKey] ASC)
);

