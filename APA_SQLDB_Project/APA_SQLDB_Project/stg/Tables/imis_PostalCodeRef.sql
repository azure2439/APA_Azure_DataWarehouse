CREATE TABLE [stg].[imis_PostalCodeRef] (
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
    [Id_Identity]       INT              IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40)     CONSTRAINT [df_PostalCodeRef_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME         CONSTRAINT [df_PostalCodeRef_LastModified] DEFAULT (getdate()) NULL,
    [StartDate]         DATETIME         CONSTRAINT [df_PostalCodeRef_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME         CONSTRAINT [df_PostalCodeRef_EndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_PostalCodeRef] PRIMARY KEY CLUSTERED ([Id_Identity] ASC)
);

