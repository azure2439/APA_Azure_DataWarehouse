CREATE TABLE [stg].[imis_PAS_Demographics] (
    [ID]                VARCHAR (10) CONSTRAINT [DF__PAS_Demograp__ID__631CE1C2] DEFAULT ('') NOT NULL,
    [PAS_TYPE]          VARCHAR (20) CONSTRAINT [DF__PAS_Demog__PAS_T__641105FB] DEFAULT ('') NOT NULL,
    [POP_CITY_COUNTY]   VARCHAR (20) CONSTRAINT [DF__PAS_Demog__POP_C__65052A34] DEFAULT ('') NOT NULL,
    [POP_REG_STATE]     VARCHAR (20) CONSTRAINT [DF__PAS_Demog__POP_R__65F94E6D] DEFAULT ('') NOT NULL,
    [TIME_STAMP]        BIGINT       NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_pasdemoLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_pasdemoLastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_pasdemoStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_pasdemoEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [pkPAS_DemographicsID] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

