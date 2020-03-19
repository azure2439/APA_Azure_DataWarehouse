CREATE TABLE [stg].[imis_ORG_Demographics] (
    [ID]                  VARCHAR (10) CONSTRAINT [DF__ORG_Demograp__ID__1F3E4F6F] DEFAULT ('') NOT NULL,
    [PAS_CODE]            VARCHAR (15) CONSTRAINT [DF__ORG_Demog__PAS_C__203273A8] DEFAULT ('') NOT NULL,
    [ORG_TYPE]            VARCHAR (60) CONSTRAINT [DF__ORG_Demog__ORG_T__212697E1] DEFAULT ('') NOT NULL,
    [POPULATION]          VARCHAR (10) CONSTRAINT [DF__ORG_Demog__POPUL__221ABC1A] DEFAULT ('') NOT NULL,
    [ANNUAL_BUDGET]       VARCHAR (10) CONSTRAINT [DF__ORG_Demog__ANNUA__230EE053] DEFAULT ('') NOT NULL,
    [STAFF_SIZE]          VARCHAR (10) CONSTRAINT [DF__ORG_Demog__STAFF__2403048C] DEFAULT ('') NOT NULL,
    [PARENT_ID]           VARCHAR (9)  CONSTRAINT [DF__ORG_Demog__PAREN__24F728C5] DEFAULT ('') NOT NULL,
    [TOP_CITY]            BIT          CONSTRAINT [DF__ORG_Demog__TOP_C__25EB4CFE] DEFAULT ((0)) NOT NULL,
    [TOP_COUNTY]          BIT          CONSTRAINT [DF__ORG_Demog__TOP_C__26DF7137] DEFAULT ((0)) NOT NULL,
    [PLANNING_FUNCTION]   BIT          CONSTRAINT [DF__ORG_Demog__PLANN__27D39570] DEFAULT ((0)) NOT NULL,
    [SCHOOL_PROGRAM_TYPE] VARCHAR (10) CONSTRAINT [DF__ORG_Demog__SCHOO__28C7B9A9] DEFAULT ('') NOT NULL,
    [TIME_STAMP]          BIGINT       NULL,
    [Id_Identitycolumn]   INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]       VARCHAR (40) CONSTRAINT [df_orgdemoLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]        DATETIME     CONSTRAINT [df_orgdemoLastModified] DEFAULT (getdate()) NULL,
    [IsActive]            BIT          NULL,
    [StartDate]           DATETIME     CONSTRAINT [df_orgdemoStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]             DATETIME     CONSTRAINT [df_orgdemoEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [pkORG_DemographicsID] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

