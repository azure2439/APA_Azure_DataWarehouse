CREATE TABLE [rpt].[dimRace] (
    [RaceIDKey]         INT          IDENTITY (1, 1) NOT NULL,
    [RaceID]            VARCHAR (20) NULL,
    [Ethnicity]         VARCHAR (60) NULL,
    [Custom_Rollup]     VARCHAR (60) NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [DF_dimRace_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [DF_dimRace_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          CONSTRAINT [DF_dimRace_IsActive] DEFAULT ((1)) NULL,
    [StartDate]         DATETIME     CONSTRAINT [DF_dimRace_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [DF_dimRace_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    [Origin]            VARCHAR (20) NULL,
    [OriginDescription] VARCHAR (80) NULL,
    PRIMARY KEY CLUSTERED ([RaceIDKey] ASC)
);

