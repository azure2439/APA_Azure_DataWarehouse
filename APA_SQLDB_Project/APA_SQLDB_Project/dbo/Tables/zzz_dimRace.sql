CREATE TABLE [dbo].[zzz_dimRace] (
    [RaceIDKey]         INT          IDENTITY (1, 1) NOT NULL,
    [RaceID]            VARCHAR (20) NULL,
    [Ethnicity]         VARCHAR (60) NULL,
    [Custom_Rollup]     VARCHAR (60) NULL,
    [LastUpdatedBy]     VARCHAR (40) NULL,
    [LastModified]      DATETIME     NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     NULL,
    [EndDate]           DATETIME     NULL,
    [Origin]            VARCHAR (20) NULL,
    [OriginDescription] VARCHAR (80) NULL
);

