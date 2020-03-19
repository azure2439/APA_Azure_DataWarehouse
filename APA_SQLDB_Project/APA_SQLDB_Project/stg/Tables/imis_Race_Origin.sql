CREATE TABLE [stg].[imis_Race_Origin] (
    [ID]                   VARCHAR (10) CONSTRAINT [DF__Race_Origin___ID__41936773] DEFAULT ('') NOT NULL,
    [RACE]                 VARCHAR (60) CONSTRAINT [DF__Race_Origi__RACE__42878BAC] DEFAULT ('') NOT NULL,
    [ORIGIN]               VARCHAR (60) CONSTRAINT [DF__Race_Orig__ORIGI__437BAFE5] DEFAULT ('') NOT NULL,
    [SPAN_HISP_LATINO]     VARCHAR (60) CONSTRAINT [DF__Race_Orig__SPAN___446FD41E] DEFAULT ('') NOT NULL,
    [AI_AN]                VARCHAR (60) CONSTRAINT [DF__Race_Orig__AI_AN__4563F857] DEFAULT ('') NOT NULL,
    [ASIAN_PACIFIC]        VARCHAR (60) CONSTRAINT [DF__Race_Orig__ASIAN__46581C90] DEFAULT ('') NOT NULL,
    [OTHER]                VARCHAR (60) CONSTRAINT [DF__Race_Orig__OTHER__474C40C9] DEFAULT ('') NOT NULL,
    [ETHNICITY_VERIFYDATE] DATETIME     NULL,
    [ORIGIN_VERIFYDATE]    DATETIME     NULL,
    [ETHNICITY_NOANSWER]   BIT          CONSTRAINT [DF__Race_Orig__ETHNI__48406502] DEFAULT ((0)) NOT NULL,
    [ORIGIN_NOANSWER]      BIT          CONSTRAINT [DF__Race_Orig__ORIGI__4934893B] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]           BIGINT       NULL,
    [Id_Identitycolumn]    INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]        VARCHAR (40) CONSTRAINT [df_raceLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]         DATETIME     CONSTRAINT [df_raceLastModified] DEFAULT (getdate()) NULL,
    [IsActive]             BIT          NULL,
    [StartDate]            DATETIME     CONSTRAINT [df_raceStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]              DATETIME     CONSTRAINT [df_raceEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [pkRace_OriginID] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

