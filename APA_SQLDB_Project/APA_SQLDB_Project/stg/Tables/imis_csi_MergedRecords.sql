CREATE TABLE [stg].[imis_csi_MergedRecords] (
    [DuplicateID]       VARCHAR (10) NOT NULL,
    [MergeToID]         VARCHAR (10) NOT NULL,
    [UserName]          VARCHAR (30) NOT NULL,
    [DateOfMerge]       DATETIME     DEFAULT (getdate()) NULL,
    [Id_Identitycolumn] INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]     VARCHAR (40) CONSTRAINT [df_csiMerged_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]      DATETIME     CONSTRAINT [df_csiMerged_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]          BIT          NULL,
    [StartDate]         DATETIME     CONSTRAINT [df_csiMergedStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]           DATETIME     CONSTRAINT [df_csiMergedEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [PK_imis_csi_MergedRecords] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

