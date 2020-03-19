CREATE TABLE [tmp].[imis_csi_MergedRecords] (
    [DuplicateID] VARCHAR (10) NOT NULL,
    [MergeToID]   VARCHAR (10) NOT NULL,
    [UserName]    VARCHAR (30) NOT NULL,
    [DateOfMerge] DATETIME     DEFAULT (getdate()) NULL
);

