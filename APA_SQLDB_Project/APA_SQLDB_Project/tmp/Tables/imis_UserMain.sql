CREATE TABLE [tmp].[imis_UserMain] (
    [UserKey]                   UNIQUEIDENTIFIER CONSTRAINT [DF_UserMain_UserKey] DEFAULT (newid()) NOT NULL,
    [ContactMaster]             VARCHAR (100)    NULL,
    [UserId]                    VARCHAR (60)     CONSTRAINT [DF_UserMain_UserId] DEFAULT ('') NOT NULL,
    [IsDisabled]                BIT              CONSTRAINT [DF_UserMain_IsDisabled] DEFAULT ((1)) NOT NULL,
    [EffectiveDate]             DATETIME         NOT NULL,
    [ExpirationDate]            DATETIME         NULL,
    [UpdatedByUserKey]          UNIQUEIDENTIFIER NOT NULL,
    [UpdatedOn]                 DATETIME         NOT NULL,
    [CreatedByUserKey]          UNIQUEIDENTIFIER NOT NULL,
    [CreatedOn]                 DATETIME         NOT NULL,
    [MarkedForDeleteOn]         DATETIME         NULL,
    [DefaultDepartmentGroupKey] UNIQUEIDENTIFIER NOT NULL,
    [DefaultPerspectiveKey]     UNIQUEIDENTIFIER NOT NULL,
    [ProviderKey]               NVARCHAR (100)   NULL,
    [MultiFactorInfo]           NVARCHAR (MAX)   NULL,
    CONSTRAINT [PK_UserMain] PRIMARY KEY CLUSTERED ([UserKey] ASC),
    CONSTRAINT [FK_UserMain_UserMain_CreatedBy] FOREIGN KEY ([CreatedByUserKey]) REFERENCES [tmp].[imis_UserMain] ([UserKey]),
    CONSTRAINT [FK_UserMain_UserMain_UpdatedBy] FOREIGN KEY ([UpdatedByUserKey]) REFERENCES [tmp].[imis_UserMain] ([UserKey])
);

