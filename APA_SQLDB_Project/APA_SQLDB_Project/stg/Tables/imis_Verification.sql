CREATE TABLE [stg].[imis_Verification] (
    [ID]                 VARCHAR (10) CONSTRAINT [DF__Verification__ID__57CCC155] DEFAULT ('') NOT NULL,
    [VERI_TYPE]          VARCHAR (20) CONSTRAINT [DF__Verificat__VERI___58C0E58E] DEFAULT ('') NOT NULL,
    [VERI_DATE]          DATETIME     NULL,
    [SCHOOL]             VARCHAR (60) CONSTRAINT [DF__Verificat__SCHOO__59B509C7] DEFAULT ('') NOT NULL,
    [SCHOOLS_NOT_LISTED] VARCHAR (60) CONSTRAINT [DF__Verificat__SCHOO__5AA92E00] DEFAULT ('') NOT NULL,
    [GRAD_DATE]          DATETIME     NULL,
    [STUDENT_ID]         VARCHAR (25) CONSTRAINT [DF__Verificat__STUDE__5B9D5239] DEFAULT ('') NOT NULL,
    [MUNICIPALITY]       VARCHAR (60) CONSTRAINT [DF__Verificat__MUNIC__5C917672] DEFAULT ('') NOT NULL,
    [BOARD]              VARCHAR (60) CONSTRAINT [DF__Verificat__BOARD__5D859AAB] DEFAULT ('') NOT NULL,
    [EARNING]            BIT          CONSTRAINT [DF__Verificat__EARNI__5E79BEE4] DEFAULT ((0)) NOT NULL,
    [TIME_STAMP]         BIGINT       NULL,
    [Id_Identitycolumn]  INT          IDENTITY (1, 1) NOT NULL,
    [LastUpdatedBy]      VARCHAR (40) CONSTRAINT [df_verLastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]       DATETIME     CONSTRAINT [df_verLastModified] DEFAULT (getdate()) NULL,
    [IsActive]           BIT          NULL,
    [StartDate]          DATETIME     CONSTRAINT [df_verStartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]            DATETIME     CONSTRAINT [df_verEndtDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    CONSTRAINT [pkVerificationID] PRIMARY KEY CLUSTERED ([Id_Identitycolumn] ASC)
);

