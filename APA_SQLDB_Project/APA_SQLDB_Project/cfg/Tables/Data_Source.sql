CREATE TABLE [cfg].[Data_Source] (
    [Source_Type]         VARCHAR (30)   NULL,
    [Source]              VARCHAR (60)   NOT NULL,
    [SourceSystemName]    VARCHAR (60)   NULL,
    [SourceExtension]     VARCHAR (20)   NULL,
    [LastModified]        VARCHAR (30)   NULL,
    [Schema]              VARCHAR (4000) NULL,
    [Target]              VARCHAR (60)   NULL,
    [Active]              INT            NULL,
    [TIME_STAMP]          DATETIME       CONSTRAINT [DF_Data_Source_TIME_STAMP] DEFAULT (getdate()) NULL,
    [UpdatedBy]           VARCHAR (40)   CONSTRAINT [DF_Data_Source_UpdatedBy] DEFAULT (suser_sname()) NULL,
    [ID]                  INT            IDENTITY (1, 1) NOT NULL,
    [Target_Raw]          VARCHAR (80)   NULL,
    [RefreshType]         VARCHAR (20)   NULL,
    [RefreshFrequency]    VARCHAR (30)   NULL,
    [Max_SourceTimestamp] VARCHAR (60)   NULL,
    CONSTRAINT [PK_Data_Source] PRIMARY KEY CLUSTERED ([Source] ASC)
);

