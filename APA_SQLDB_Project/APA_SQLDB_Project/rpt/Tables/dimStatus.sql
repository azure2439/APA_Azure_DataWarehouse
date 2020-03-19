CREATE TABLE [rpt].[dimStatus] (
    [StatusKey]     INT          IDENTITY (1, 1) NOT NULL,
    [Status]        VARCHAR (30) NULL,
    [Gender]        VARCHAR (30) NULL,
    [Category]      VARCHAR (20) NULL,
    [LastUpdatedBy] VARCHAR (40) CONSTRAINT [DF_dimStatus_LastUpdatedBy] DEFAULT (suser_sname()) NULL,
    [LastModified]  DATETIME     CONSTRAINT [DF_dimStatus_LastModified] DEFAULT (getdate()) NULL,
    [IsActive]      BIT          CONSTRAINT [DF_dimStatus_IsActive] DEFAULT ((1)) NULL,
    [StartDate]     DATETIME     CONSTRAINT [DF_dimStatus_StartDate] DEFAULT ('1901-01-01 00:00:00') NULL,
    [EndDate]       DATETIME     CONSTRAINT [DF_dimStatus_EndDate] DEFAULT ('2999-12-31 00:00:00') NULL,
    PRIMARY KEY CLUSTERED ([StatusKey] ASC)
);

