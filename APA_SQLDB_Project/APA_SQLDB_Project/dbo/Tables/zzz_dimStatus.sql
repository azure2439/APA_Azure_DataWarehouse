CREATE TABLE [dbo].[zzz_dimStatus] (
    [StatusKey]     INT          IDENTITY (1, 1) NOT NULL,
    [Status]        VARCHAR (30) NULL,
    [Gender]        VARCHAR (30) NULL,
    [Category]      VARCHAR (20) NULL,
    [LastUpdatedBy] VARCHAR (40) NULL,
    [LastModified]  DATETIME     NULL,
    [IsActive]      BIT          NULL,
    [StartDate]     DATETIME     NULL,
    [EndDate]       DATETIME     NULL
);

