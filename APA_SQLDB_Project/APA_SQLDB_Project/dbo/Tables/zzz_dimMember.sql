﻿CREATE TABLE [dbo].[zzz_dimMember] (
    [MemberIDKey]      INT           IDENTITY (1, 1) NOT NULL,
    [Parent]           VARCHAR (10)  NULL,
    [Member_ID]        VARCHAR (10)  NULL,
    [First_Name]       VARCHAR (70)  NULL,
    [Middle_Name]      VARCHAR (70)  NULL,
    [Last_Name]        VARCHAR (70)  NULL,
    [Full_Name]        VARCHAR (70)  NULL,
    [Email]            VARCHAR (100) NULL,
    [Home_Phone]       VARCHAR (25)  NULL,
    [Mobile_Phone]     VARCHAR (25)  NULL,
    [Work_Phone]       VARCHAR (25)  NULL,
    [Designation]      VARCHAR (20)  NULL,
    [Functional_Title] VARCHAR (20)  NULL,
    [Birth_Date]       DATETIME      NULL,
    [JoinDate]         DATETIME      NULL,
    [Cohort]           VARCHAR (20)  NULL,
    [LastUpdatedBy]    VARCHAR (40)  NULL,
    [LastModified]     DATETIME      NULL,
    [IsActive]         BIT           NULL,
    [StartDate]        DATETIME      NULL,
    [EndDate]          DATETIME      NULL,
    [Merged_Date]      DATETIME      NULL,
    [Cohortquarter]    VARCHAR (20)  NULL
);

