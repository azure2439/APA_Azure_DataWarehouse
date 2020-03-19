CREATE TABLE [tmp].[FactMember_Final_Current] (
    [Member_ID]        VARCHAR (10) NULL,
    [Member_Type]      VARCHAR (5)  NULL,
    [SalaryRange]      VARCHAR (5)  NULL,
    [RaceID]           VARCHAR (60) NULL,
    [Origin]           VARCHAR (60) NULL,
    [Chapter]          VARCHAR (15) NULL,
    [TransDate]        DATETIME     NULL,
    [Paid_Thru_Date]   DATETIME     NULL,
    [Home_Address_Num] INT          NULL,
    [Work_Address_Num] INT          NULL,
    [Status]           VARCHAR (5)  NULL,
    [Gender]           VARCHAR (1)  NULL,
    [Category]         VARCHAR (5)  NULL,
    [Org_Type]         VARCHAR (60) NULL,
    [Member_Status]    VARCHAR (10) NULL,
    [Product_Code]     VARCHAR (31) NULL,
    [Prod_Type]        VARCHAR (10) NULL,
    [Source_Code]      VARCHAR (40) NULL,
    [IsCurrent]        SMALLINT     NULL,
    [Complimentary]    BIT          NULL
);

