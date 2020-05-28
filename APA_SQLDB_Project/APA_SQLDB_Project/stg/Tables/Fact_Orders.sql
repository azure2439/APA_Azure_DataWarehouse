CREATE TABLE [stg].[Fact_Orders] (
    [Seqn]              INT          NOT NULL,
    [Member_ID]         VARCHAR (20) NULL,
    [Product_Code]      VARCHAR (31) NULL,
    [Unit_Price]        MONEY        NOT NULL,
    [Registrant_Class]  VARCHAR (10) NOT NULL,
    [Registrant_Status] VARCHAR (5)  NOT NULL,
    [Member_Type]       VARCHAR (5)  NULL,
    [Salary_Range]      VARCHAR (5)  NULL,
    [Race_ID]           VARCHAR (60) NULL,
    [Origin]            VARCHAR (60) NULL,
    [Primary_Chapter]   VARCHAR (15) NULL,
    [Gender]            VARCHAR (1)  NULL,
    [Status]            VARCHAR (5)  NULL,
    [Category]          VARCHAR (5)  NULL,
    [Member_Status]     VARCHAR (10) NULL,
    [Address_Num1]      INT          NULL,
    [Address_Num2]      INT          NULL,
    [Lastupdated]       DATETIME     DEFAULT (getdate()) NULL,
    [LastModifiedBy]    VARCHAR (40) DEFAULT (suser_sname()) NULL
);

