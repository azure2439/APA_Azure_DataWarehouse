CREATE TABLE [rpt].[tblFactClaims] (
    [ID_Claim]         INT            NOT NULL,
    [LogStatusKey]     INT            NOT NULL,
    [CMPeriodKey]      INT            NOT NULL,
    [ProductIDKey]     INT            NOT NULL,
    [FlagKey]          INT            NOT NULL,
    [MemberIDKey]      INT            NOT NULL,
    [Credits]          NUMERIC (6, 2) NULL,
    [Law_Credits]      NUMERIC (6, 2) NULL,
    [Ethics_Credits]   NUMERIC (6, 2) NULL,
    [SubmittedDateKey] INT            NOT NULL,
    [MemberTypeKey]    INT            NOT NULL,
    [ChapterKey]       INT            NOT NULL,
    [SalaryKey]        INT            NOT NULL,
    [RaceKey]          INT            NOT NULL,
    [StatusKey]        INT            NOT NULL,
    [MemberStatusKey]  INT            NOT NULL,
    [Addressnum1key]   INT            NOT NULL,
    [Addressnum2key]   INT            NOT NULL,
    [LastModified]     DATETIME       DEFAULT (getdate()) NULL,
    [LastUpdatedby]    VARCHAR (40)   DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([ID_Claim] ASC)
);

