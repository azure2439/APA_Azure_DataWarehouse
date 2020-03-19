CREATE TABLE [tmp].[imis_Demo_Idonate] (
    [ID]             VARCHAR (10) NOT NULL,
    [SEQN]           INT          DEFAULT ((0)) NOT NULL,
    [QuestionCode]   VARCHAR (30) DEFAULT ('') NOT NULL,
    [SelectedAnswer] VARCHAR (60) DEFAULT ('') NOT NULL,
    [InvoiceNumber]  INT          DEFAULT ((0)) NOT NULL,
    [Create_Date]    DATETIME     NULL,
    [TIME_STAMP]     ROWVERSION   NULL,
    CONSTRAINT [PK_Demo_Idonate] PRIMARY KEY CLUSTERED ([ID] ASC, [SEQN] ASC)
);

