CREATE TABLE [etl].[PipelineExecutionLog] (
    [PipelineExecLogID] INT           IDENTITY (1, 1) NOT NULL,
    [ActivityType]      VARCHAR (50)  NULL,
    [ActivityDescr]     VARCHAR (200) NULL,
    [SourceSystem]      VARCHAR (50)  NULL,
    [IsSuccess]         BIT           NOT NULL,
    [PipeLineName]      VARCHAR (100) NULL,
    [RunID]             VARCHAR (100) NULL,
    [LogDetails]        VARCHAR (MAX) NULL,
    [LastModified]      DATETIME      DEFAULT (getdate()) NOT NULL,
    [LastUpdatedBy]     VARCHAR (200) DEFAULT (suser_sname()) NOT NULL,
    CONSTRAINT [PK_PipelineExecLogID] PRIMARY KEY CLUSTERED ([PipelineExecLogID] ASC)
);

