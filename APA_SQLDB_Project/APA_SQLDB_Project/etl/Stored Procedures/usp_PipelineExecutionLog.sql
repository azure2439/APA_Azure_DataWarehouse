


Create PROCEDURE [etl].[usp_PipelineExecutionLog] 
@ActivityType varchar(50),
@ActivityDescr varchar(200),
@SourceSystem varchar(50),
@IsSuccess bit,
@PipelineName varchar(100),
@RunID varchar(100),
@Logdetails varchar(max)

as

/*******************************************************************************************************************************
 Description:   This Stored Procedure is to capture Pipeline Execution status's and insert into the PipelineExecutionLog table
				for query and reference. 
				

Added By:		Ajay Punyapu on 2/27/2020				
*******************************************************************************************************************************/


Insert into etl.PipelineExecutionLog (ActivityType, ActivityDescr, SourceSystem, IsSuccess, PipeLineName, RunID, LogDetails)
select @ActivityType, @ActivityDescr, @SourceSystem, @IsSuccess, @PipelineName, @RunID, @Logdetails
