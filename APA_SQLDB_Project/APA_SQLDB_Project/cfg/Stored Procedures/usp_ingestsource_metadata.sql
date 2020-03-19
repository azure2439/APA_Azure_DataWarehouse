

CREATE procedure [cfg].[usp_ingestsource_metadata] (@sourcetype varchar(60), @sourcesystem varchar(30), @refreshfrequency varchar(30))
as
(
select Source, Target, LastModified, Target_Raw from cfg.Data_Source
where source_type = @sourcetype
and SourceSystemName = @sourcesystem
and refreshfrequency = @refreshfrequency
and Active  = 1
)


