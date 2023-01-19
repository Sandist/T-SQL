SELECT TOP 50
 DB_NAME(database_id),
 mid.*, 
 migs.*,
 avg_total_user_cost * avg_user_impact * (user_seeks + user_scans) as Perf
FROM sys.dm_db_missing_index_group_stats as migs
INNER JOIN sys.dm_db_missing_index_groups AS mig
 ON (migs.group_handle = mig.index_group_handle)
INNER JOIN sys.dm_db_missing_index_details AS mid
 ON (mig.index_handle = mid.index_handle)
--where statement = '[ELF].[dbo].[ОперДокумент_Позиции]'
ORDER BY avg_total_user_cost * avg_user_impact * (user_seeks + user_scans) DESC
