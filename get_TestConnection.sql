ALTER proc [dbo].[get_TestConnection]
		@Mode varchar(250) = 'ping' 
	   ,@ServerName varchar(255) = NULL
	   ,@Query varchar(max) = NULL
	   
AS
	--kmm 2023-01-02 
	--Процедура проверяет соединение с сервером 3 способами
		--1 @Mode = sqltest - отправляем запрос на сервер и не ожидаем ответа более 3 секунд
		--2 @Mode = pssqltest - отправляем запрос к серверу используя powershell - то есть создаем новое соединение
		--3 @Mode = ping - пингуем сервер используя powershell
		--4 @Mode = testlinkedserver - проверяем соединение по связанному серверу используя процедуру sp_testlinkedserver
	
	--https://fixmypc.ru/post/test-connection-ili-zamena-ping-v-powershell/
BEGIN

	SET NOCOUNT ON;
	SET LOCK_TIMEOUT 3000;

	DECLARE @cmd VARCHAR(8000)

	drop table if exists #res
	create table #res (mes varchar(max))

	if @Mode = 'ping'
		SET @cmd = 'powershell Test-Connection "'+@ServerName+'" -Count 1 -quiet'

	if @Mode = 'pssqltest'
		SET @cmd = 'powershell.exe -c " try {$ServerName = '''+@ServerName+'''; $DatabaseName = ''AutoSupply2''; [System.Reflection.Assembly]::LoadWithPartialName(''Microsoft.SqlServer.Smo'') | Out-Null; $serverInstance = New-Object (''Microsoft.SqlServer.Management.Smo.Server'') $ServerName; $results = $serverInstance.Databases[$DatabaseName].ExecuteWithResults(''select GETDATE() as [GETDATE]''); echo $results.Tables[0] | Format-List;} catch {echo ''error''}"'

	if @Mode = 'sqltest'
		BEGIN TRY
			--SET @cmd = '
			--declare @s varchar(max) = ''IF EXISTS ('+@Query+') SELECT 1 ELSE SELECT 2''
			--EXEC (@s) at ['+@ServerName+']'

			SET @cmd = 'IF EXISTS (SELECT TOP 1 1
								   FROM OPENQUERY('+@ServerName+','''+@Query+''')
								   ) SELECT 1 ELSE SELECT 2'
			--print @cmd
			insert into #res (mes)
			EXEC (@cmd)
		END TRY
		BEGIN CATCH
			truncate table #res
		END CATCH

	if @Mode = 'testlinkedserver'
		BEGIN TRY
			SET @cmd = 'exec sp_testlinkedserver ' + @ServerName 
			EXEC (@cmd)
			RETURN 1
		END TRY
		BEGIN CATCH
			RETURN 0
		END CATCH


	IF @Mode <> 'sqltest'
		begin try
			--print @cmd
			insert into #res (mes)
			EXEC master.dbo.xp_cmdshell @cmd
		end try
		begin catch
		THROW;
			select 0 as result
		end catch

	IF EXISTS( select top 1 *
				from #res
				where mes is not null
				  and mes not in ('error','false')
			 )
		RETURN 1 
	ELSE 
		RETURN 0 

END
