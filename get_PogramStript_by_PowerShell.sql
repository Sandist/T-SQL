ALTER procedure [dbo].[get_PogramStript_by_PowerShell]
		@ServerName nvarchar(255) 
	   ,@dbName nvarchar(255) 
	   ,@SchemaName nvarchar(255) 
	   ,@ObjectName nvarchar(255) 
	   ,@ObjectType nvarchar(10) 
	   ,@isCreate bit = 1
	   ,@IncludePermission bit = 0
	   ,@txt NVARCHAR(MAX) OUTPUT
	   ,@permissions NVARCHAR(MAX) OUTPUT

as
	--kmm 2023-03-06 с помощью PowerShell получаем скрипт программного объекта (SP, Function, View)
BEGIN

	SET NOCOUNT ON;

	--@ObjectType:
	--P = SQL Stored Procedure
	--PC = Assembly (CLR) stored-procedure
	--RF = Replication-filter-procedure
	--V = View
	--AF = Aggregate function (CLR)
	--FN = SQL scalar function
	--FS = Assembly (CLR) scalar-function
	--FT = Assembly (CLR) table-valued function
	--IF = SQL inline table-valued function
	--TF = SQL table-valued-function
	--F - function, добавил сам, чтобы не париться если нужно и просто передать что нужна функция
	
	
	DECLARE @cmd VARCHAR(8000) --сюда собираем скрипт powershell
		   ,@ObjectTypePS varchar(100) --объясняем PS что какой программный объект нужен
		   ,@HeaderOperation varchar(10) --нужен скрипт ALTER или CREATE
		   

	SELECT @ObjectTypePS = CASE WHEN @ObjectType in ('P','PC','RF') THEN 'StoredProcedures'
								WHEN @ObjectType in ('F','AF','FN','FS','FT','IF','TF') THEN 'UserDefinedFunctions'
								WHEN @ObjectType = 'V' THEN 'Views'
						   END
		  ,@HeaderOperation = CASE WHEN @isCreate = 1 THEN '$false'
								   ELSE '$true'
							  END
	IF @ObjectTypePS IS NULL
	begin
		raiserror('данный тип объекта не поддерживается',16,1)
		return
	end

	--к сожалению так и не смог разобраться как передавать скрипт не в одну строку и не отдельным файлом ps1
	SET @cmd = 'powershell.exe -c " try {[System.Reflection.Assembly]::LoadWithPartialName(''Microsoft.SqlServer.SMO'') | out-null; '
	SET @cmd += '$srv = new-object Microsoft.SqlServer.Management.Smo.Server('''+@ServerName+'''); '
	SET @cmd += '$db = $srv.Databases.Item('''+@dbName+'''); '
	SET @cmd += '$obj = $db.'+@ObjectTypePS+' | where { $_.Schema -eq '''+@SchemaName+''' -and $_.Name -eq '''+@ObjectName+'''}; '
	SET @cmd += '$retval = $obj.ScriptHeader('+@HeaderOperation+') + $obj.TextBody; '
	SET @cmd += 'ECHO $retval.ToString(); ' 

	IF @IncludePermission = 1
	BEGIN
		SET @cmd += 'ECHO ''{|START PERMISSIONS|}''; '
		SET @cmd += '$objPermission=$obj.EnumObjectPermissions() | Select-Object objectschema, objectname, permissiontype, PermissionState, Grantee; '
		SET @cmd += 'if ($objPermission)'
		SET @cmd += '{'
		SET @cmd += '	foreach ($rp in $objPermission)'
		SET @cmd += '	{'
		SET @cmd += '		$spcontent = ''BEGIN TRY '' + $rp.PermissionState.tostring() + '' '' + $rp.permissiontype.tostring() + '' ON ['' + $rp.objectschema + ''].['' + $rp.objectname + ''] TO ['' + $rp.grantee + '']; END TRY BEGIN CATCH SET @err += ''''|''''+ISNULL(ERROR_MESSAGE(),'''''''') END CATCH'';'
		SET @cmd += '		ECHO $spcontent;'
		SET @cmd += '	}'
		SET @cmd += '}'
	END
	SET @cmd += '} catch {echo ''error''} "'


	declare @Tab table (id int primary key identity(1,1), line NVARCHAR(MAX))
    insert into @tab
	EXEC master.dbo.xp_cmdshell @cmd

	--пришлось переиграть, суть в том что в sp_executesql нельзя использовать go - так как там только один пакет может быть
	--а если ты хочешь использовать create или alter запрос тебе нужен отдельный пакет
	--поэтому права будем разделять и запускать отдельным пакетом
	
--	select @txt = N'USE ['+@dbName+']
--GO
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
--'+(Select STRING_AGG(line,'
--') from @tab)
	
	declare @permissionLine int
	select @permissionLine = id
	from @Tab
	where line = '{|START PERMISSIONS|}'

	SET @txt = (Select STRING_AGG(line,'
') from @tab where @permissionLine is null or id < @permissionLine)

	IF @permissionLine IS NOT NULL
	BEGIN
--		SET @permissions = 'declare @err varchar(max) = ''''
--'
		SET @permissions = (Select STRING_AGG(line,'
') from @tab where id > @permissionLine)
	END

	return  

END
