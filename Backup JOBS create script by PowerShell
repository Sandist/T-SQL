
ALTER PROCEDURE [dbo].[zabbix_Backup_Jobs_By_PowerShell]
    @BackupDir varchar(max)
	--https://en.dirceuresende.com/blog/sql-server-how-to-back-up-all-sql-agent-jobs-via-clr-c-or-powershell-command-line/
AS
BEGIN
   
    SET NOCOUNT ON; --help

	DECLARE 
		 @ServerName varchar(255) = @@SERVERNAME
		,@Status varchar(max) = ''

		DECLARE @Script VARCHAR(MAX) = '
		$ServerNameList = "'+@ServerName+'"
		$OutputFolder = "'+@BackupDir+'"
		$DoesFolderExist = Test-Path $OutputFolder
		$null = if (!$DoesFolderExist){MKDIR "$OutputFolder"}
 
		[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
 
		$objSQLConnection = New-Object System.Data.SqlClient.SqlConnection
 
		foreach ($ServerName in $ServerNameList)
		{
 
			# Try
			# {
				$objSQLConnection.ConnectionString = "Server=$ServerName;Integrated Security=SSPI;"
				#Write-Host "Try to connect to server $ServerName..." -NoNewline
				$objSQLConnection.Open() | Out-Null
				#Write-Host "Connected."
				$objSQLConnection.Close()
			# }
			# Catch
			# {
			# 	Write-Host -BackgroundColor Red -ForegroundColor White "Falha"
			# 	$errText = $Error[0].ToString()
			# 	if ($errText.Contains("network-related"))
			# 		{Write-Host "Filed connecting to server."}
 
			# 	Write-Host $errText
        
			# 	continue
 
			# }
 
			$srv = New-Object "Microsoft.SqlServer.Management.Smo.Server" $ServerName
 
			# Собираем все в один файл
			 $srv.JobServer.Jobs | foreach {$_.Script() + "GO`r`n"} | out-file "$OutputFolder\jobs_$ServerNameList.sql"
 
			# Каждый джоб в отдельном файле
			# $srv.JobServer.Jobs | foreach-object -process {out-file -filepath $("$OutputFolder\" + $($_.Name -replace ":", "") + ".sql") -inputobject $_.Script() }
 
		}'
    
	 BEGIN TRY
			DECLARE 
				@scriptPS VARCHAR(MAX) = CAST(NEWID() AS VARCHAR(50)) + '.ps1',
				@FilePS VARCHAR(1501)
 
 
			SET @FilePS = @BackupDir + @scriptPS

			  DECLARE
				@objFileSystem INT,
				@objTextStream INT,
				@objErrorObject INT,
				@strErrorMessage VARCHAR(1000),
				@Command VARCHAR(1000),
				@hr INT
 
			SELECT
				@strErrorMessage = 'opening the File System Object'
    
			EXECUTE @hr = sp_OACreate
				'Scripting.FileSystemObject',
				@objFileSystem OUT
 
    
			IF @HR = 0
				SELECT
					@objErrorObject = @objFileSystem,
					@strErrorMessage = 'Creating file "' + @FilePS + '"'
    
    
			IF @HR = 0
				EXECUTE @hr = sp_OAMethod
					@objFileSystem,
					'CreateTextFile',
					@objTextStream OUT,
					@FilePS,
					2,
					True
 
			IF @HR = 0
				SELECT
					@objErrorObject = @objTextStream,
					@strErrorMessage = 'writing to the file "' + @FilePS + '"'
    
    
			IF @HR = 0
				EXECUTE @hr = sp_OAMethod
					@objTextStream,
					'Write',
					NULL,
					@Script
 
    
			IF @HR = 0
				SELECT
					@objErrorObject = @objTextStream,
					@strErrorMessage = 'closing the file "' + @FilePS + '"'
    
    
			IF @HR = 0
				EXECUTE @hr = sp_OAMethod
					@objTextStream,
					'Close'
 
    
			IF @hr <> 0
			BEGIN
    
				DECLARE
					@Source VARCHAR(255),
					@Description VARCHAR(255),
					@Helpfile VARCHAR(255),
					@HelpID INT
    
				EXECUTE sp_OAGetErrorInfo
					@objErrorObject,
					@source OUTPUT,
					@Description OUTPUT,
					@Helpfile OUTPUT,
					@HelpID OUTPUT
        
        
				SELECT
					@strErrorMessage = 'Error whilst ' + COALESCE(@strErrorMessage, 'doing something') + ', ' + COALESCE(@Description, '')
        
        
				RAISERROR (@strErrorMessage,16,1)
        
			END
    
    
			EXECUTE sp_OADestroy
				@objTextStream
    
			EXECUTE sp_OADestroy
				@objTextStream
    
			SET @scriptPS = @BackupDir + @scriptPS
        
        
			DECLARE @cmd VARCHAR(4000)
			SET @cmd = 'powershell -ExecutionPolicy Unrestricted -File "' + @scriptPS + '"'
	
			drop table if exists #res
			create table #res (mes varchar(max))
			insert into #res (mes)
			EXEC master.dbo.xp_cmdshell @cmd
	
			SELECT @Status = STRING_AGG(mes,' ')
			FROM #res
			WHERE mes IS NOT NULL

			EXEC @hr = sp_OACreate
				'Scripting.FileSystemObject',
				@objFileSystem OUT
	
			EXEC @hr = sp_OAMethod
					@objFileSystem,
					'DeleteFile',
					NULL,
					@FilePS

			EXEC sp_OADestroy
				@objFileSystem
    

			SELECT ISNULL(NULLIF(@Status,''),'OK') as status
	END TRY
	BEGIN CATCH
		SELECT @Status = ERROR_MESSAGE()
		SELECT @Status
		THROW;
	END CATCH

END
