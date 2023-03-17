ALTER PROCEDURE [dbo].[replication_obj_modules]
as
	--kmm 2023-03-09 Процедура репликации программных модулей

	--1 исхожу из того, что у подчиненного сервера (куда пишем) не обязательно должен быть доступ на сервер мастер (откуда берем данные). 
		--поэтому не будем обращаться с починенного сервера к мастеру
	--2 если мы добавили запись на репликацию объекта и при этом этого объекта нет на сервере мастере, тогда удаляем его и на подчиненных серверах
	--3 что нужно чтобы все работало
		-- процедура get_PogramStript_by_PowerShell - она выдает код объекта
		-- таблица с настройками репликаций ITBASE.dbo.replication_objects
		-- таблица логов itbase.dbo.replication_objects_log
		-- ко всем серверам нужны linked с включенными параметрами RPC

BEGIN
	SET NOCOUNT ON;

	if OBJECT_ID('ITBASE.dbo.get_PogramStript_by_PowerShell') is null 
	BEGIN
		RAISERROR('отсутствует процедура ITBASE.dbo.get_PogramStript_by_PowerShell',16,1)
		RETURN
	END

	if OBJECT_ID('ITBASE.dbo.replication_objects') is null 
	BEGIN
		RAISERROR('отсутствует таблица ITBASE.dbo.replication_objects',16,1)
		RETURN
	END

	IF OBJECT_ID('ITBASE.dbo.replication_objects_log') is null 
	BEGIN
		RAISERROR('отсутствует таблица ITBASE.dbo.replication_objects_log',16,1)
		RETURN
	END

	--делаем снимок таблицы настроек
	--srv_type = 1 - в поле srv_kuda будут указаны подчиненные сервера, разделителем будет "|" (вертикальная черта)
	--srv_type = 2 - в поле srv_kuda будет указан extid из таблицы Elf.dbo.текстовыйЭкспорт
	DROP TABLE IF EXISTS #replication_objects
	SELECT DISTINCT 
		   id, srv, db, scma, obj
	      ,t.[value] as srv_kuda
		  ,srv_type
	INTO #replication_objects
	FROM (
			SELECT id, srv, db, scma, obj, srv_kuda, srv_type
			FROM ITBASE.dbo.replication_objects
			WHERE srv = @@SERVERNAME
			  AND deleted = 0
			  AND srv_type = 1
			UNION ALL
			SELECT id, srv, db, scma, obj, CONVERT(VARCHAR(MAX),t.РасширенныйКомментарий) as srv_kuda, srv_type
			FROM ITBASE.dbo.replication_objects ro
			INNER JOIN Elf.dbo.текстовыйЭкспорт t
				ON ro.srv_kuda = t.ExtID
			WHERE srv = @@SERVERNAME
			  AND ro.deleted = 0 and t.deleted = 0
			  AND ro.srv_type = 2
		 ) A
	OUTER APPLY(SELECT *
	            FROM STRING_SPLIT(srv_kuda,'|')
				) t
	WHERE srv <> t.[value]

	--Получаем все базы данных текущего сервера в которых нас интересуют объекты
	--так как sys.objects у каждой базы свой, поэтому для каждой базы придется собирать инфу отдельно
	DECLARE CUR CURSOR FORWARD_ONLY READ_ONLY
	FOR
		select DISTINCT db
		from #replication_objects

	DECLARE @srv sysname
		   ,@db sysname
		   ,@scma sysname
		   ,@obj sysname
		   ,@obj_action varchar(100)
		   ,@obj_type varchar(10)
		   ,@obj_type_word varchar(50)
		   --,@id_task int
		   ,@SQL VARCHAR(max) = ''
		   ,@NSQL NVARCHAR(MAX) = N''
		   ,@obj_text nvarchar(max) = N''
		   ,@obj_permissions nvarchar(max) 
		   ,@err varchar(max)

	OPEN CUR
	FETCH NEXT FROM CUR INTO @db

	--Сюда соберем все объекты которы нас интересуют, кода в последний раз были обновлены, и существуют ли они (может удалили)
	DROP TABLE IF EXISTS #objects_info
	CREATE TABLE #objects_info (db sysname, scma sysname, obj sysname, obj_type varchar(50), modify_date datetime, isExists bit)

	WHILE @@FETCH_STATUS = 0
	BEGIN
	
		SET @SQL = '
			SELECT r.db, r.scma, r.obj, o.type as obj_type, o.modify_date
				  ,CASE WHEN o.name IS NULL THEN 0 ELSE 1 END as isExists
			FROM (select distinct db, scma, obj
						from #replication_objects r
						where db = '''+@db+'''
					   ) r
			LEFT JOIN (select name, type, schema_id, modify_date
					   from ['+@db+'].sys.objects
					   where type in (''P'', ''PC'', ''RF'', -- stored procs  
									  ''V'', -- views  
									  ''TF'', ''TF'', ''FT'', -- table-valued functions  
									  ''FN'', ''FS'', -- scalar-valued functions  
									  ''AF'' -- aggregate functions  
									 ) 
					  ) o
				 ON r.obj = o.name
			LEFT JOIN ['+@db+'].sys.schemas s 
				ON o.schema_id = s.schema_id  
			   AND r.scma = s.name
			'
		INSERT INTO #objects_info (db, scma, obj, obj_type, modify_date, isExists)
		EXEC (@SQL)
		FETCH NEXT FROM CUR INTO @db
	END

	CLOSE CUR
	DEALLOCATE CUR


	--Берем все подчиненные сервера
	DECLARE CUR_SRV CURSOR FORWARD_ONLY READ_ONLY
	FOR 
		select DISTINCT srv_kuda
		from #replication_objects

	OPEN CUR_SRV
	FETCH NEXT FROM CUR_SRV INTO @srv

	--инфа по объектам на удаленном сервере
	DROP TABLE IF EXISTS #objects_info_srv_kuda
	CREATE TABLE #objects_info_srv_kuda (srv_kuda sysname, db sysname, scma sysname, obj sysname, obj_type varchar(50), modify_date datetime)

	WHILE @@FETCH_STATUS = 0 --srv
	BEGIN
		--сначала проверим есть ли у нас доступ к серверу
		SET @SQL = 'exec sp_testlinkedserver ['+@srv+']'
		BEGIN TRY
			EXEC (@SQL)
		END TRY
		BEGIN CATCH
			--Если доступа нет , тогда будем писать в лог и идти дальше
			insert into ITBASE.dbo.replication_objects_log(id_task, error_text, srv_kuda)
			select DISTINCT id, 'Не удалось получить доступ к серверу ['+@srv+']', srv_kuda
			from #replication_objects
			where srv_kuda = @srv

			--Исключаем из дальнейших операций сервер к которому не можем получить доступ
			DELETE FROM #replication_objects
			WHERE srv_kuda = @srv

			GOTO Cont --чтобы не усложнять конструкцию отсылаю к следующему витку
		END CATCH

		--теперь соберем информацию по всем необходимым объектам на подчиненном сервере
		--переберем базы и заджойним объекты из снимка #replication_objects
		DECLARE CUR_DB CURSOR FORWARD_ONLY READ_ONLY
		FOR
			select distinct DB
			from #replication_objects
			where srv_kuda = @srv
	
		OPEN CUR_DB
		FETCH NEXT FROM CUR_DB INTO @db

		WHILE @@FETCH_STATUS = 0 --db
		BEGIN
			SET @SQL = '
			SELECT '''+@srv+''' as srv_kuda, '''+@db+''' as db, s.name as scma, o.name as obj, o.type as obj_type, o.modify_date
			FROM ['+@srv+'].['+@db+'].sys.objects o
			INNER JOIN ['+@srv+'].['+@db+'].sys.schemas s 
				ON o.schema_id = s.schema_id  
			INNER JOIN (select distinct scma, obj
						from #replication_objects r
						where db = '''+@db+'''
						  and srv_kuda = '''+@srv+'''
					   ) r
				ON r.scma = s.name
				and r.obj = o.name
			WHERE o.type in (''P'', ''PC'', ''RF'', -- stored procs  
							 ''V'', -- views  
							 ''TF'', ''TF'', ''FT'', -- table-valued functions  
							 ''FN'', ''FS'', -- scalar-valued functions  
							 ''AF'' -- aggregate functions  
							) 
			'
			INSERT INTO #objects_info_srv_kuda (srv_kuda, db, scma, obj, obj_type, modify_date)
			EXEC (@SQL) 
			FETCH NEXT FROM CUR_DB INTO @db
		END
		CLOSE CUR_DB
		DEALLOCATE CUR_DB

		Cont:
		FETCH NEXT FROM CUR_SRV INTO @srv
	END

	CLOSE CUR_SRV
	DEALLOCATE CUR_SRV

	--Теперь в разрезе 2 серверов у нас есть таблицы:
		--#objects_info - в которой у нас объекты сервера мастера
		--#objects_info_srv_kuda - в которой у нас объекты сервера подчиненного
	--Сравним и найдем объекты которые нужно создать/изменить/удалить на подчиненном сервере
	DROP TABLE IF EXISTS #fin
	SELECT DISTINCT 
			r.id, r.db, r.scma, r.obj, r.srv_kuda
			,CASE WHEN o.isExists = 0 AND k.modify_date IS NOT NULL 
					THEN 'DROP' --Если на мастере нет, а на подчиненном есть, пот этом этот объект должен реплицироваться, тогда удаляем его на подчиненном сервере
				WHEN o.isExists = 1 AND k.modify_date IS NOT NULL AND o.obj_type <> k.obj_type 
					THEN 'ERROR'  --Если объект существует на мастере и на подчиненном сервере, но при этом тип объекта различается, то выводим ошибку и ничего не делаем
				WHEN o.isExists = 1 AND k.modify_date IS NOT NULL AND o.obj_type = k.obj_type AND o.modify_date > k.modify_date 
					THEN 'UPDATE' --Если объект существует на мастере и на подчиненном, тип совпадает и при этом версия на мастере больше чем на подчиненном, тогда обновляем на починенном
				WHEN o.isExists = 1 AND k.modify_date IS NULL 
					THEN 'CREATE' --Если на мастере есть, а на подчиненном нет, тогда создаем объект на подчиненном сервере
			END AS obj_action --какая операция над объектом должна быть выполнена
		   ,ISNULL(o.obj_type,k.obj_type) as obj_type --тип из sys.objects - будем передавать в процедуру для получения скрипта
		   ,CASE WHEN ISNULL(o.obj_type,k.obj_type) IN ('P', 'PC', 'RF') THEN 'PROCEDURE'
		         WHEN ISNULL(o.obj_type,k.obj_type) IN ('V') THEN 'VIEW'
				 WHEN ISNULL(o.obj_type,k.obj_type) IN ('TF', 'TF', 'FT','FN', 'FS','AF') THEN 'FUNCTION'
			END AS obj_type_word --тип объекта - нужно для формирования текста операции DROP 
	INTO #fin
	FROM #replication_objects r
	LEFT JOIN #objects_info o
		ON r.db = o.db
		AND r.scma = o.scma
		AND r.obj = o.obj
	LEFT JOIN #objects_info_srv_kuda k
		ON r.db = k.db
		AND r.scma = k.scma
		AND r.obj = k.obj
		AND r.srv_kuda = k.srv_kuda

	-- obj_action = ERROR - выше описал
	insert into ITBASE.dbo.replication_objects_log(id_task, error_text, srv_kuda)
	SELECT DISTINCT id, 'ОШИБКА! на реплицируемом сервере ['+srv_kuda+'] существует объект ['+db+'].['+scma+'].['+obj+'] с таким же БД.СХЕМА.ИМЯ, но другим типом', srv_kuda
	FROM #fin
	WHERE obj_action = 'ERROR'

	--проблеммные репликации мы удаляем из снимка так как их уже добавили в лог
	--и те объекты по которым никаких действий совершать не нужно
	DELETE FROM #fin
	WHERE obj_action = 'ERROR'
	   OR obj_action IS NULL

	---------------------------------------------------------------------------------------------------------
	--SELECT * FROM #fin


	DECLARE @isCreate bit = 0

	--Теперь идем от объекта - чтобы не запрашивать скрипт через smo более 1 раза
	DECLARE CUR_OBJ CURSOR FORWARD_ONLY READ_ONLY
	FOR
		SELECT DISTINCT db, scma, obj, obj_action, obj_type, obj_type_word
		FROM #fin

	OPEN CUR_OBJ
	FETCH NEXT FROM CUR_OBJ INTO @db, @scma, @obj, @obj_action, @obj_type, @obj_type_word

	WHILE @@FETCH_STATUS = 0
	BEGIN
		--Формируем команду, если нужно удалить объект на подчиненном сервере
		IF @obj_action = 'DROP'
			SET @obj_text = 'DROP ' + @obj_type_word + ' ' + @scma + '.' + @obj
		ELSE 
			--Иначе получаем скрипт из get_PogramStript_by_PowerShell, но сначала определим нужен скрипт на создание или изменение
			BEGIN
				IF @obj_action = 'CREATE' 
					SET @isCreate = 1
				ELSE
					SET @isCreate = 0

				EXEC itbase.dbo.get_PogramStript_by_PowerShell
					@ServerName = @@SERVERNAME --на каком сервере лежит объект с оторого берем скрипт создания/изменения
				   ,@dbName = @db --база данных
				   ,@SchemaName = @scma --схема
				   ,@ObjectName = @obj --имя объекта
				   ,@ObjectType = @obj_type --тип объекта (описано подробно внутри процедуры)
				   ,@isCreate = @isCreate --скрипт создания или изменения
				   ,@IncludePermission = 1 --нужно ли получить дополнительно скрипт получения прав доступа
				   ,@txt = @obj_text OUTPUT --скрипт создания/изменения
				   ,@permissions = @obj_permissions OUTPUT --скрипт по политики безопасности (права доступа)
			END

	   -- PRINT @obj_text
	   -- PRINT @obj_permissions
	   -- PRINT '--------------------------------------------------------------------------------'

		--перебираем все сервера на которых запускаем подготовленный выше запрос 
		DECLARE CUR_SRV_FIN CURSOR FORWARD_ONLY READ_ONLY
		FOR
			SELECT DISTINCT srv_kuda
			FROM #fin
			WHERE db = @db
			  AND scma = @scma
			  AND obj = @obj
			  AND obj_action = @obj_action
			  AND obj_type = @obj_type

		OPEN CUR_SRV_FIN
		FETCH NEXT FROM CUR_SRV_FIN INTO @srv

		WHILE @@FETCH_STATUS = 0
		BEGIN
			BEGIN TRY
				--запускаем основной запрос
				--SET @NSQL = ' EXEC ['+@srv+'].['+@db+'].dbo.sp_executeSQL @obj_text'
				--EXEC sp_executeSQL @NSQL, N'@obj_text nvarchar(max)', @obj_text
				--пришлост применить такую конструкцию
				--1 - собираем запрос в try...catch для запуска на подчиненном сервере - только в таком случае мы сможем отловить ошибку
				--2 - собираем запрос для запуска с сервера мастера - в этот раз подставляем сервер и базу данных подчиненного сервера
				--3 - запускаем полученную матрешку уже на сервере мастере и прокидыввем код ошибки через все 3 слоя
				SET @NSQL = '
							 DECLARE @NSQL nvarchar(max)

							 SET @NSQL = ''BEGIN TRY
							 				  EXEC [ITBASE].dbo.sp_executeSQL @obj_text
										   END TRY
										   BEGIN CATCH
											  SET @err = ERROR_MESSAGE()
										   END CATCH
										 ''
							 EXEC ['+@srv+'].['+@db+'].dbo.sp_executeSQL @NSQL, N''@obj_text nvarchar(max), @err varchar(1000) OUTPUT'', @obj_text, @err = @err OUTPUT
							 '
				EXEC sp_executeSQL @NSQL, N'@obj_text nvarchar(max), @err varchar(4000) OUTPUT',  @obj_text, @err = @err OUTPUT
				
				--запускаем запрос с правами
				IF @obj_permissions IS NOT NULL AND NULLIF(@err,'') IS NULL
				BEGIN
					--сначала собираем запрос который будет подставлять нужный нам сервер
					SET @NSQL = ' DECLARE @err varchar(4000) = ''''
									EXEC ['+@srv+'].['+@db+'].dbo.sp_executeSQL @obj_permissions, N''@err varchar(4000) = '''''''' OUTPUT'', @err = @err OUTPUT
								  SET @err_o = @err
								 '
					--теперь запустим готовый запрос, при этом получаем текст ошибки, в случае если такие есть
					EXEC sp_executeSQL @NSQL, N'@obj_permissions nvarchar(max), @err_o varchar(4000) OUTPUT', @obj_permissions, @err_o = @err OUTPUT
				END
				--пишем в лог, что все успешно выполнили
				--есть нюанс, дело в том, что try...catch не отлавливает ошибки связанные с отсутствием объектов
				--поэтому для таких случаев пишем ошибку на всякий в лог даже после "успешного" выполнения
				INSERT INTO itbase.dbo.replication_objects_log (id_task, command_type, error_text, srv_kuda)
				SELECT id, LEFT(obj_action,1), ISNULL(NULLIF(@err,''),ERROR_MESSAGE()), srv_kuda
				FROM #fin
				WHERE db = @db
				  AND scma = @scma
				  AND obj = @obj
				  AND obj_action = @obj_action
				  AND obj_type = @obj_type
				  AND srv_kuda = @srv
			END TRY
			BEGIN CATCH
				INSERT INTO itbase.dbo.replication_objects_log (id_task, command_type, error_text, srv_kuda)
				SELECT id, LEFT(@obj_action,1), ERROR_MESSAGE(), srv_kuda
				FROM #fin
				WHERE db = @db
				  AND scma = @scma
				  AND obj = @obj
				  AND obj_action = @obj_action
				  AND obj_type = @obj_type
				  AND srv_kuda = @srv
			END CATCH

			FETCH NEXT FROM CUR_SRV_FIN INTO @srv
		END
		CLOSE CUR_SRV_FIN
		DEALLOCATE CUR_SRV_FIN

		FETCH NEXT FROM CUR_OBJ INTO @db, @scma, @obj, @obj_action, @obj_type, @obj_type_word
	END 
	CLOSE CUR_OBJ
	DEALLOCATE CUR_OBJ
	
	--select * from itbase.dbo.replication_objects_log

END
