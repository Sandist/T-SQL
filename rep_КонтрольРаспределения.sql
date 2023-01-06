
 ALTER procedure [dbo].[rep_КонтрольРаспределения]
 --declare
 		@Точки varchar(max) = ''
        ,@Товары varchar(max) = ''
 	   ,@Бренды varchar(max) = ''
 	   ,@ВсеТовары int = 0
 	   ,@Автозаказ int = 10
 	   ,@ТипД int = 10
 	   ,@ЕстьЗаказ int = 10
 	   ,@ТочкиИскл varchar(max) = ''
 	   ,@ТоварыИскл varchar(max) = ''
 	   ,@БрендыИскл varchar(max) = ''
 	   ,@ВозвратНаРЦ int = 0 --Вывоз товара из магазина на склад (форма Контроль перераспределения)
 	   ,@БезРаспределения bit = 0
 	   ,@Борк bit = 0
 	   ,@ПоМес bit = 0
 	   
 /* @ВсеТовары:
 	   --1 Остаток на складе – в выборку попадают все товары, которые имеют остаток на РЦ > 0, независимо от остатка на ТТ и ТД.
 	   --2 Акция – в выборку попадают все товары из выборки Все товары, но которые сейчас подсвечены красным цветом. 
 	   --3 Нематрица – в выборку попадают все товары из выборки Все товары с признаками, кроме тех, что указаны выше.
 	   --4 Список – вывод информации без группирующей строки.
 	   --5 Матрица – в выборку попадают все товары из выборки Все товары, но только с данными матричными признаками:
 		   E,М,МБ,ММ,РР
 	   --6 ПИК – в выборку попадают все товары из выборки Все товары, у которых проставлен данный признак в карте товара. 
 	   --7 Матрица на складе – в выборку попадают все товары с данными матричными признаками, которые имеют остаток на РЦ > 0, независимо от остатка на ТТ и ТД :
            E,М,МБ,ММ,РР
 	   --8 Аксы – в выборку попадают товары из выборки Все товары из групп, у которых есть префикс А.*
 	   --9 Основной товар – в выборку попадают товары из выборки Все товары из групп, у которых нет префикса А.*
 	   --10 Только те позиции по которым есть излишек
 
 		   все товары|0
 	Остаток на складе|1
 				Акция|2
 			Нематрица|3
 			   Список|4
 			  Матрица|5
 				  ПИК|6
 	Матрица на складе|7
 				 Аксы|8
 	   Основной товар|9
 		  С излишками|10
 
 --11.09.2020 11 пункт переехал с параметра @ВсеТовары в @ТипД
 ТипД
 --11 Не доступен к развозу
 Не доступен к развозу|11
 
 */
 as
 -- =============================================
 -- Author:		kmm
 -- Create date: 06-02-2020
 -- Description:	Процедура для БУРа КонтрольРаспределения
 -- Заявка: 453580
 -- =============================================
 BEGIN

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
 	SET NOCOUNT ON;

	declare @dateB datetime = GETDATE()
		   ,@i int
		   ,@paramstr varchar(max) = ''
		   ,@IntoLog bit = 1 --нужно писать в лог или нет

	SELECT @paramstr = CONCAT('SELECT '
	   ,' @Точки = ' + CASE WHEN @Точки IS NULL THEN 'NULL' WHEN @Точки = '' THEN '''''' ELSE QUOTENAME(@Точки,'''') END 
       ,', @Товары = ' + CASE WHEN @Товары IS NULL THEN 'NULL' WHEN @Товары = '' THEN '''''' ELSE QUOTENAME(@Товары,'''') END
 	   ,', @Бренды = ' + CASE WHEN @Бренды IS NULL THEN 'NULL' WHEN @Бренды = '' THEN '''''' ELSE QUOTENAME(@Бренды,'''') END
 	   ,', @ВсеТовары = ' + ISNULL(CONVERT(VARCHAR,@ВсеТовары),'NULL')
 	   ,', @Автозаказ = ' + ISNULL(CONVERT(VARCHAR,@Автозаказ),'NULL')
 	   ,', @ТипД = ' + ISNULL(CONVERT(VARCHAR,@ТипД),'NULL')
 	   ,', @ЕстьЗаказ = ' + ISNULL(CONVERT(VARCHAR,@ЕстьЗаказ),'NULL')
 	   ,', @ТочкиИскл = ' + CASE WHEN @ТочкиИскл IS NULL THEN 'NULL' WHEN @ТочкиИскл = '' THEN '''''' ELSE QUOTENAME(@ТочкиИскл,'''') END
 	   ,', @ТоварыИскл = ' + CASE WHEN @ТоварыИскл IS NULL THEN 'NULL' WHEN @ТоварыИскл = '' THEN '''''' ELSE QUOTENAME(@ТоварыИскл,'''') END
 	   ,', @БрендыИскл = ' + CASE WHEN @БрендыИскл IS NULL THEN 'NULL' WHEN @БрендыИскл = '' THEN '''''' ELSE QUOTENAME(@БрендыИскл,'''') END
 	   ,', @ВозвратНаРЦ = ' + ISNULL(CONVERT(VARCHAR,@ВозвратНаРЦ),'NULL')
 	   ,', @БезРаспределения = ' + ISNULL(CONVERT(VARCHAR,@БезРаспределения),'NULL')
 	   ,', @Борк = ' + ISNULL(CONVERT(VARCHAR,@Борк),'NULL')
 	   ,', @ПоМес = ' + ISNULL(CONVERT(VARCHAR,@ПоМес),'NULL'))

	declare @t table (id int identity(1,1) primary key, step varchar(250), dtBegin datetime, dtEnd datetime, durationSec int, paramstr varchar(max))
	insert into @t (step, dtBegin, paramstr)
	select 'start',GETDATE(),@paramstr
	select @i = MAX(id) from @t

	declare @Ноль_строка varchar(10) --нужно для отображения нуля с нужным количеством лидирующих пробелов при выводе ОбщОст
 
 --SELECT @Точки     ='',@Товары    ='1408GU1MS5LDV8,',@Бренды    ='',@ВсеТовары =0,@Автозаказ =10,@ТипД      =10,@ЕстьЗаказ =10,@ТочкиИскл     ='',@ТоварыИскл    ='',@БрендыИскл    ='' ,@ВозвратНаРЦ=0
 
 		SELECT @Товары = REPLACE (@Товары,' ','')
 		  ,@Точки = REPLACE (@Точки,' ','')
 		  ,@Бренды = REPLACE (@Бренды,' ','')
 		  ,@ТоварыИскл = REPLACE (@ТоварыИскл,' ','')
 		  ,@ТочкиИскл = REPLACE (@ТочкиИскл,' ','')
 		  ,@БрендыИскл = REPLACE (@БрендыИскл,' ','')
 
 	if OBJECT_ID('tempdb..#TT') is not null drop table #TT
 	create table #TT(TT_ExtID varchar(14))
 	if ISNULL(@Точки,'') <> ''
 		insert into #tt(TT_ExtID)
 		select distinct strToken
 		from ITBase.dbo.fnExplodeStr(@Точки,',',-1) t
		
	--DROP TABLE IF EXISTS #Zont
	--SELECT ExtID_ВажностьРЦ, ExtID_ВажностьТТ
	--INTO #Zont
	--FROM v_Важность_НастройкиРазвозаПоГруппе
	--WHERE Тип > 0
	--GROUP BY ExtID_ВажностьРЦ, ExtID_ВажностьТТ
	
	--INSERT INTO #TT (TT_ExtID) 
	--SELECT z.ExtID_ВажностьТТ
	--FROM #TT t
	--INNER JOIN #Zont z 
	--	ON  t.TT_ExtID = z.ExtID_ВажностьРЦ
	--LEFT JOIN #TT tt
	--	ON tt.TT_ExtID = z.ExtID_ВажностьТТ
	--WHERE tt.TT_ExtID IS NULL
	--GROUP BY z.ExtID_ВажностьТТ	
 
 	if OBJECT_ID('tempdb..#TT_ex') is not null drop table #TT_ex
 	create table #TT_ex(TT_ExtID varchar(14))
 	IF ISNULL(@ТочкиИскл,'') <> ''
 		insert into #tt_ex(TT_ExtID)
 		select distinct strToken
 		from ITBase.dbo.fnExplodeStr(@ТочкиИскл,',',-1) t
 
 	if object_id('tempdb.dbo.#subjTT') is not null drop table #subjTT
 	create table #subjTT (DocID_Важность int primary key)
 	if EXISTS (select 1 from #TT)
 		insert into #subjTT (DocID_Важность)
 		select distinct vp2.DocID as DocID_Важность--, vp2.ExtID as ExtID_Важность
 		from elf.dbo.ВажностьПроцесса as vp with(nolock)
 		inner join elf.dbo.ВажностьПроцесса as vp2 with(nolock) on
 			  vp2.GroupPath + CONVERT(VARCHAR,vp2.DocID) + '.' like vp.GroupPath + CONVERT(VARCHAR,vp.DocID) + '.%'
 		inner join #TT tt on vp.ExtID = tt.TT_ExtID
		--inner join v_Важность v on vp2.DocID = v.DocID
		where vp2.isGroup = 0

	if object_id('tempdb.dbo.#subjTT_ex') is not null drop table #subjTT_ex
	create table #subjTT_ex (DocID_Важность int)
	if EXISTS (select 1 from #TT_ex)
	    insert into #subjTT_ex (DocID_Важность)
		select distinct vp2.DocID as DocID_Важность--, vp2.ExtID as ExtID_Важность
		from elf.dbo.ВажностьПроцесса as vp with(nolock)
		inner join elf.dbo.ВажностьПроцесса as vp2 with(nolock) on
			  vp2.GroupPath + CONVERT(VARCHAR,vp2.DocID) + '.' like vp.GroupPath + CONVERT(VARCHAR,vp.DocID) + '.%'
		inner join #TT_ex tt on vp.ExtID = tt.TT_ExtID
		--inner join v_Важность v on vp2.DocID = v.DocID
		where vp2.isGroup = 0

	if OBJECT_ID('tempdb..#Tov') is not null drop table #Tov
	create table #Tov(Tov_ExtID varchar(14))
	IF ISNULL(@Товары,'') <> ''
		insert into #Tov(Tov_ExtID)
		select distinct strToken
		from ITBase.dbo.fnExplodeStr(@Товары,',',-1)

	if OBJECT_ID('tempdb..#Tov_ex') is not null drop table #Tov_ex
	create table #Tov_ex(Tov_ExtID varchar(14))
	IF ISNULL(@ТоварыИскл,'') <> ''
		insert into #Tov_ex(Tov_ExtID)
		select distinct strToken
		from ITBase.dbo.fnExplodeStr(@ТоварыИскл,',',-1)

	if object_id('tempdb.dbo.#subjTov') is not null drop table #subjTov
	create table #subjTov (DocID_Ресурс int)
	if EXISTS (select 1 from #Tov)
		insert into #subjTov (DocID_Ресурс)
		select distinct ss.DocID as DocID_Ресурс--, ss.ExtID as ExtID_Ресурс
		from elf.dbo.Ресурс as s with(nolock)
		inner join elf.dbo.Ресурс as ss with(nolock) on
			  ss.GroupPath + cast(ss.DocID as varchar) + '.' like s.GroupPath + cast(s.DocID as varchar) + '.%'
		inner join #Tov tov on s.ExtID = tov.Tov_ExtID
		--inner join v_Ресурс res on ss.ExtID = res.ExtID_Товар
		where ss.isGroup = 0

	if object_id('tempdb.dbo.#subjTov_ex') is not null drop table #subjTov_ex
	create table #subjTov_ex (DocID_Ресурс int)
	if EXISTS (select 1 from #Tov_ex)
		insert into #subjTov_ex (DocID_Ресурс)
		select distinct ss.DocID as DocID_Ресурс--, ss.ExtID as ExtID_Ресурс
		from elf.dbo.Ресурс as s with(nolock)
		inner join elf.dbo.Ресурс as ss with(nolock) on
			  ss.GroupPath + cast(ss.DocID as varchar) + '.' like s.GroupPath + cast(s.DocID as varchar) + '.%'
		inner join #Tov_ex tov on s.ExtID = tov.Tov_ExtID
		--inner join v_Ресурс res on ss.ExtID = res.ExtID_Товар
		where ss.isGroup = 0

	if OBJECT_ID('tempdb..#Brand') is not null drop table #Brand
	create table #Brand(Tov_ExtID varchar(14))
	IF ISNULL(@Бренды,'') <> ''
		insert into #Brand(Tov_ExtID)
		select distinct strToken
		from ITBase.dbo.fnExplodeStr(@Бренды,',',-1)
	
	if OBJECT_ID('tempdb..#Brand_ex') is not null drop table #Brand_ex
	create table #Brand_ex(Tov_ExtID varchar(14))
	IF ISNULL(@БрендыИскл,'') <> ''
		insert into #Brand_ex(Tov_ExtID)
		select distinct strToken
		from ITBase.dbo.fnExplodeStr(@БрендыИскл,',',-1)

	if object_id('tempdb.dbo.#subBrand') is not null drop table #subBrand
	create table #subBrand (DocID_Производитель int)
	if EXISTS (select 1 from #Brand)
		insert into #subBrand (DocID_Производитель)
		select distinct ss.DocID as DocID_Производитель
		from ELF.dbo.Производитель as s with(nolock)
		inner join ELF.dbo.Производитель as ss with(nolock) on
			  ss.GroupPath + cast(ss.DocID as varchar) + '.' like s.GroupPath + cast(s.DocID as varchar) + '.%'
		inner join #Brand tov on s.ExtID = tov.Tov_ExtID
		where ss.isGroup = 0

	if object_id('tempdb.dbo.#subBrand_ex') is not null drop table #subBrand_ex
	create table #subBrand_ex (DocID_Производитель int)
	if EXISTS (select 1 from #Brand_ex)
		insert into #subBrand_ex (DocID_Производитель)
		select distinct ss.DocID as DocID_Производитель
		from ELF.dbo.Производитель as s with(nolock)
		inner join ELF.dbo.Производитель as ss with(nolock) on
			  ss.GroupPath + cast(ss.DocID as varchar) + '.' like s.GroupPath + cast(s.DocID as varchar) + '.%'
		inner join #Brand_ex tov on s.ExtID = tov.Tov_ExtID
		where ss.isGroup = 0

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#InWay',GETDATE()
	select @i = MAX(id) from @t

	if OBJECT_ID('tempdb..#InWay')is not null drop table #InWay
	create table #InWay (DocID_Ресурс varchar(14), DocID_Важность varchar(14), ВПути INT, Зонтик INT)
	;WITH INW AS (
				select DocID_Ресурс, DocID_Важность, SUM(ВПути) as ВПути, Зонтик
				from (
						select DocID_Ресурс, DocID_Важность, ВПути, Зонтик
						from ТоварВПути
						union all
						--добавляем в группу по зонтику все впути магазинов под зонтиком без склада (склад плюсуется еще в процедуре формирования ВПути)
						--делаю так потому что нужно только выводить все значения впути под зонтиком, расчет остается неизменным
						--kmm2022-05-30
						select DocID_Ресурс, Зонтик, SUM(ВПути) as ВПути, NULL as Зонтик
						from ТоварВПути
						where Зонтик is not null
						  AND ЭтоСкладЗонтика = 0
						group by DocID_Ресурс, Зонтик
					  ) a
				group by DocID_Ресурс, DocID_Важность, Зонтик
				)
	insert into #InWay (DocID_Ресурс, DocID_Важность, ВПути, Зонтик)
	select vp.DocID_Ресурс, vp.DocID_Важность, SUM(vp.ВПути) as ВПути, VP.Зонтик
	from INW vp
	inner join v_ТТ_БУР v on v.DocID_Важность = vp.DocID_Важность
	left join #subjTov res on vp.DocID_Ресурс = res.DocID_Ресурс
	left join #subjTov_ex res_ex on vp.DocID_Ресурс = res_ex.DocID_Ресурс
	left join #subjTT tt on vp.DocID_Важность = tt.DocID_Важность
	left join #subjTT_ex tt_ex on vp.DocID_Важность = tt_ex.DocID_Важность
	where (ISNULL(@Товары,'') = '' or res.DocID_Ресурс is not null)
	  AND (ISNULL(@Точки,'') = '' or tt.DocID_Важность is not null)
	  AND res_ex.DocID_Ресурс is null
	  AND tt_ex.DocID_Важность is null
	  AND ((Предприятие in ('ТП','ТД') or v.DocID_Важность = 15 /*Скл.Запад*/) AND (@Борк = 0 OR @ВозвратНаРЦ = 1)
		or (@ВозвратНаРЦ = 0 AND @Борк = 1 AND Предприятие = 'БОРК'))
	group by vp.DocID_Ресурс, vp.DocID_Важность, VP.Зонтик

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#ПоделеннаяПоставка + #Ресурс + #Важность',GETDATE()
	select @i = MAX(id) from @t
		
	IF OBJECT_ID('tempdb..#ПоделеннаяПоставка') is not null DROP TABLE #ПоделеннаяПоставка
	create table #ПоделеннаяПоставка (DocID_Ресурс int, ПодПост int)
	insert into #ПоделеннаяПоставка (DocID_Ресурс, ПодПост)
	select DocID_Ресурс
		  ,SUM(pp.ПодПост) as ПодПост
	from dbo.ПоделеннаяПоставка pp with(nolock) 
	group by DocID_Ресурс

	if OBJECT_ID ('tempdb..#Ресурс') is not null drop table #Ресурс
	select res.SAP_Имя1, res.SAP_Имя2, res.SAP_Имя3, res.SAP_Имя5
		  ,res.SAP_Производитель, res.Код_Товар
		  ,res.АльтИмя_Товар as [Наименование товара]
		  ,res.ExtID_Товар as ExtID_Ресурс
		  ,res.DocID as DocID_Ресурс
		  ,res.Матрица
		  ,res.КластерЦУ as КластерЦена, res.Цена
		  ,res.Матрица7
		  ,CASE WHEN mbTB.DocID_Ресурс IS NOT NULL THEN 1 ELSE 0 END as isMinTB
		  ,ISNULL(zh.ОжПост ,0) as ОжПост
		  ,res.SAP_ПроизводительГруппа
		  ,res.ПИК, res.АКС
		  ,res.SAP_ExtID
		  ,res.МЗ
		  ,res.GroupPath + CONVERT(VARCHAR,res.DocID) + '.' AS grPath
	into #Ресурс 
	from v_Ресурс res
	left join #subjTov st on res.DocID = st.DocID_Ресурс
	left join #subjTov_ex st_ex on res.DocID = st_ex.DocID_Ресурс
	left join #subBrand br on res.DocID_Производитель = br.DocID_Производитель
	left join #subBrand_ex br_ex on res.DocID_Производитель = br_ex.DocID_Производитель
	left join dbo.ОжидаемаяПоставка zh with(nolock) on res.DocID = zh.DocID_Ресурс
	left join dbo.ТоварыМинТБ mbTB with(nolock) on res.DocID = mbTB.DocID_Ресурс
	where (ISNULL(@Товары,'') = '' or st.DocID_Ресурс is not null)
	  AND (ISNULL(@Бренды,'') = '' or br.DocID_Производитель is not null)
	  AND st_ex.DocID_Ресурс is null
	  AND br_ex.DocID_Производитель is null
	  AND (@ВозвратНаРЦ = 1 OR @Борк = 0 OR res.isBork = 1)


	if OBJECT_ID ('tempdb..#Важность') is not null drop table #Важность
	select distinct v.DocID as DocID_Важность
	               ,v.Предприятие
				   ,v.ExtID
	into #Важность
	from v_Важность v
	left join #subjTT tt on v.DocID = tt.DocID_Важность
	left join #subjTT_ex tt_ex on v.DocID = tt_ex.DocID_Важность
	where ((Предприятие in ('ТП','ТД') or v.DocID = 15 /*Скл.Запад*/) AND (@Борк = 0 OR @ВозвратНаРЦ = 1)
		or (@ВозвратНаРЦ = 0 AND @Борк = 1 AND Предприятие = 'БОРК'))
	  AND (ISNULL(@Точки,'') = '' or tt.DocID_Важность is not null)
	  AND tt_ex.DocID_Важность is null

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#SalesDostCl + #Sales',GETDATE()
	select @i = MAX(id) from @t
	  
	IF OBJECT_ID('tempdb..#SalesDostCl') is not null DROP TABLE #SalesDostCl
	SELECT p.DocID_Ресурс				  
		  ,SUM(CASE WHEN DocID_Важность = 25 THEN Продажи28дн ELSE 0 END) as ПродажиМесяцДоставкаКлиенту
		  ,SUM(CASE WHEN DocID_Важность = 232 THEN Продажи28дн ELSE 0 END) as ПродажиМесяцИМБОРК
		  ,SUM(CASE WHEN DocID_Важность = 25 THEN СуммаЗала ELSE 0 END) as СумЗалДоставкаКлиенту
		  ,SUM(CASE WHEN DocID_Важность = 232 THEN СуммаЗала ELSE 0 END) as СумЗалИМБОРК
	INTO #SalesDostCl
	FROM dbo.Продажи28Дней_сводная p with(nolock)
	INNER JOIN #Ресурс res on p.DocID_Ресурс = res.DocID_Ресурс
	WHERE p.DocID_Важность in (25,232)
	GROUP BY p.DocID_Ресурс

	IF OBJECT_ID('tempdb..#Sales') is not null DROP TABLE #Sales
	SELECT DocID_Ресурс, Зал_1, Зал_2, Зал_3, Зал_4, ПродажиМесяцБезДоставки
	INTO #Sales
	FROM (  SELECT p.DocID_Ресурс
				  ,SUM(CASE WHEN Zon.DocID_Важность IS NOT NULL AND p.Зонтик <> p.DocID_Важность THEN 0 ELSE Зал_1нед END) as Зал_1
				  ,SUM(CASE WHEN Zon.DocID_Важность IS NOT NULL AND p.Зонтик <> p.DocID_Важность THEN 0 ELSE Зал_2нед END) as Зал_2
				  ,SUM(CASE WHEN Zon.DocID_Важность IS NOT NULL AND p.Зонтик <> p.DocID_Важность THEN 0 ELSE Зал_3нед END) as Зал_3
				  ,SUM(CASE WHEN Zon.DocID_Важность IS NOT NULL AND p.Зонтик <> p.DocID_Важность THEN 0 ELSE Зал_4нед END) as Зал_4
				  ,SUM(CASE WHEN p.DocID_Важность in (25,232) OR (Zon.DocID_Важность IS NOT NULL AND p.Зонтик <> p.DocID_Важность) THEN 0 ELSE Продажи28дн END) as ПродажиМесяцБезДоставки
			FROM dbo.Продажи28Дней_сводная p with(nolock)
			INNER JOIN #Ресурс res on p.DocID_Ресурс = res.DocID_Ресурс
			INNER JOIN #Важность v on p.DocID_Важность = v.DocID_Важность
			LEFT JOIN #Важность Zon ON p.Зонтик = Zon.DocID_Важность
			WHERE p.DocID_Важность <> 15 --Склад запад
			GROUP BY p.DocID_Ресурс
		 )A	

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#ostZap + #URC',GETDATE()
	select @i = MAX(id) from @t
	
	IF OBJECT_ID('tempdb..#ostZap') is not null DROP TABLE #ostZap
	SELECT o.DocID_Ресурс
	      ,CASE WHEN @Борк = 1 THEN Свободный_Остаток_BORK ELSE Свободный_Остаток END Свободный_Остаток
		  ,ОбщОст, RIMPlan
		  ,CONVERT(varchar(10),ОбщОст) as ОбщОст_Строка
	INTO #ostZap 
	FROM dbo.ОстаткиСклад o with(nolock)
	INNER JOIN #Ресурс res on o.DocID_Ресурс = res.DocID_Ресурс

	SELECT @Ноль_строка = RIGHT(REPLICATE(' ', t_NUM.mx_num) + '0', t_NUM.mx_num)
	FROM (SELECT MAX(LEN(ОбщОст_Строка)) as mx_num
		  FROM #ostZap
		 )t_NUM

	IF NULLIF(@Ноль_строка,'') IS NULL 
		SET @Ноль_строка = '0'

	UPDATE o
		SET ОбщОст_Строка = t.ОбщОст_Строка
	FROM #ostZap o
	INNER JOIN (
				SELECT DocID_Ресурс
					  ,RIGHT(REPLICATE(' ', t_NUM.mx_num) + ОбщОст_Строка, t_NUM.mx_num) as ОбщОст_Строка
				FROM #ostZap
				OUTER APPLY (
				            SELECT MAX(LEN(ОбщОст_Строка)) as mx_num
							FROM #ostZap
							)t_NUM
			   ) t ON o.DocID_Ресурс = t.DocID_Ресурс

	DROP TABLE IF EXISTS #URC
	SELECT DocID_Ресурс
		  ,SUM(УРЦ) AS УРЦ
	INTO #URC
	FROM dbo.Остатки
	WHERE УРЦ > 0
	GROUP BY DocID_Ресурс

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#КластерПродаж + #Продажи_4_мес_XI',GETDATE()
	select @i = MAX(id) from @t

	IF OBJECT_ID('tempdb..#КластерПродаж') is not null DROP TABLE #КластерПродаж
	SELECT SAP_ExtID, DocID_Важность, ЦУ
	INTO #КластерПродаж
	FROM КластерТТ_ПродажиЦУ with(nolock)
	WHERE Кластер IN (1,2,3,4)
	GROUP BY SAP_ExtID, DocID_Важность, ЦУ


	IF OBJECT_ID('tempdb..#Продажи_4_мес_XI') is not null DROP TABLE #Продажи_4_мес_XI
	create table #Продажи_4_мес_XI (DocID_Ресурс varchar(14) primary key, ПродМес_1 int, ПродМес_2 int, ПродМес_3 int, ПродМес_4 int)
	IF @ПоМес = 1
	BEGIN
		insert into #Продажи_4_мес_XI (DocID_Ресурс,ПродМес_1,ПродМес_2,ПродМес_3,ПродМес_4)
		select mz.DocID_Ресурс
			  ,SUM(mz.ПродМес_1) as ПродМес_1
			  ,SUM(mz.ПродМес_2) as ПродМес_2
			  ,SUM(mz.ПродМес_3) as ПродМес_3
			  ,SUM(mz.ПродМес_4) as ПродМес_4
		from Продажи6Месяцев_XI mz
		inner join #Важность v on mz.DocID_Важность = v.DocID_Важность
		inner join #Ресурс res on mz.DocID_Ресурс = res.DocID_Ресурс
		group by mz.DocID_Ресурс
	END

	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#СправочникЗамен + #ЕстьИзменения_с_учетом_Зонтика',GETDATE()
	select @i = MAX(id) from @t

	if OBJECT_ID('tempdb..#СправочникЗамен') is not null drop table #СправочникЗамен
	create table #СправочникЗамен (DocID_Важность int, DocID_ресурс int, isNew bit, ID int, DocID_Ресурс_аналог int, Источник varchar(50))
	insert into #СправочникЗамен (DocID_Важность, DocID_ресурс, isNew, ID, DocID_Ресурс_аналог,Источник)
	select DocID_Важность, DocID_ресурс, isNew, ID, DocID_Ресурс_аналог, Источник
	from (  select DocID_Важность, DocID_ресурс, isNew, ID, DocID_Ресурс_аналог, Источник, ROW_NUMBER() OVER (PARTITION BY DocID_Важность, DocID_ресурс Order by src, ID desc) rn
			from (  select DocID_Важность, DocID_Ресурс_нов as DocID_ресурс, 1 as isNew, ID, DocID_Ресурс as DocID_Ресурс_аналог, src, Источник
					from ОбщийСправочникЗаменТоваров
					union all
					select DocID_Важность, DocID_ресурс, 0 as isNew, ID, DocID_Ресурс_нов as DocID_Ресурс_аналог, src, Источник
					from ОбщийСправочникЗаменТоваров
				 )A
		 )A
	where rn = 1
 
 
	DROP TABLE IF EXISTS #ЕстьИзменения_с_учетом_Зонтика
 	SELECT ExtID_Важность
 		  ,ExtID_Ресурс
 		  ,SUM(Заказ) AS Заказ	
 	INTO #ЕстьИзменения_с_учетом_Зонтика	
 	FROM (
			SELECT z.ExtID_Важность
 				  ,z.ExtID_Ресурс
 				  ,z.Заказ 
 			FROM dbo.ЗаказТоварМагазин_Ручной z
 			INNER JOIN #Важность v on z.ExtID_Важность = v.ExtID
 			INNER JOIN #Ресурс res on z.ExtID_Ресурс = res.ExtID_Ресурс
 			UNION ALL
			SELECT   ExtID_ВажностьРЦ
 					,ExtID_Ресурс
 					,SUM(Заказ) AS Заказ
 			FROM (
					SELECT TOP 1 WITH TIES
 							v.ExtID_ВажностьРЦ
 							,o.ExtID_Ресурс
 							,ISNULL(o.Заказ,0) AS Заказ
 					FROM dbo.ЗаказТоварМагазин_Ручной o 
 					INNER JOIN v_Важность_НастройкиРазвозаПоГруппе v
 						ON v.ExtID_ВажностьТТ = o.ExtID_Важность
 					INNER JOIN elf.dbo.Ресурс res
 						ON res.ExtID = o.ExtID_Ресурс
 						AND res.GroupPath + CONVERT(VARCHAR(20), res.DocID) + '.' LIKE v.GroupPathDocID_РесурсГруппаТоваров+'.%'
 					INNER JOIN #Важность vv on v.ExtID_ВажностьРЦ = vv.ExtID
 					INNER JOIN #Ресурс res2 on o.ExtID_Ресурс = res2.ExtID_Ресурс
 					WHERE v.Тип > 0
 					ORDER BY ROW_NUMBER() OVER (PARTITION BY v.DocID_ВажностьРЦ, v.ExtID_ВажностьТТ, o.ExtID_Ресурс ORDER BY v.lev desc)
				)A
 					GROUP BY ExtID_ВажностьРЦ
 							,ExtID_Ресурс
 		)A
 	GROUP BY ExtID_Важность
 		    ,ExtID_Ресурс
 
 	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select '#ZontOst + #DeffZont',GETDATE()
	select @i = MAX(id) from @t

	DROP TABLE IF EXISTS #ZontOst
	select v.Зонтик
		  ,v.DocID_Ресурс
		  ,MAX(CASE WHEN @Борк = 1 THEN z.Свободный_Остаток_BORK ELSE z.Свободный_Остаток END) as СкладЗонт
		  --,SUM(CASE WHEN v.Кресты = 1 THEN v.ТЗ_Маг+ISNULL(i.ВПути,0) ELSE 0 END) as ТЗ_Маг_кресты
		  --kmm 2022 Если есть хоть один магазин в зонтике у кторого нет запрета на развоз + нет остатков + нет в пути
		  -- или если на всех магазинах внутри зонтика стоит запрет развоза - тогда помечаем колонку "З" = 1
		  ,  MIN(CASE WHEN    v.Кресты = 1 
		                  AND ISNULL(IIF(v.ТЗ_Маг + ISNULL(v2.ТЗ_Маг,0) < 0, 0, v.ТЗ_Маг + ISNULL(v2.ТЗ_Маг,0)),0) + ISNULL(i.ВПути,0) + ISNULL(i2.ВПути,0) <= 0 
						  AND (v.Заказ + ISNULL(v2.Заказ,0)) = 0 
					  THEN 0 
					  ELSE 1
				 END)
		     & 
			 --kmm 2022-08-26 Если у старинки стоят кресты 
			 --и при этом у новинки есть хоть один магазин в котором товар не заблокирован или еcть заказ или есть остаток в магазине или есть товар в пути
			 --тогда такой товар мы не помечаем
		     CASE WHEN MAX(ISNULL(v.Кресты,0)) = 0 AND MAX(CONVERT(INT,sz.isNew)) = 0 
			       AND (MAX(ISNULL(v2.Кресты,0)) = 1 OR MAX(ISNULL(v2.ТЗ_Маг,0) + ISNULL(v2.Заказ,0) + ISNULL(i2.ВПути,0)) > 0 ) THEN 1
			      WHEN MAX(ISNULL(v.Кресты,0)) = 0 THEN 0
				  ELSE 1
			 END as ТЗ_Маг_кресты
	into #ZontOst
	from v_ОсновнойАлгоритмРасчета v
	INNER JOIN #Ресурс res on v.DocID_Ресурс = res.DocID_Ресурс
	left join #InWay i on v.DocID_Важность = i.DocID_Важность
					  and v.DocID_Ресурс = i.DocID_Ресурс
	left join ОстаткиСкладЗонтик z
		on v.DocID_Важность = z.DocID_Важность
			and v.DocID_Ресурс = z.DocID_Ресурс
	left join #СправочникЗамен sz
		on sz.DocID_ресурс = v.DocID_Ресурс
		and sz.DocID_Важность = v.DocID_Важность
	left join v_ОсновнойАлгоритмРасчета v2
		on sz.DocID_Ресурс_аналог = v2.DocID_Ресурс
		and v.DocID_Важность = v2.DocID_Важность
	left join #InWay i2 on v2.DocID_Важность = i2.DocID_Важность
					  and v2.DocID_Ресурс = i2.DocID_Ресурс
	where v.Зонтик <> -1
	group by v.Зонтик
		    ,v.DocID_Ресурс
    
	--kmm 2022-12-06 Получаем тип дефицита в разрезе товара для зонтиков
	--цель, определить есть ли для товара хоть один зонтик у которого есть дефицит
	--если есть тогда выводим ! в УСД
	DROP TABLE IF EXISTS #DeffZont
	SELECT mt.DocID_Ресурс
	INTO #DeffZont
	FROM v_ОсновнойАлгоритмРасчета mt
	inner join #Ресурс res on mt.DocID_Ресурс = res.DocID_Ресурс
	WHERE Зонтик <> -1
	GROUP BY mt.DocID_Ресурс
	HAVING MAX(ТипДефицита) IN (2,3,4)


	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	insert into @t (step, dtBegin)
	select 'fin',GETDATE()
	select @i = MAX(id) from @t

		declare @SQL nvarchar(max) = N''
	       ,@Params nvarchar(max) = ''

	SET @Params = '  @ПоМес bit
				    ,@Точки varchar(max) 
					,@Борк bit
				    ,@ВозвратНаРЦ int
					,@БезРаспределения bit
					,@Автозаказ int
				    ,@ТипД int
					,@Ноль_строка varchar(10)'

	SET @SQL = N'
	SELECT res.SAP_Имя1, res.SAP_Имя2, res.SAP_Имя3, res.SAP_Имя5
	              ,res.SAP_Производитель
				  ,res.Код_Товар as Код
				  ,res.[Наименование товара] as Имя
				  ,res.ExtID_Ресурс
				  ,res.Матрица as Матр
				  ,res.КластерЦена as ЦУ
				  ,res.Цена
				  --,A.типД--ISNULL(A.типД, A.типД_Зонт) as типД
				  ,TypeDeficit.типД
				  ,res.ОжПост
				  ,ISNULL(pp.ПодПост,0) as ПодПост
				  ,ISNULL(z.Свободный_Остаток,0) as Склад
				  --,ISNULL(z.ОбщОст,0) as ОбщОст
				  ,CASE WHEN ISNULL(z.ОбщОст,0) = 0 AND urc.УРЦ > 0 THEN ''УРЦ''
				        ELSE ISNULL(z.ОбщОст_строка, @Ноль_строка)
				   END as ОбщОст
				  ,res.Матрица7
				  ,ISNULL(A.ОстМаг,0) as ОстМаг
				  ,ISNULL(i.ВПути,0) as [В пути]
				  ,A.КолТЦ
				  ,ISNULL(A.ОбщПотр,0) as ОбщПотр
				  ,A.Заказ
				  ,ISNULL(A.Мин,0) as Мин
				  ,ISNULL(A.Макс,0) as Макс
				  ,ISNULL(A.МаксД,0) as МаксД
				  ,ISNULL(A.Изл,0) as Изл
				  ,CASE WHEN @ПоМес = 1 THEN ISNULL(mz.ПродМес_4,0) ELSE ISNULL(s.Зал_4,0) END as Зал_4 --_4
				  ,CASE WHEN @ПоМес = 1 THEN ISNULL(mz.ПродМес_3,0) ELSE ISNULL(s.Зал_3,0) END as Зал_3 --_3
				  ,CASE WHEN @ПоМес = 1 THEN ISNULL(mz.ПродМес_2,0) ELSE ISNULL(s.Зал_2,0) END as Зал_2 --_2
				  ,CASE WHEN @ПоМес = 1 THEN ISNULL(mz.ПродМес_1,0) ELSE ISNULL(s.Зал_1,0) END as Зал_1 --_1
				  ,ISNULL(s.ПродажиМесяцБезДоставки,0) + CASE WHEN ISNULL(@Точки,'''') <> '''' 
				                                              THEN 0 
															  ELSE CASE WHEN @Борк = 1 THEN ISNULL(resIM.ИМ_Борк,0) ELSE ISNULL(resIM.ИМ_ТП,0) END
														 END as [28Дн]
				  ,A.Вз
				  ,A.ЕстьИзм
				  ,ЕстьО
				  ,СменаКреста
				  ,ISNULL(kv.Кластер,''6'') as ABC
				  ,id_СправочникЗамен
				  ,isNew_СправочникЗамен
				  ,Источник
				  --,CONVERT(INT,0) as Зонтик_Склад_ТТ
				  ,Зонтик_Склад_ТТ
				  --,СкладЗонтик
				  ,CASE WHEN tdz.DocID_Ресурс IS NOT NULL THEN ''!'' ELSE CONVERT(VARCHAR(1),'''') END as УСД --Удаленный Склад Дефицит (есть дефицит на удаленном складе (сейчас это ГрРостов))
	from (select res.DocID_Ресурс 
				  --,MAX(CASE WHEN mt.Предприятие IN (''ТП'',''ТД'') AND mt.Зонтик = -1  THEN mt.ТипДефицита END) as типД
				  --,MAX(CASE WHEN mt.Предприятие IN (''ТП'',''ТД'') AND mt.Зонтик <> -1 THEN mt.ТипДефицита END) as типД_Зонт
				  --------***************ЗАКАЗ****************-------
				  ,CASE WHEN @ВозвратНаРЦ = 0
								THEN SUM(CASE WHEN ISNULL(zt.БезРаспределения,0) = 0 AND @БезРаспределения = 1 THEN 0 
								              WHEN zt.Заказ is not null THEN zt.Заказ
											  WHEN ISNULL(Кресты,0) <> 1 AND (res.isMinTB = 0 OR v.Предприятие = ''БОРК'') THEN 0
											  WHEN Доступен_к_развозу IS NOT NULL AND Доступен_к_развозу <> 1 AND (res.isMinTB = 0 OR v.Предприятие = ''БОРК'') AND i.DocID_Важность is null THEN 0
											  WHEN mt.DocID_Важность = 15 THEN 0
											  ELSE mt.Заказ
										 END)
						--WHEN @ВозвратНаРЦ = 1
						--		THEN CASE WHEN SUM(ISNULL(vt.Вывоз,0)) <> 0 THEN -SUM(ISNULL(vt.Вывоз,0)) + SUM(CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ISNULL(vz.Вывоз,0) END)
						--				  ELSE SUM(CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ISNULL(vz.Вывоз,0) END)
						--			 END
						WHEN @ВозвратНаРЦ = 1
								THEN SUM(CASE WHEN ISNULL(vz.Вывоз,0) = 0 AND ISNULL(vt.Вывоз_ручной,0) = 0 AND ISNULL(Кресты,0) <> 1 AND (res.isMinTB = 0 OR v.Предприятие = ''БОРК'') THEN 0
										      WHEN ISNULL(vz.Вывоз,0) = 0 AND (ISNULL(vt.Вывоз,0)) <> 0 THEN -(ISNULL(vt.Вывоз,0)) + (CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ISNULL(vz.Вывоз,0) END)
										      ELSE (CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ISNULL(vz.Вывоз,0) END)
									     END)
				   END as Заказ
				   --------***************ЗАКАЗ****************-------
				  --,SUM(CASE WHEN mt.DocID_Важность = 15 OR ТЗ_Маг < 0 OR (ZonSkl.DocID_Важность IS NOT NULL AND mt.Зонтик_Склад = mt.DocID_Важность) THEN 0 ELSE ТЗ_Маг END) as ОстМаг
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 OR ТЗ_Маг < 0 OR z.Зонтик = mt.DocID_Важность THEN 0 ELSE ТЗ_Маг END) as ОстМаг
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE mt.Автозаказ END) as КолТЦ
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ПотребностьДоМаксимума END) as ОбщПотр
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 OR ISNULL(mt.Автозаказ,0) <> 1 THEN 0 ELSE Минимум END) as Мин
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 OR ISNULL(mt.Автозаказ,0) <> 1 THEN 0 ELSE Максимум END) as Макс
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 OR ISNULL(mt.Автозаказ,0) <> 1 THEN 0 ELSE [Максимум_при_дефиците] END) as МаксД
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 THEN 0 ELSE ISNULL(Излишек,0) END) as Изл
				  ,SUM(CASE WHEN mt.DocID_Важность = 15 OR ISNULL(mt.Автозаказ,0) <> 1 THEN 0 ELSE ISNULL(mt.[Витринный_Запас],0) END) as Вз
				  ,CASE WHEN @ВозвратНаРЦ = 0 THEN MAX(CASE WHEN ezz.Заказ IS NOT NULL THEN 1 ELSE 0 END)
					    WHEN @ВозвратНаРЦ = 1 THEN NULL
				   END as ЕстьИзм
				,MAX(CASE WHEN ISNULL(Кресты,0) = 1 AND Доступен_к_развозу IS NOT NULL AND Доступен_к_развозу <> 1 AND (res.isMinTB = 0 OR v.Предприятие = ''БОРК'')
				           AND kp.DocID_Важность IS NOT NULL AND v.DocID_Важность NOT IN (25,232)
						   AND i.DocID_Важность is null/*kmm 20210211 нет в пути*/ 
						   AND ISNULL(n.Автозаказ,1) <> 0
						   THEN 1 ELSE 0 END
				     ) as ЕстьО
				--,MAX(ISNULL(mt.ТЗ_Склад,0)) as ТЗ_Склад
				,MAX(CASE WHEN sm.DocID_Важность is null THEN 0
				          WHEN sm.DocID_Важность is not null AND ISNULL(ТЗ_Маг,0) = 0 AND ISNULL(s.Зал_1нед + s.Зал_2нед + s.Зал_3нед + s.Зал_4нед,0) = 0 AND ISNULL(i.ВПути,0) > 0 THEN 0
						  WHEN sm.DocID_Важность is not null AND ISNULL(ТЗ_Маг,0) = 0 AND ISNULL(s.Зал_1нед + s.Зал_2нед + s.Зал_3нед + s.Зал_4нед,0) = 0 THEN 1
						  ELSE 0
					 END) as СменаКреста
				--,MAX(CASE WHEN @Борк = 1 THEN OstSklZont.Свободный_Остаток_BORK ELSE OstSklZont.Свободный_Остаток END) СкладЗонтик
				,MAX(z.СкладЗонт) as СкладЗонтик
				--,MIN(CASE WHEN CASE WHEN @Борк = 1 THEN OstSklZont.Свободный_Остаток_BORK ELSE OstSklZont.Свободный_Остаток END > 0
				--		  THEN CASE WHEN Зонтик <> -1 AND Кресты = 1 THEN ТЗ_Маг ELSE 0 END > 0 
				--					THEN 0
				--					ELSE 1
				--			   END
				--		  ELSE 0
				--	 END) AS Зонтик_Склад_ТТ
			    ,MAX(CASE WHEN z.СкладЗонт > 0 AND ТЗ_Маг_кресты = 0 THEN 1 ELSE 0 END) as Зонтик_Склад_ТТ
				,MAX(sz.ID) as id_СправочникЗамен
				,ISNULL(MAX(CONVERT(INT,sz.isNew)),0) as isNew_СправочникЗамен
				,MAX(Источник) as Источник
			FROM t_ОсновнойАлгоритмРасчета mt
			FULL JOIN ЗаказТоварМагазин_Ручной zt with(nolock) on mt.ExtID_Важность = zt.ExtID_Важность
																	  AND mt.ExtID_Ресурс = zt.ExtID_Ресурс
			FULL JOIN dbo.ВывозТоварМагазин_Ручной vz with(nolock) on mt.ExtID_Важность = vz.ExtID_Важность
												           AND mt.ExtID_Ресурс = vz.ExtID_Ресурс
		    INNER JOIN #Важность v on COALESCE(mt.ExtID_Важность,zt.ExtID_Важность,vz.ExtID_Важность) = v.ExtID
			INNER JOIN #Ресурс res on COALESCE(mt.ExtID_Ресурс,zt.ExtID_Ресурс,vz.ExtID_Ресурс) = res.ExtID_Ресурс
			LEFT JOIN #ЕстьИзменения_с_учетом_Зонтика ezz on v.ExtID = ezz.ExtID_Важность
											   and res.ExtID_Ресурс = ezz.ExtID_Ресурс
			LEFT JOIN dbo.v_ВывозТоваров vt with(nolock) on mt.DocID_Важность = vt.DocID_Важность
												        AND mt.DocID_Ресурс = vt.DocID_Ресурс
			LEFT JOIN #КластерПродаж kp on v.DocID_Важность = kp.DocID_Важность
									   AND res.SAP_ExtID = kp.SAP_ExtID
									   AND res.КластерЦена = kp.ЦУ
			LEFT JOIN dbo.СменаКреста_на1 sm with(nolock) on res.DocID_Ресурс = sm.DocID_Ресурс
														 AND v.DocID_Важность = sm.DocID_Важность
														 AND sm.dt1 is not null
			LEFT JOIN #InWay i on res.DocID_Ресурс = i.DocID_Ресурс
							  AND v.DocID_Важность = i.DocID_Важность
			LEFT JOIN dbo.Продажи28Дней_сводная s with(nolock) on res.DocID_Ресурс = s.DocID_Ресурс
														      and v.DocID_Важность = s.DocID_Важность
			--LEFT JOIN #Важность ZonSkl on mt.Зонтик_Склад = ZonSkl.DocID_Важность
			--LEFT JOIN #TT_ZONT ttz on ttz.DocID_ВажностьРЦ = v.DocID
			--LEFT JOIN ОстаткиСкладЗонтик OstSklZont
			--	ON OstSklZont.DocID_Важность = v.DocID_Важность
			--	AND OstSklZont.DocID_Ресурс = res.DocID_Ресурс
			LEFT JOIN #ZontOst z
				ON (z.Зонтик = v.DocID_Важность OR z.Зонтик = mt.Зонтик)
				and z.DocID_Ресурс = res.DocID_Ресурс
			LEFT JOIN dbo.v_НастройкиТоварТТ n 
				ON res.DocID_Ресурс = n.DocID_Ресурс
			   AND v.DocID_Важность = n.DocID_Важность
			LEFT JOIN #СправочникЗамен sz on res.DocID_ресурс = sz.DocID_ресурс
										 and v.DocID_Важность = sz.DocID_Важность
			WHERE 1=1 '
		IF @Автозаказ <> 10 SET @SQL += N' AND mt.Автозаказ = @Автозаказ ' --WHERE (@Автозаказ = 10 or mt.Автозаказ = @Автозаказ)
		--IF @ТипД < 10 SET @SQL += N' AND CASE WHEN mt.ТипДефицита = 4 THEN 3 ELSE mt.ТипДефицита END = @ТипД ' --AND (@ТипД >= 10 or mt.ТипДефицита = @ТипД)
		IF @ВсеТовары = 13 SET @SQL += N' AND mt.НовыйТовар = 1 ' --AND (@ВсеТовары <> 13 OR mt.НовыйТовар = 1)
			
		SET @SQL += N'  
			GROUP BY res.DocID_Ресурс
		)A
	INNER JOIN #Ресурс res on A.DocID_Ресурс = res.DocID_Ресурс
	LEFT JOIN #ostZap z on A.DocID_Ресурс = z.DocID_Ресурс
	LEFT JOIN (select DocID_Ресурс, SUM(CASE WHEN v.DocID_Важность IS NOT NULL AND i.DocID_Важность <> i.Зонтик THEN 0 ELSE ВПути END) as ВПути
			   from #InWay i
			   left join #Важность v 
				on i.Зонтик = v.DocID_Важность
			   group by DocID_Ресурс
			  ) i on A.DocID_Ресурс = i.DocID_Ресурс
	LEFT JOIN #Sales s on A.DocID_Ресурс = s.DocID_Ресурс
	LEFT JOIN #ПоделеннаяПоставка pp on A.DocID_Ресурс = pp.DocID_Ресурс
	LEFT JOIN #Продажи_4_мес_XI mz on A.DocID_Ресурс = mz.DocID_Ресурс
	LEFT JOIN v_РесурсИМ_Борк_ТП resIM on A.DocID_Ресурс = resIM.DocID_Ресурс
	LEFT JOIN #URC URC ON A.DocID_Ресурс = URC.DocID_Ресурс
	left join dbo.КластерВыручка kv with(nolock) on A.DocID_Ресурс = kv.DocID_Ресурс
											    and (@Борк = 1 and kv.Предприятие = ''БОРК''
												  OR @Борк = 0 and kv.Предприятие = ''ТПТД'')
	LEFT JOIN (select v.DocID_Ресурс
					 ,MAX(v.ТипДефицита) as типД
					 --,MAX(CASE WHEN ТипДефицита = 4 THEN 3 ELSE ТипДефицита END) as типД
				from dbo.v_ОсновнойАлгоритмРасчета v
				LEFT JOIN XI_НастройкиТоварТТ_АкцияФишки af
					ON af.DocID_Ресурс = v.DocID_Ресурс
					AND af.DocID_Важность = v.DocID_Важность
					AND af.РучнойДефицит = 1
				where v.DocID_Важность <> 25
					AND v.Предприятие in (''ТП'',''ТД'')
					AND v.Зонтик = -1
					AND af.DocID_Важность IS NULL
				GROUP BY v.DocID_Ресурс) TypeDeficit
				ON TypeDeficit.DocID_Ресурс = res.DocID_Ресурс
	LEFT JOIN #DeffZont tdz ON tdz.DocID_Ресурс = res.DocID_Ресурс
	WHERE 1=1 '
	IF @ЕстьЗаказ = 1 SET @SQL += N' AND Заказ > 0 '
	IF @ЕстьЗаказ = 0 SET @SQL += N' AND Заказ = 0 ' --(@ЕстьЗаказ = 10 or (@ЕстьЗаказ = 1 AND Заказ > 0) or (@ЕстьЗаказ = 0 AND Заказ = 0) )
	IF @ТипД < 10 SET @SQL += N' AND CASE WHEN TypeDeficit.типД = 4 THEN 3 ELSE TypeDeficit.типД END = @ТипД ' --AND (@ТипД >= 10 or mt.ТипДефицита = @ТипД)
	IF @ТипД <> 11 SET @SQL += N' AND (ISNULL(z.ОбщОст,0) > 0 OR ОстМаг > 0 OR ОжПост > 0 OR ВПути > 0) ' --AND (@ТипД = 11 OR ISNULL(z.ОбщОст,0) > 0 OR ОстМаг > 0 OR ОжПост > 0 OR ВПути > 0)
	IF @ТипД = 11 SET @SQL += N' AND ((ISNULL(z.ОбщОст,0) > 0 AND ISNULL(z.Свободный_Остаток,0) <= 0 AND res.МЗ >= ISNULL(z.ОбщОст,0)) OR (ISNULL(z.ОбщОст,0) <= 0 AND ISNULL(z.Свободный_Остаток,0) <= 0 AND ISNULL(A.СкладЗонтик,0) > 0))' --AND (@ТипД <> 11 OR (ISNULL(z.ОбщОст,0) > 0 AND ISNULL(ТЗ_Склад,0) <= 0 AND res.МЗ >= ISNULL(z.ОбщОст,0)))
	IF @ВсеТовары = 1 SET @SQL += N' AND z.Свободный_Остаток > 0 ' --OR (@ВсеТовары = 1 AND z.Свободный_Остаток > 0)
	IF @ВсеТовары = 2 SET @SQL += N' AND Матрица7 = 1 ' --OR (@ВсеТовары = 2 AND Матрица7 = 1)
	IF @ВсеТовары = 3 SET @SQL += N' AND Матрица not in (''E'',''М'',''МБ'',''ММ'',''РР'',''P'',''M'') ' --OR (@ВсеТовары = 3 AND Матрица not in (''E'',''М'',''МБ'',''ММ'',''РР''))
	IF @ВсеТовары = 5 SET @SQL += N' AND Матрица in (''E'',''М'',''МБ'',''ММ'',''РР'',''P'',''M'') ' --OR (@ВсеТовары = 5 AND Матрица in (''E'',''М'',''МБ'',''ММ'',''РР''))
	IF @ВсеТовары = 6 SET @SQL += N' AND ПИК = 1 ' --OR (@ВсеТовары = 6 AND ПИК = 1)
	IF @ВсеТовары = 7 SET @SQL += N' AND z.Свободный_Остаток > 0 AND Матрица in (''E'',''М'',''МБ'',''ММ'',''РР'',''P'',''M'') '--OR (@ВсеТовары = 7 AND z.Свободный_Остаток > 0 AND Матрица in (''E'',''М'',''МБ'',''ММ'',''РР''))
	IF @ВсеТовары = 8 SET @SQL += N' AND АКС = 1 ' --OR (@ВсеТовары = 8 AND АКС = 1)
	IF @ВсеТовары = 9 SET @SQL += N' AND АКС = 0 ' --OR (@ВсеТовары = 9 AND АКС = 0)
	IF @ВсеТовары = 10 SET @SQL += N' AND Изл > 0 ' --OR (@ВсеТовары = 10 and Изл > 0)
	SET @SQL += N'	
	order by SAP_Имя5, SAP_Имя1, SAP_Имя2, SAP_Имя3, [Наименование товара]'

	--print SUBSTRING(@SQL,1,4000)
	--print SUBSTRING(@SQL,4001,8000)
	--print SUBSTRING(@SQL,8001,12000)
	exec sys.sp_executesql @SQL, @params
						  ,@ПоМес
						  ,@Точки
						  ,@Борк
						  ,@ВозвратНаРЦ
						  ,@БезРаспределения
						  ,@Автозаказ
						  ,@ТипД
						  ,@Ноль_строка


	update @t
		set dtend = GETDATE()
		   ,durationSec = datediff(second,dtbegin,getdate())
	where id = @i
	
	--if datediff(MINUTE,@dateB,GETDATE()) > 2
	if @IntoLog = 1
		insert into dbo._kmm_exec_log (proc_name, step, dtBegin, dtEnd, durationSec,[param])
		select 'rep_КонтрольРаспределения', step, dtBegin, dtEnd, durationSec, paramstr
		from @t

end
