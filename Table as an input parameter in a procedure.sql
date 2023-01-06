if OBJECT_ID('TestProc') is not null drop procedure TestProc;
if OBJECT_ID('tempdb..#tmp') is not null drop table #tmp
if TYPE_ID(N'TableType') is not null drop type TableType

--создаем тип таблица
create type TableType as table (id int) ;
go --Важный момент, сначала нужно отправить на исполнение пакет с созданием типа, иначе в одном пакете при объявлении табличной переменной тип еще существовать не будет

--Создаем процедуру и описываем параметр с нашим типом и добавляем readonly - можно только читать данные
create procedure TestProc 
	@tbl TableType readonly
as
begin	
	select count(*) as cnt
	from @tbl
end;
go

--Наша основная таблица (для меня это имитация источника данных)
create table #tmp (id int)
insert into #tmp (id)
select t
from (values (1),(2),(3),(4),(5),(6)) A(t);

--Далее создаем табличную переменную типа TableType и заполняем ее данными из источника (в реальности это скорее всего будет запрос)
declare @t TableType
insert into @t (id)
select id
from #tmp

exec TestProc @t
