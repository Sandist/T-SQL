
ALTER proc [seneca].[clear_remains_prices]
AS 
	--kmm 2023-01-16 удаляем из таблиц remains и prices помеченные на удаление строки старше @ActivePeriod
BEGIN

	DECLARE @ActivePeriod datetime = DATEADD(DAY,-30,CONVERT(DATE,GETDATE()))

	DROP TABLE IF EXISTS #del_prices
	SELECT docid
	INTO #del_prices
	FROM itbase.seneca.prices
	WHERE deleted = 1
	  and ts < @ActivePeriod

	DROP TABLE IF EXISTS #del_remains
	SELECT id
	INTO #del_remains
	FROM itbase.seneca.remains
	WHERE deleted = 1
	  and ts < @ActivePeriod

	SELECT 1;
	WHILE @@rowcount > 0
		DELETE TOP (50000)
		FROM p
		FROM itbase.seneca.prices p --WITH (ROWLOCK)
		INNER JOIN #del_prices d	
			ON p.docid = d.docid

	SELECT 1;
	WHILE @@rowcount > 0
		DELETE TOP (50000)
		FROM r
		FROM itbase.seneca.remains r --WITH (ROWLOCK)
		INNER JOIN #del_remains d
			ON r.id = d.id


END
