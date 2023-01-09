SELECT * 
FROM OpenQuery(magicbox, 'set fmtonly off; EXEC ITBASE.dbo.srvPORTALDB1_Sklad_Catalog
WITH RESULT SETS
(
  (
    Имя varchar(1000), Альтимя varchar(1000), Код varchar(20), extid varchar(14), РесурсКуда int, КоличКуда decimal(10,2)
  )
)
')
