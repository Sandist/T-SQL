ALTER PROCEDURE [dbo].[HTTPS_GetTEXTfromURL] 
      @url VARCHAR(4000),
	  @Token varchar(1000) = '',
      @txt VARCHAR(MAX) OUTPUT

AS
BEGIN      
    SET TEXTSIZE 2147483647; --1024;

    SET NOCOUNT ON;      
    DECLARE @Obj int 
    DECLARE @Result int 
    DECLARE @HTTPStatus int 
    DECLARE @ErrorMsg varchar(MAX)

    EXEC @Result = sp_OACreate 'MSXML2.ServerXMLHTTP', @Obj OUT 
 
    EXEC @Result = sp_OAMethod @Obj, 'open', NULL, 'GET', @url, false
	IF NULLIF(@Token,'') IS NOT NULL
		EXEC @Result = sp_OAMethod @Obj, 'setRequestHeader', null, 'token', @Token
    EXEC @Result = sp_OAMethod @Obj, 'setRequestHeader', NULL, 'Content-Type', 'application/x-www-form-urlencoded'
    EXEC @Result = sp_OAMethod @Obj, send, NULL, ''
    EXEC @Result = sp_OAGetProperty @Obj, 'status', @HTTPStatus OUT 
 
    declare @Tab table (x VARCHAR(MAX))
    insert into @tab
    EXEC @Result = sp_OAGetProperty @Obj, 'responseText' 
    
    set @txt = (Select x from @tab)
	
	-- Close the connection.
	EXEC @Result = sp_OADestroy @Obj;

END
