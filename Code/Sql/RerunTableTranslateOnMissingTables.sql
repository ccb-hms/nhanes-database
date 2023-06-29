/*
    This is a debugging tool to help identify tables that fail to translate and re-run 
    their translations once a bugfix has been proposed.
*/

DROP TABLE IF EXISTS #tmpRawTables
DROP TABLE IF EXISTS #tmpTranslatedTables

-- loop over all missing tables in the last release
 SELECT TABLE_NAME
 INTO #tmpRawTables
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'Raw'

 SELECT TABLE_NAME
 INTO #tmpTranslatedTables
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'Translated'

DROP TABLE IF EXISTS #tmpDebugTables
SELECT R.TABLE_NAME INTO #tmpDebugTables FROM #tmpRawTables R LEFT OUTER JOIN #tmpTranslatedTables T ON R.TABLE_NAME = T.TABLE_NAME WHERE T.TABLE_NAME IS NULL ORDER BY R.TABLE_NAME

SELECT * FROM #tmpDebugTables

DECLARE cTableNames CURSOR FOR SELECT * FROM #tmpDebugTables ORDER BY TABLE_NAME
DECLARE @currTableName varchar(256)
OPEN cTableNames

FETCH NEXT FROM cTableNames INTO @currTableName
WHILE @@FETCH_STATUS = 0  
BEGIN
    PRINT @currTableName
    EXECUTE dbo.spTranslateTable
        'Raw'
        ,@currTableName
        ,'Translated'
        ,@currTableName
    FETCH NEXT FROM cTableNames INTO @currTableName
END

CLOSE cTableNames
DEALLOCATE cTableNames