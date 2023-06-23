/*
    Stored procedure to translate variable responses in an NHANES questionnaire table
*/
CREATE PROC spTranslateTable 
    @SourceTableSchema varchar(MAX),
    @SourceTableName varchar(MAX),
    @DestinationTableSchema varchar(MAX),
    @DestinationTableName varchar(MAX)
AS

    -- check that the variable codebook actually has data for this table
    DECLARE @variableTranslationCount INT
    SELECT @variableTranslationCount = COUNT(*) FROM Metadata.VariableCodebook C WHERE C.TableName = @SourceTableName
    
    -- if there are no translatable variables for this table, just copy it over
    IF @variableTranslationCount = 0
        BEGIN
        
            PRINT 'There are no variables available to translate for ' + @SourceTableSchema + '.' + @SourceTableName + '.  Copying table as-is.'
            DECLARE @CopyTableStatement varchar(MAX)
            SET @CopyTableStatement = '
                DROP TABLE IF EXISTS ' + @DestinationTableSchema + '.' + @DestinationTableName + '
                SELECT * INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' FROM ' + @SourceTableSchema + '.' + @SourceTableName + '
                CREATE CLUSTERED COLUMNSTORE INDEX idxSeqn ON ' + @DestinationTableSchema + '.' + @DestinationTableName
        
            EXEC(@CopyTableStatement)
            -- we can't do any of the steps below, so just return
            RETURN
        END
    
    -- drop the destination table if it exists
    DECLARE @DropDestinationStmt varchar(8000)
    SET @DropDestinationStmt = 'DROP TABLE IF EXISTS ' + @DestinationTableSchema + '.' + @DestinationTableName
    EXEC(@DropDestinationStmt)

    -- get globally unique names for global temp table
    DECLARE @UnpivotTempTableName varchar(8000)
    SET @UnpivotTempTableName = '##' + REPLACE(CAST(NEWID() AS varchar(256)), '-', '_')

    -- get globally unique names for global temp table
    DECLARE @TranslatedTempTableName varchar(8000)
    SET @TranslatedTempTableName = '##' + REPLACE(CAST(NEWID() AS varchar(256)), '-', '_')

    -- get all column names of the source table from the information schema
    DROP TABLE IF EXISTS #tmpColNames
    SELECT COLUMN_NAME
    INTO #tmpColNames
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @SourceTableName AND TABLE_SCHEMA = @SourceTableSchema
    
    -- figure out which primary key column this table has
    DROP TABLE IF EXISTS #tmpPkColNames
    CREATE TABLE #tmpPkColNames (ColumnName varchar(8000), Priority int)

    INSERT INTO #tmpPkColNames VALUES ('SEQN', 1)
    INSERT INTO #tmpPkColNames VALUES ('SAMPLEID', 2)
    INSERT INTO #tmpPkColNames VALUES ('DRXFDCD', 3)
    INSERT INTO #tmpPkColNames VALUES ('DRXMC', 4)
    INSERT INTO #tmpPkColNames VALUES ('POOLID', 4)
    
    DECLARE @pkColName varchar(256)
    SELECT TOP 1 @pkColName = P.ColumnName FROM #tmpColNames C INNER JOIN #tmpPkColNames P ON C.COLUMN_NAME = P.ColumnName ORDER BY P.Priority

    PRINT @pkColName

    -- create comma delimited list of columns to be selected from the source table, including casting to varchar
    DECLARE @SourceSelectColNames varchar(MAX)
    SELECT @SourceSelectColNames = STRING_AGG(CAST('CAST([' + COLUMN_NAME + '] AS varchar(MAX)) AS [' + COLUMN_NAME + ']' AS varchar(MAX)), ', ')
    FROM #tmpColNames

    -- PRINT @SourceSelectColNames

    -- create comma delimited list of columns to be unpivoted, excluding primary key columns and old metadata
    DECLARE @UnpivotColNames varchar(MAX)
    SELECT @UnpivotColNames=STRING_AGG(CAST('[' + COLUMN_NAME + ']' AS varchar(MAX)), ', ') 
    FROM #tmpColNames
    WHERE 
        -- primary key column names
        -- TODO: copy them out of the #tmpPkColNames so we only need to update constants in one place
        COLUMN_NAME != 'SEQN'
        AND COLUMN_NAME != 'SAMPLEID'
        AND COLUMN_NAME != 'DRXFDCD'
        AND COLUMN_NAME != 'DRXMC'
        AND COLUMN_NAME != 'POOLID'
        -- metadata column names
        -- TODO: remove these and test
        AND COLUMN_NAME != 'DownloadUrl'
        AND COLUMN_NAME != 'Questionnaire'
        AND COLUMN_NAME != 'Description'

    -- PRINT @UnpivotColNames

    -- assemble dynamic SQL to unpivot the original table
    DECLARE @unpivotStmt varchar(MAX)
    SET @unpivotStmt = '
        SELECT ' + @pkColName + ', Variable, Response 
        INTO ' + @UnpivotTempTableName + '
        FROM (
            SELECT 
                ' + @SourceSelectColNames + '
            FROM ' + @SourceTableSchema + '.' + @SourceTableName + '
        ) SourceTable
        UNPIVOT (
            Response FOR Variable IN (
                ' + @UnpivotColNames + '
            )
        ) AS unpvt'

    -- PRINT @unpivotStmt
    EXEC ('DROP TABLE IF EXISTS ' + @UnpivotTempTableName)
    EXEC(@unpivotStmt)

    -- assemble SQL to join the unpivoted table to the variable codebook
    -- to decode the responses
    DECLARE @TranslateStmt varchar(MAX)
    SET @TranslateStmt = '
        SELECT 
            T.' + @pkColName + ',
            T.Variable,
            COALESCE(CAST(V.ValueDescription AS VARCHAR), CAST(T.Response AS VARCHAR)) AS ValueDescription
        INTO 
            ' + @TranslatedTempTableName + '
        FROM 
            ' + @UnpivotTempTableName + ' T 
            LEFT OUTER JOIN Metadata.VariableCodebook V ON 
                T.Variable = V.Variable 
                AND CAST(T.Response AS VARCHAR) = CAST(V.CodeOrValue AS VARCHAR)
                AND V.TableName = ''' + @SourceTableName + '''        
    '
    -- PRINT @TranslateStmt
    EXEC (@TranslateStmt)

    -- assemble SQL to pivot the translated table back into the original schema
    DECLARE @PivotStmt varchar(MAX)
    SET @PivotStmt = '
        SELECT * INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' FROM (
            SELECT 
                ' + @pkColName + ', 
                Variable, 
                ValueDescription 
            FROM ' + @TranslatedTempTableName + '
        ) AS SourceTable
        PIVOT (
            MAX(ValueDescription)
            FOR Variable IN (
                    ' + @UnpivotColNames + '
                ) 
        ) AS PivotTable
    '

    -- PRINT @PivotStmt
    EXEC(@PivotStmt) 

    -- insert rows where all of the variable responses were NULL in the original data
    DECLARE @InsertNullStmt varchar(8000)
    SET @InsertNullStmt = '
        INSERT INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' (' + @pkColName + ')
        SELECT A.' + @pkColName + ' FROM ' + @SourceTableSchema + '.' + @SourceTableName + ' A LEFT OUTER JOIN ' + @DestinationTableSchema + '.' + @DestinationTableName + ' T ON A.' + @pkColName + ' = T.' + @pkColName + ' WHERE T.' + @pkColName + ' IS NULL
    '

    -- PRINT @InsertNullStmt
    EXEC(@InsertNullStmt)

    -- create a clustered index on the destination table with compression
    DECLARE @IndexStmt varchar(8000)
    SET @IndexStmt = 'CREATE CLUSTERED COLUMNSTORE INDEX idxSeqn ON ' + @DestinationTableSchema + '.' + @DestinationTableName

    --EXEC('SELECT * FROM ' + @DestinationTableName)

    DECLARE @GarbageCollectionStmt varchar(8000)
    SET @GarbageCollectionStmt = 'DROP TABLE IF EXISTS ' + @UnpivotTempTableName
    EXEC(@GarbageCollectionStmt)
    SET @GarbageCollectionStmt = 'DROP TABLE IF EXISTS ' + @TranslatedTempTableName
    EXEC(@GarbageCollectionStmt)
