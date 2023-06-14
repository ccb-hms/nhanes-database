    /*
        Stored procedure to translate variable responses in an NHANES questionnaire table
    */

CREATE PROC spTranslateTable 
    @SourceTableSchema varchar(8000),
    @SourceTableName varchar(128),
    @DestinationTableSchema varchar(8000),
    @DestinationTableName varchar(128)
AS

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

    -- create comma delimited list of columns to be selected from the source table
    DECLARE @SourceSelectColNames varchar(8000)
    SELECT @SourceSelectColNames=STRING_AGG('[' + COLUMN_NAME + ']', ', ') 
    FROM #tmpColNames

    -- PRINT @SourceSelectColNames

    -- create comma delimited list of columns to be unpivoted
    DECLARE @UnpivotColNames varchar(8000)
    SELECT @UnpivotColNames=STRING_AGG('[' + COLUMN_NAME + ']', ', ') 
    FROM #tmpColNames
    WHERE 
        COLUMN_NAME != 'SEQN'
        AND COLUMN_NAME != 'DownloadUrl'
        AND COLUMN_NAME != 'Questionnaire'
        AND COLUMN_NAME != 'Description'

    -- PRINT @UnpivotColNames

    -- assemble dynamic SQL to unpivot the original table
    DECLARE @unpivotStmt varchar(8000)
    SET @unpivotStmt = '
        SELECT SEQN, Variable, Response 
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
    DECLARE @TranslateStmt varchar(8000)
    SET @TranslateStmt = '
        SELECT 
            T.SEQN,
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
    DECLARE @PivotStmt varchar(8000)
    SET @PivotStmt = '
        SELECT * INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' FROM (
            SELECT 
                SEQN, 
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
        INSERT INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' (SEQN)
        SELECT A.SEQN FROM ' + @SourceTableSchema + '.' + @SourceTableName + ' A LEFT OUTER JOIN ' + @DestinationTableSchema + '.' + @DestinationTableName + ' T ON A.SEQN = T.SEQN WHERE T.SEQN IS NULL
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
