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
    SELECT COLUMN_NAME, ORDINAL_POSITION
    INTO #tmpColNames
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE 
        TABLE_NAME = @SourceTableName 
        AND TABLE_SCHEMA = @SourceTableSchema
    
    -- debugging
    -- SELECT * FROM #tmpColNames

    -- enumerate possible primary key column names
    DROP TABLE IF EXISTS #tmpPkColNames
    CREATE TABLE #tmpPkColNames (ColumnName varchar(8000), Priority int)
    INSERT INTO #tmpPkColNames VALUES ('SEQN', 1)
    INSERT INTO #tmpPkColNames VALUES ('SAMPLEID', 2)
    INSERT INTO #tmpPkColNames VALUES ('DRXFDCD', 3)
    INSERT INTO #tmpPkColNames VALUES ('DRXMC', 4)
    INSERT INTO #tmpPkColNames VALUES ('POOLID', 5)

    -- figure out which primary key column this table has
    DECLARE @pkColName varchar(MAX)
    SELECT TOP 1 @pkColName = P.ColumnName FROM #tmpColNames C INNER JOIN #tmpPkColNames P ON C.COLUMN_NAME = P.ColumnName ORDER BY P.Priority

    -- debugging
    -- PRINT @pkColName

    -- Figure out which columns should remain numeric and not get translated
    DROP TABLE IF EXISTS #tmpNumericColumns

    SELECT C.COLUMN_NAME, ORDINAL_POSITION
    INTO #tmpNumericColumns
    FROM 
        #tmpColNames C 
        INNER JOIN Metadata.VariableCodebook V ON
            V.TableName = @SourceTableName
            AND C.COLUMN_NAME = V.Variable
            AND V.ValueDescription = 'Range of Values'
    WHERE C.COLUMN_NAME != @pkColName
    GROUP BY C.COLUMN_NAME, ORDINAL_POSITION

    -- debugging
    -- SELECT * FROM #tmpNumericColumns

    -- remove the numeric column names from the table that contains all columns to be translated
    DELETE FROM #tmpColNames WHERE COLUMN_NAME IN (SELECT COLUMN_NAME FROM #tmpNumericColumns)

    -- check whether there are any categorical columns left
    DECLARE @categoricalVariableCount INT
    SELECT @categoricalVariableCount = COUNT(*) FROM #tmpColNames WHERE COLUMN_NAME != @pkColName

    -- debugging
    -- PRINT 'number categorical cols left:'
    -- PRINT @categoricalVariableCount
    
    -- if there are no translatable variables for this table, just copy it over
    IF @categoricalVariableCount = 0
        BEGIN
            EXEC('
                DROP TABLE IF EXISTS ' + @DestinationTableSchema + '.' + @DestinationTableName + '
                SELECT * INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' FROM ' + @SourceTableSchema + '.' + @SourceTableName + '
                CREATE CLUSTERED COLUMNSTORE INDEX idxSeqn ON ' + @DestinationTableSchema + '.' + @DestinationTableName)
            
            -- we can't do any of the steps below, so just return
            RETURN
        END

     -- create comma delimited list of the numeric columns, will be used later to retrieve them from the source table
    DECLARE @NumericSelectColNames varchar(MAX)
    SELECT 
        @NumericSelectColNames = STRING_AGG(CAST('[' + COLUMN_NAME + ']' AS varchar(MAX)), ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM #tmpNumericColumns

    -- debugging
    -- PRINT 'Numeric cols:'
    -- PRINT @NumericSelectColNames
    
    -- create comma delimited list of columns to be selected from the source table, including casting to varchar
    DECLARE @SourceSelectColNames varchar(MAX)
    SELECT 
        @SourceSelectColNames = STRING_AGG(CAST('CAST([' + COLUMN_NAME + '] AS varchar(MAX)) AS [' + COLUMN_NAME + ']' AS varchar(MAX)), ', ')
    FROM #tmpColNames
    WHERE COLUMN_NAME != @pkColName

    SET @SourceSelectColNames = CAST('[' AS varchar(MAX)) + @pkColName + CAST('], ' AS varchar(MAX)) + @SourceSelectColNames

    -- debugging
    -- PRINT 'source select col names:' 
    -- PRINT @SourceSelectColNames

    -- create comma delimited list of columns to be unpivoted, excluding primary key column
    DECLARE @UnpivotColNames varchar(MAX)
    SELECT @UnpivotColNames=STRING_AGG(CAST('[' + COLUMN_NAME + ']' AS varchar(MAX)), ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM #tmpColNames
    WHERE 
        COLUMN_NAME != @pkColName

    -- debugging
    -- PRINT 'unpivot col names:'
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

    -- debugging
    -- PRINT @unpivotStmt
    EXEC ('DROP TABLE IF EXISTS ' + @UnpivotTempTableName)
    EXEC(@unpivotStmt)

    -- assemble SQL to join the unpivoted table to the variable codebook
    -- to decode the responses
    -- Defining VARCHAR(256) resolves issue #79 in Github, where varchars are being cut off in the translated tables
    DECLARE @TranslateStmt varchar(MAX)
    SET @TranslateStmt = '
        SELECT 
            T.' + @pkColName + ',
            T.Variable,
            COALESCE(CAST(V.ValueDescription AS VARCHAR(256)), CAST(T.Response AS VARCHAR(256))) AS ValueDescription
        INTO 
            ' + @TranslatedTempTableName + '
        FROM 
            ' + @UnpivotTempTableName + ' T 
            LEFT OUTER JOIN Metadata.VariableCodebook V ON 
                T.Variable = V.Variable 
                AND CAST(T.Response AS VARCHAR(256)) = CAST(V.CodeOrValue AS VARCHAR(256))
                AND V.TableName = ''' + @SourceTableName + '''        
    '
    
    -- debugging
    -- PRINT @TranslateStmt
    EXEC (@TranslateStmt)

    -- assemble SQL to pivot the translated table back into the original schema
    DECLARE @PivotStmt varchar(MAX)

    -- if there are numeric columns that need to be merged with the translated categotical columns
    IF @NumericSelectColNames IS NOT NULL
        BEGIN
            -- build the query to include a join against the source table
            SET @PivotStmt = '
                WITH PivotTable AS(
                    SELECT * 
                    FROM (
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
                )
                -- full outer join to accommodate scenarios where the categorical variables were all NULL
                SELECT COALESCE(P.' + @pkColName + ', S.' + @pkColName + ') AS ' + @pkColName + REPLACE(', ' + @UnpivotColNames, ', [', ', P.[') +
                REPLACE(', ' + @NumericSelectColNames, ', [', ', S.[') + ' INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' 
                FROM PivotTable P FULL OUTER JOIN ' + @SourceTableSchema + '.' + @SourceTableName + ' S ON S.[' + @pkColName + '] = P.[' + @pkColName + ']
            '
        END
    ELSE
        BEGIN
            -- since we have no columns to merge from the original source table, just pivot the 
            -- translated variables and insert into the destination table
            SET @PivotStmt = '
                WITH PivotTable AS(
                    SELECT * 
                    FROM (
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
                )
                SELECT P.* INTO ' + @DestinationTableSchema + '.' + @DestinationTableName + ' 
                FROM PivotTable P
            '
        END

    -- debugging
    -- PRINT 'Pivot statement:'
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
    SET @IndexStmt = 'CREATE CLUSTERED COLUMNSTORE INDEX ccix ON ' + @DestinationTableSchema + '.' + @DestinationTableName
    EXEC(@IndexStmt)

    -- debugging
    -- EXEC('SELECT TOP 5 * FROM ' + @DestinationTableSchema + '.' + @DestinationTableName + ' ORDER BY SEQN')
    -- EXEC('SELECT TOP 5 * FROM ' + @SourceTableSchema + '.' + @SourceTableName + ' ORDER BY SEQN')

    DECLARE @GarbageCollectionStmt varchar(8000)
    SET @GarbageCollectionStmt = 'DROP TABLE IF EXISTS ' + @UnpivotTempTableName
    EXEC(@GarbageCollectionStmt)
    SET @GarbageCollectionStmt = 'DROP TABLE IF EXISTS ' + @TranslatedTempTableName
    EXEC(@GarbageCollectionStmt)
    
    CHECKPOINT