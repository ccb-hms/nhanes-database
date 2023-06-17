DROP PROCEDURE IF EXISTS spPairwiseTableUnion
GO

CREATE PROCEDURE spPairwiseTableUnion 
    @T1Name varchar(128),
    @T2Name varchar(128),
    @OutputTableName varchar(128),
    @MissingValue varchar(128) = NULL
AS
    SET NOCOUNT ON

    IF OBJECT_ID(N’tempdb..#temptablename’) IS NOT NULL
    BEGIN
            -- TODO: temp tables do not show up in INFORMATION_SCHEMA
    END

    SELECT TABLE_NAME, COLUMN_NAME INTO #tmpAllColumns1
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_CATALOG = 'NhanesLandingZone' AND TABLE_NAME = @T1Name

    SELECT TABLE_NAME, COLUMN_NAME INTO #tmpAllColumns2
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_CATALOG = 'NhanesLandingZone' AND TABLE_NAME = @T2Name

    DROP TABLE IF EXISTS #tmpUnionTableSchema
    SELECT 
        COALESCE(TC1.COLUMN_NAME, TC2.COLUMN_NAME) AS COLUMN_NAME,
        TC1.TABLE_NAME AS T1TName, TC1.COLUMN_NAME AS T1CName,
        TC2.TABLE_NAME AS T2TName, TC2.COLUMN_NAME AS T2CName
        INTO #tmpUnionTableSchema
    FROM #tmpAllColumns1 TC1
        FULL OUTER JOIN #tmpAllColumns2 TC2 ON
            TC1.COLUMN_NAME = TC2.COLUMN_NAME
    ORDER BY COALESCE(TC1.COLUMN_NAME, TC2.COLUMN_NAME)

    SELECT * FROM #tmpUnionTableSchema

    -- build up queries for union
    DECLARE @Query1 varchar(8000)
    DECLARE @Query2 varchar(8000)
    SET @Query1 = 'SELECT '
    SET @Query2 = 'SELECT '

    DECLARE @ColumnName varchar(128)
    DECLARE @T1CName varchar(128)
    DECLARE @T2Cname varchar(128)

    DECLARE cColumnDefinition CURSOR FOR   
    SELECT COLUMN_NAME, T1CName, T2CName FROM #tmpUnionTableSchema ORDER BY COLUMN_NAME
    OPEN cColumnDefinition  
    
    FETCH NEXT FROM cColumnDefinition   
    INTO @ColumnName, @T1CName, @T2CName
    
    WHILE @@FETCH_STATUS = 0  
    BEGIN  

        SET @Query1 = @Query1 + CASE WHEN @T1CName IS NULL THEN 'NULL AS ' + @T2CName ELSE @T1CName END
        SET @Query2 = @Query2 + CASE WHEN @T2CName IS NULL THEN 'NULL AS ' + @T1CName ELSE @T2CName END

        FETCH NEXT FROM cColumnDefinition   
        INTO @ColumnName, @T1CName, @T2CName

        IF @@FETCH_STATUS = 0 SET @Query1 = @Query1 + ', '
        IF @@FETCH_STATUS = 0 SET @Query2 = @Query2 + ', '
    END

    CLOSE cColumnDefinition
    DEALLOCATE cColumnDefinition

    SET @Query1 = @Query1 + ' INTO #tmp FROM ' + @T1Name
    SET @Query2 = @Query2 + ' FROM ' + @T2Name
    EXEC(@Query1 + ' UNION ' + @Query2 + '; DROP TABLE IF EXISTS ' + @OutputTableName + '; SELECT * INTO ' + @OutputTableName + ' FROM #tmp')

GO