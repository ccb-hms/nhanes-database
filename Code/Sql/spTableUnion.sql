DROP PROCEDURE IF EXISTS spTableUnion
GO

DROP TYPE IF EXISTS TableList
GO

CREATE TYPE TableList AS TABLE (TableName varchar(128));
GO

CREATE PROCEDURE spTableUnion
    @TableList TableList READONLY,
    @OutputTableName varchar(128)
AS
    DECLARE @nTables INT
    SELECT @nTables = COUNT(*) FROM @TableList

    IF @nTables < 2 THROW 999999, 'This function requires at least two rows that name tables to be merged in the @TableList parameter', 1

    DECLARE @TableName varchar(128)

    DECLARE cTables CURSOR FOR 
    SELECT TableName FROM @TableList ORDER BY TableName
    OPEN cTables

    FETCH NEXT FROM cTables INTO @TableName

    -- start by copying the first table into the list into the output table
    EXEC('SELECT * INTO ' + @OutputTableName + ' FROM ' + @TableName )

    FETCH NEXT FROM cTables INTO @TableName

    -- now iterate for all remaining tables
    WHILE @@FETCH_STATUS = 0  
    BEGIN 
        PRINT @TableName
        EXEC('SELECT * FROM ' + @OutputTableName)
        EXEC('SELECT * FROM ' + @TableName)
        EXEC spPairwiseTableUnion @OutputTableName, @TableName, @OutputTableName
        FETCH NEXT FROM cTables INTO @TableName

        EXEC('SELECT * FROM ' + @OutputTableName)
    END 

    CLOSE cTables
    DEALLOCATE cTables

GO