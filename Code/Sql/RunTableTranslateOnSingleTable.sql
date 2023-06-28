-- Manually run spTranslateTable for debugging

DECLARE @RC int
DECLARE @SourceTableSchema varchar(8000)
DECLARE @SourceTableName varchar(128)
DECLARE @DestinationTableSchema varchar(8000)
DECLARE @DestinationTableName varchar(128)

-- TODO: Set parameter values here.
-- CAFE_G
SET @SourceTableSchema  = 'Raw'
SET @SourceTableName  = 'POOLTF_G'
SET @DestinationTableSchema  = 'Translated'

EXECUTE @RC = [dbo].[spTranslateTable] 
   @SourceTableSchema
  ,@SourceTableName
  ,@DestinationTableSchema
  ,@DestinationTableName = @SourceTableName

EXEC ('SELECT TOP 10 * FROM ' + @DestinationTableSchema + '.' + @SourceTableName + ' ORDER BY SEQN')
EXEC ('SELECT TOP 10 * FROM ' + @SourceTableSchema + '.' + @SourceTableName + ' ORDER BY SEQN')

-- SELECT * FROM Metadata.VariableCodebook V WHERE V.TableName='POOLTF_G'