-- Manually run spTranslateTable

DECLARE @RC int
DECLARE @SourceTableSchema varchar(8000)
DECLARE @SourceTableName varchar(128)
DECLARE @DestinationTableSchema varchar(8000)
DECLARE @DestinationTableName varchar(128)

-- TODO: Set parameter values here.
-- CAFE_G
SET @SourceTableSchema  = 'Raw'
SET @SourceTableName  = 'AUX_I'
SET @DestinationTableSchema  = 'Translated'

EXECUTE @RC = [dbo].[spTranslateTable] 
   @SourceTableSchema
  ,@SourceTableName
  ,@DestinationTableSchema
  ,@DestinationTableName = @SourceTableName

EXEC ('SELECT * FROM ' + @DestinationTableSchema + '.' + @SourceTableName)  