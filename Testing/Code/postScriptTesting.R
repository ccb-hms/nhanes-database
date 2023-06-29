# postScriptTesting.R
# Tests to be run post-build on the NHANES db to verify completion and consistency in the data.

library(glue)
library(stringr)

# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "sa"
sqlPassword = "yourStrong(!)Password"
sqlDefaultDb = "NhanesLandingZone"

# loop waiting for SQL Server database to become available
for (i in 1:60) {
    cn = tryCatch(
        # connect to SQL
        MsSqlTools::connectMsSqlSqlLogin(
            server = sqlHost, 
            user = sqlUserName, 
            password = sqlPassword, 
            database = sqlDefaultDb
        ), warning = function(e) {
            return(NA)
        }, error = function(e) {
            return(NA)
        }
    )
    
    suppressWarnings({
         if (is.na(cn)) {
            Sys.sleep(10)
        } else {
            break
        }
    })
   
}

suppressWarnings({
    if (is.na(cn)) {
        stop("could not connect to SQL Server")
    }
})

# get a list of all base tables
m = DBI::dbGetQuery(cn, "
                        SELECT DISTINCT(TABLE_NAME)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE 
                            TABLE_TYPE = 'BASE TABLE' 
                            AND TABLE_CATALOG='NhanesLandingZone'
                            AND TABLE_NAME != 'QuestionnaireVariables'
                            AND TABLE_NAME != 'DownloadErrors'
                            AND TABLE_NAME != 'VariableCodebook'
                            AND TABLE_NAME != 'QuestionnaireDescriptions'
                            AND TABLE_NAME != 'dbxrefs'
                            AND TABLE_NAME != 'entailed_edges'
                            AND TABLE_NAME != 'edges'
                            AND TABLE_NAME != 'labels'
                            AND TABLE_NAME != 'nhanes_variables_mappings'
                            AND TABLE_NAME != 'ExcludedTables'
                        ORDER BY TABLE_NAME ASC
                        ")


##################################################################################################################
# TEST: The following variables should be present and in the correct format
# RESULT: All of the tests should return TRUE, FALSE reflects a mismatched variable
##################################################################################################################

# regular expression pattern for version
pattern <- "^v(0|[1-9]|[1-9][0-9]|100)\\.(0|[1-9]|[1-9][0-9]|100)\\.(0|[1-9]|[1-9][0-9]|100)$"

# returns false if the version format is wrong
!is.na(grep(pattern, Sys.getenv("EPICONDUCTOR_CONTAINER_VERSION"), value = TRUE))

# Returns false if the date is not in YYYY-MM-DD format
!is.na(as.Date(Sys.getenv("EPICONDUCTOR_COLLECTION_DATE"), format="%Y-%m-%d"))


##################################################################################################################
# TEST: Metadata and Ontology Schema Tables have the required names and columns
# RESULT: All of the following setequal lines should return TRUE. A FALSE return means there are mismatched columns
##################################################################################################################


mismatchedCols <- function(query, tableName){
        if(!query){
                    result = paste("Mismatched Columns Found in", tableName)
                    return(result)
        }
        else{return(invisible(NULL))}
}

DownloadErrors = c("DataType", "FileUrl", "Error")
mismatchedCols(setequal(DownloadErrors, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='DownloadErrors'"))), "[NhanesLandingZone].[Metadata].[DownloadErrors]")

QuestionnaireDescriptions = c("Description", "DataGroup", "TableName", "Year")
mismatchedCols(setequal(QuestionnaireDescriptions, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QuestionnaireDescriptions'"))), "[NhanesLandingZone].[Metadata].[QuestionnaireDescriptions]")

QuestionnaireVariables = c("Variable", "TableName", "Description", "Target", "SasLabel")
mismatchedCols(setequal(QuestionnaireVariables, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QuestionnaireVariables'"))), "[NhanesLandingZone].[Metadata].[QuestionnaireVariables]")

VariableCodebook = c("Variable", "TableName", "CodeOrValue", "ValueDescription", "Count", "Cumulative", "SkipToItem")
mismatchedCols(setequal(VariableCodebook, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='VariableCodebook'"))), "[NhanesLandingZone].[Metadata].[VariableCodebook]")

dbxrefs = c("Subject", "Object", "Ontology")
mismatchedCols(setequal(dbxrefs, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='dbxrefs'"))), "[NhanesLandingZone].[Ontology].[dbxrefs]")

edges = c("Subject", "Object", "Ontology")
mismatchedCols(setequal(edges, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='edges'"))), "[NhanesLandingZone].[Ontology].[edges]")

entailed_edges = c("Subject", "Object", "Ontology")
mismatchedCols(setequal(entailed_edges, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='entailed_edges'"))), "[NhanesLandingZone].[Ontology].[entailed_edges]")

labels = c("Subject", "Object", "IRI", "Ontology", "Direct", "Inherited")
mismatchedCols(setequal(labels, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='labels'"))), "[NhanesLandingZone].[Ontology].[labels]")

nhanes_variables_mappings = c("Variable", "TableName", "SourceTermID", "SourceTerm", "MappedTermLabel", "MappedTermCURIE", "MappedTermIRI", "MappingScore", "Tags", "Ontology")
mismatchedCols(setequal(nhanes_variables_mappings, unlist(DBI::dbGetQuery(cn, "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='nhanes_variables_mappings'"))), "[NhanesLandingZone].[Ontology].[nhanes_variables_mappings]")


##################################################################################################################
# TEST: All tables in questionnaire descriptions are present in the db
# RESULT: Returns any tables in QuestionnaireDescriptions not found in 'Raw' schema
##################################################################################################################

questionnaireToRaw = "
                    SELECT TableName
                    FROM Metadata.QuestionnaireDescriptions
                    WHERE TableName NOT IN ( SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Raw' )
                    ORDER BY TableName ASC
                    "

SqlTools::dbSendUpdate(cn, questionnaireToRaw) # returns an integer of result len


##################################################################################################################
# TEST: all tables have < 10 % null values
# RESULT: Returns a dataframe with the tables and columns that have >10% null values
##################################################################################################################

# create an empty dataframe
df <- setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("TableColumn", "NullPercent"))

# loop through each table in the 'Raw' schema
for (i in 1:nrow(m)) {
    
    currTableName = m[i,"TABLE_NAME"]
    
    # get a list of columns for the current table
    tableCols = DBI::dbGetQuery(
        cn, 
        paste(
            sep="", 
            "SELECT DISTINCT(COLUMN_NAME), TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='", currTableName, "'", " AND TABLE_SCHEMA = 'Raw'")
        )
    
    nullPercentQuery = "SELECT "
    
    # loop through each column name
    for (j in 1:nrow(tableCols)) {
        
        columnName = tableCols[j, "COLUMN_NAME"]
        
        # appends a select statement to nullPercentQuery, building a single query to return a table
        # that contains each column and it's null percentage
        nullPercentQuery = paste(sep="",nullPercentQuery, "100.0 * SUM(CASE WHEN [", columnName, "] IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS ", currTableName, "_", columnName, "_NullPercent,")
    
    }
    # removes the last comma from the query
    nullPercentQuery = str_sub(nullPercentQuery, end = -2)
    
    # adds the target table to the query
    nullPercentQuery = paste(sep="",nullPercentQuery, " FROM [NhanesLandingZone].[Raw].[",currTableName,"]")
    
    nullPercentColumns = DBI::dbGetQuery(cn, nullPercentQuery)
    
    for (k in 1:length(nullPercentColumns)) {
        
        nullVal = nullPercentColumns[,k]
        if (nullVal>10) {df[nrow(df) + 1,] <- list(names(nullPercentColumns[k]), nullPercentColumns[k])}
    }    

}


##################################################################################################################
# TEST: All raw tables have been translated
# RESULT: Returns any table names in the raw schema not found in the translated schema
##################################################################################################################

rawToTranslated = "
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'Raw' 
                    AND TABLE_NAME NOT IN ( SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Translated' )
                    ORDER BY TABLE_NAME ASC
                    "

DBI::dbGetQuery(cn, rawToTranslated)


##################################################################################################################
# TEST: All translated tables have same cols as raw
# RESULT: Returns any cols in 'raw' that don't appear in the 'translated' version for each table
##################################################################################################################

for (i in 1:nrow(m)) {

    currTableName = m[i,"TABLE_NAME"]
    
    missingCols = DBI::dbGetQuery(
        cn, 
        paste(
            sep="", 
            "SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '", currTableName, "' AND TABLE_SCHEMA = 'Raw' AND COLUMN_NAME NOT IN ( SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '", currTableName, "' AND TABLE_SCHEMA = 'Translated' )" 
        )

    # what to do when we find the missing columns?
}
