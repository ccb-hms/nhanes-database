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
                        WHERE TABLE_CATALOG='NhanesLandingZone'
                        AND TABLE_SCHEMA = 'Raw'
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


mismatchedCols <- function(cols, tableName){
        query = setequal(cols, unlist(DBI::dbGetQuery(cn, paste("SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='",tableName,"' AND TABLE_CATALOG='NhanesLandingZone'", sep=''))))
        if(!query){
                    result = paste("Mismatched Columns Found in", tableName)
                    return(result)
        }
        else{return(invisible(NULL))}
}

DownloadErrors = c("DataType", "FileUrl", "Error")
mismatchedCols(DownloadErrors,"DownloadErrors")

QuestionnaireDescriptions = c("Description", "TableName", "BeginYear", "EndYear", "DataGroup", "UseConstraints")
mismatchedCols(QuestionnaireDescriptions, "QuestionnaireDescriptions")

QuestionnaireVariables = c("Variable", "TableName", "Description", "Target", "SasLabel", "UseConstraints")
mismatchedCols(QuestionnaireVariables, "QuestionnaireVariables")

VariableCodebook = c("Variable", "TableName", "CodeOrValue", "ValueDescription", "Count", "Cumulative", "SkipToItem")
mismatchedCols(VariableCodebook, "VariableCodebook")

dbxrefs = c("Subject", "Object", "Ontology")
mismatchedCols(dbxrefs, "dbxrefs")

edges = c("Subject", "Object", "Ontology")
mismatchedCols(edges, "edges")

entailed_edges = c("Subject", "Object", "Ontology")
mismatchedCols(entailed_edges, "entailed_edges")

labels = c("Subject", "Object", "IRI", "Ontology", "Direct", "Inherited")
mismatchedCols(labels, "labels")

nhanes_variables_mappings = c("Variable", "TableName", "SourceTermID", "SourceTerm", "MappedTermLabel", "MappedTermCURIE", "MappedTermIRI", "MappingScore", "Tags", "Ontology")
mismatchedCols(nhanes_variables_mappings, "nhanes_variables_mappings")

##################################################################################################################
# TEST: All tables in questionnaire descriptions are present in the db
# RESULT: Returns any tables in QuestionnaireDescriptions not found in 'Raw' schema, should be empty result otherwise
##################################################################################################################

questionnaireToRaw = "
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Raw'
                    AND TABLE_CATALOG='NhanesLandingZone'
                    AND TABLE_NAME NOT IN ( SELECT TableName FROM Metadata.QuestionnaireDescriptions )
                    AND TABLE_NAME NOT IN ( SELECT TableName FROM Metadata.ExcludedTables )
                    AND TABLE_NAME NOT LIKE '%P_%'
                    ORDER BY TABLE_NAME ASC
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
            "SELECT DISTINCT(COLUMN_NAME), TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='", currTableName, "'", " AND TABLE_CATALOG='NhanesLandingZone' AND TABLE_SCHEMA = 'Raw'")
        )
    
    # check if the table has a SEQN column. If it does, proceed to the row count test
    if (sum(str_detect(tableCols$COLUMN_NAME, "SEQN")) > 0){}

        # Compare the row counts between the Raw and Translated versions of the table. If they are not equal, keep going to compare each column against SEQN individually
        rowCountQuery = paste(sep="", "SELECT CASE WHEN (select count(*) from [NhanesLandingZone].[Raw].[", currTableName, "])=(select count(*) from [NhanesLandingZone].[Translated].[", currTableName, "]) THEN 1 ELSE 0 END AS RowCountResult")

        rowCountResult = DBI::dbGetQuery(cn, rowCountQuery)[,]

        if (rowCountResult != 1){
            for (j in 1:nrow(tableCols)) {

                columnName = tableCols[j, "COLUMN_NAME"]
                
                # compare the columns in both tables, they should be both equal, never one null and the other not
                compareColumns = paste(sep="", "SELECT R.SEQN, T.SEQN, R.",columnName,", T.",columnName," FROM [RAW].[",currTableName,"] R INNER JOIN Translated.",currTableName," T ON R.SEQN = T.SEQN WHERE (R.",columnName," IS NOT NULL AND T.",columnName," IS NULL) OR (R.",columnName," IS NULL AND T.",columnName," IS NOT NULL)")
                
                compareColumnsResult = DBI::dbGetQuery(cn, compareColumns)[,]
            
                }
        }
    }


##################################################################################################################
# TEST: All raw tables have been translated
# RESULT: Returns any table names in the raw schema not found in the translated schema, should be empty otherwise
##################################################################################################################

rawToTranslated = "
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'Raw' 
                    AND TABLE_CATALOG='NhanesLandingZone'
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
            "SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '", currTableName, "' AND TABLE_SCHEMA = 'Raw' AND TABLE_CATALOG='NhanesLandingZone' AND COLUMN_NAME NOT IN ( SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '", currTableName, "' AND TABLE_SCHEMA = 'Translated' )" 
        ))
}
