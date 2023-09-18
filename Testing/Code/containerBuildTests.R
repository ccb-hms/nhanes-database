# containerBuildTests.R
# Tests to be run post-build on the NHANES db to verify completion and consistency in the data.

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
allTableNames = DBI::dbGetQuery(cn, "
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

# Stops build if the version format is wrong
if (is.na(grep(pattern, Sys.getenv("EPICONDUCTOR_CONTAINER_VERSION"), value = TRUE))) {
    stop(paste("Docker Container Environment Variable EPICONDUCTOR_CONTAINER_VERSION: ", Sys.getenv("EPICONDUCTOR_CONTAINER_VERSION")," not in format vX.X.X"), sep='')
}

# Stops build if the date is not in YYYY-MM-DD format
!is.na(as.Date(Sys.getenv("EPICONDUCTOR_COLLECTION_DATE"), format="%Y-%m-%d"))
if (is.na(as.Date(Sys.getenv("EPICONDUCTOR_COLLECTION_DATE"), format="%Y-%m-%d"))) {
    stop(paste("Docker Container Environment Variable EPICONDUCTOR_COLLECTION_DATE: ", Sys.getenv("EPICONDUCTOR_COLLECTION_DATE")," not in format YYYY-MM-DD"), sep='')
}

##################################################################################################################
# TEST: Metadata and Ontology Schema Tables have the required names and columns
# RESULT: Stops the build if there are mismatched columns in any of the metadata or ontology schema table
##################################################################################################################

mismatchedCols <- function(cols, tableName){
        query = setequal(cols, unlist(DBI::dbGetQuery(cn, paste("SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='",tableName,"' AND TABLE_CATALOG='NhanesLandingZone'", sep=''))))
        if(!query){
                    stop(paste("Mismatched Columns Found in", tableName), sep='')
        }
        else{return(invisible(NULL))}
}

DownloadErrors = c("DataType", "FileUrl", "Error")
mismatchedCols(DownloadErrors,"DownloadErrors")


QuestionnaireDescriptions = c("Description", "TableName", "BeginYear", "EndYear", "DataGroup", "UseConstraints", "DocFile", "DataFile", "DatePublished")
mismatchedCols(QuestionnaireDescriptions, "QuestionnaireDescriptions")


QuestionnaireVariables = c("Variable", "TableName", "Description", "Target", "SasLabel", "UseConstraints","ProcessedText","Tags","VariableID","OntologyMapped")
mismatchedCols(QuestionnaireVariables, "QuestionnaireVariables")


VariableCodebook = c("Variable", "TableName", "CodeOrValue", "ValueDescription", "Count", "Cumulative", "SkipToItem")
mismatchedCols(VariableCodebook, "VariableCodebook")


dbxrefs = c("Subject", "Object", "Ontology")
mismatchedCols(dbxrefs, "dbxrefs")


edges = c("Subject", "Object", "Ontology")
mismatchedCols(edges, "edges")


entailed_edges = c("Subject", "Object", "Ontology")
mismatchedCols(entailed_edges, "entailed_edges")


labels = c("Subject", "Object", "IRI", "DiseaseLocation", "Ontology", "Direct", "Inherited")
mismatchedCols(labels, "labels")


nhanes_variables_mappings = c("Variable", "TableName", "SourceTermID", "SourceTerm", "MappedTermLabel", "MappedTermCURIE", "MappedTermIRI", "MappingScore", "Tags", "Ontology")
mismatchedCols(nhanes_variables_mappings, "nhanes_variables_mappings")


##################################################################################################################
# TEST: All tables in RAW schema are present in QuestionnaireDescriptions (or ExcludedTables).
# RESULT: Returns RAW tables that do not have an entry in QuestionnaireDescriptions or ExcludedTables.
##################################################################################################################

questionnaireToRaw = "
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Raw'
                    AND TABLE_CATALOG='NhanesLandingZone'
                    AND TABLE_NAME NOT IN ( SELECT TableName FROM Metadata.QuestionnaireDescriptions )
                    AND TABLE_NAME NOT IN ( SELECT TableName FROM Metadata.ExcludedTables )
                    AND TABLE_NAME NOT LIKE '%P_%'
                    ORDER BY TABLE_NAME ASC
                    "

if (nrow(DBI::dbGetQuery(cn, questionnaireToRaw))>0) {
    print(paste("Tables found in RAW schema that do not exist in QuestionnaireDescriptions: ", DBI::dbGetQuery(cn, questionnaireToRaw)), sep='')
}
##################################################################################################################
# TEST: All tables in QuestionnaireDescriptions schema are present in the db
# RESULT: Returns QuestionnaireDescriptions tables that do not exist in RAW schema.
##################################################################################################################

rawToQuestionnaire = "
                    SELECT TableName FROM Metadata.QuestionnaireDescriptions
                    WHERE TableName NOT IN ( SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Raw' AND TABLE_CATALOG='NhanesLandingZone' )
                    AND TableName NOT IN ( SELECT TableName FROM Metadata.ExcludedTables )
                    AND TableName NOT LIKE '%P_%'
                    ORDER BY TableName ASC
                    "

DBI::dbGetQuery(cn, rawToQuestionnaire) # returns any tables found
if (nrow(DBI::dbGetQuery(cn, rawToQuestionnaire))>0) {
    stop(paste("Tables found in QuestionnaireDescriptions that do not exist in RAW schema: ", DBI::dbGetQuery(cn, rawToQuestionnaire)), sep='')
}
##################################################################################################################
# TEST: Raw and Translated tables have the same row counts
# RESULT: 
##################################################################################################################

for (i in 1:nrow(allTableNames)) {

    currTableName = allTableNames[i,"TABLE_NAME"]

    c1 = DBI::dbGetQuery(cn, paste0("SELECT COUNT(*) FROM Raw.", currTableName))
    c2 = DBI::dbGetQuery(cn, paste0("SELECT COUNT(*) FROM Translated.", currTableName))

    if (c1 != c2) {
        stop(paste0("Raw.", currTableName, " has ", c1, " and Translated.", currTableName, " has ", c2, " rows."), sep='')
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




if (nrow(DBI::dbGetQuery(cn, rawToTranslated))>0) {
    stop(paste("RAW tables not found in TRANSLATED schema: ", DBI::dbGetQuery(cn, rawToTranslated)), sep='')
}

##################################################################################################################
# TEST: All translated tables have same cols as raw
# RESULT: Returns any cols in 'raw' that don't appear in the 'translated' version for each table
##################################################################################################################

for (i in 1:nrow(allTableNames)) {

    currTableName = allTableNames[i,"TABLE_NAME"]
    
    missingCols = DBI::dbGetQuery(
        cn, 
        paste(
            sep="", 

            # The subquery needs to specify the catalog the same way the parent query does.
            "SELECT COLUMN_NAME, TABLE_NAME 
             FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_NAME = '", currTableName, "' 
             AND TABLE_SCHEMA = 'Raw' 
             AND TABLE_CATALOG='NhanesLandingZone' 
             AND COLUMN_NAME NOT IN ( SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '", currTableName, "' AND TABLE_CATALOG='NhanesLandingZone' AND TABLE_SCHEMA = 'Translated' )" 
        ))
    
    if (nrow(missingCols)>0) {
        stop(paste("TRANSLATED table ", currTableName, " missing columns defined in RAW version: ", missingCols), sep='')
    }
}
