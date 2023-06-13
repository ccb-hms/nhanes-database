# Load the NHANES variable codebooks available here:
# git clone https://github.com/ccb-hms/NHANES-metadata.git
# cd NHANES-metadata/
# git checkout tags/1.2.0
#
# run this from the root of the NHANES-metadata repository

library(glue)
library(stringr)

# this is the location of the combined output file
codebookFile = paste(sep = "/", getwd(), "metadata/nhanes_variables_codebooks.tsv")
tablesFile = paste(sep = "/", getwd(), "metadata/nhanes_tables.tsv")
variablesFile = paste(sep = "/", getwd(), "metadata/nhanes_variables.tsv")
ontologyMappings = paste(sep = "/", getwd(), "ontology-mappings/")
ontologyTables = paste(sep = "/", getwd(), "ontology-tables/")

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

# load the codebook

# create the VariableCodebook table in SQL
SqlTools::dbSendUpdate(cn, "
    CREATE TABLE NhanesLandingZone.Metadata.VariableCodebook (
        Variable varchar(64),
        Questionnaire varchar(64),
        CodeOrValue varchar(64),
        ValueDescription varchar(256),
        Count int,
        Cumulative int,
        SkipToItem varchar(64)
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    BULK INSERT NhanesLandingZone.Metadata.VariableCodebook FROM '", codebookFile, "'
    WITH (FORMAT=CSV, KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR='\t')
")

SqlTools::dbSendUpdate(cn, insertStatement)

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")

# load the table descriptions

# create the nhanes_tables table in SQL
SqlTools::dbSendUpdate(cn, "
    CREATE TABLE ##tmp_nhanes_tables (
        [Table] varchar(64),
        TableName varchar(1024),
        DataGroup varchar(64),
        Year int
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    BULK INSERT ##tmp_nhanes_tables FROM '", tablesFile, "'
    WITH (FORMAT=CSV, KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR='\t')
")

SqlTools::dbSendUpdate(cn, insertStatement)

# clean up and insert in new table with consistent nomenclature
SqlTools::dbSendUpdate(cn, "
    SELECT 
        T.TableName AS Description,
        T.DataGroup,
        Q.TableName,
        T.Year
    INTO NhanesLandingZone.Metadata.QuestionnaireDescriptions
    FROM 
        ##tmp_nhanes_tables T 
        INNER JOIN NhanesLandingZone.Metadata.QuestionnaireVariables Q ON
            T.[Table] = Q.TableName
    GROUP BY
        T.TableName,
        T.DataGroup,
        Q.TableName,
        T.Year
")

SqlTools::dbSendUpdate(cn, "DROP TABLE ##tmp_nhanes_tables")

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")


# load the variable descriptions

# create the nhanes_tables table in SQL
SqlTools::dbSendUpdate(cn, "
    CREATE TABLE ##tmp_nhanes_variables (
        Variable varchar(64),
        [Table] varchar(64),
        SasLabel varchar(64),
        EnglishText varchar(1024),
        Target varchar(128)
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    BULK INSERT ##tmp_nhanes_variables FROM '", variablesFile, "'
    WITH (FORMAT=CSV, KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR='\t')
")

SqlTools::dbSendUpdate(cn, insertStatement)

# add columns to QuestionnaireVariables table to accommodate additional data
SqlTools::dbSendUpdate(cn, "
    ALTER TABLE NhanesLandingZone.Metadata.QuestionnaireVariables 
    ADD 
        Description varchar(1024) NULL, 
        Target varchar(128) NULL,
        SasLabel varchar(64)
")

# update the new columns in the NhanesLandingZone.dbo.QuestionnaireVariables
# with values from the imported table
SqlTools::dbSendUpdate(cn, "
    UPDATE Q
    SET 
        Q.Description = V.EnglishText,
        Q.Target = V.Target,
        Q.SasLabel = V.SasLabel
    FROM 
        NhanesLandingZone.Metadata.QuestionnaireVariables Q
        INNER JOIN ##tmp_nhanes_variables V ON
            Q.TableName = V.[Table]
            AND Q.Variable = V.Variable
")

SqlTools::dbSendUpdate(cn, "DROP TABLE ##tmp_nhanes_variables")

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

SqlTools::dbSendUpdate(cn, "UPDATE [NhanesLandingZone].[Metadata].[QuestionnaireVariables] SET Description =  'Respondent sequence number', SasLabel =  'Respondent sequence number' WHERE Variable = 'SEQN'")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")

#--------------------------------------------------------------------------------------------------------
# Add Ontology Tables
#--------------------------------------------------------------------------------------------------------
ontology_tables <- list.files(ontologyTables)

for (currTable in ontology_tables) {
    if (currTable != "README.md") {
    path = ontologyTables
    loaded_data <- read.csv(file = paste0(path, currTable), sep = "\t")
    
    sqlTableName = gsub(str_extract(currTable, '.*(?=\\.tsv)'), pattern = 'ontology_', replace = "", fixed = TRUE)
    
    # generate SQL table definitions from column types in tibbles
    createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), paste(sep=".", "Ontology", sqlTableName), loaded_data) # nolint

    # change TEXT to VARCHAR(256)
    createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(512)", fixed = TRUE) # nolint # nolint

    # change DOUBLE to float
    createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)
    
    # remove double quotes, which interferes with the schema specification
    createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)

    # create the table in SQL
    SqlTools::dbSendUpdate(cn, createTableQuery)

    # run bulk insert
    insertStatement = paste(sep="",
                            "BULK INSERT Ontology.",
                            sqlTableName,
                            " FROM '",
                            paste0(path, currTable),
                            "' WITH (FORMAT=CSV, KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
    )
    SqlTools::dbSendUpdate(cn, insertStatement)

  # keep memory as clean as possible
  rm(loaded_data)
  gc()
 }
}

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")
# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")

# --------------------------------------------------------------------------------------------------------
# Add Ontology Mappings
# --------------------------------------------------------------------------------------------------------
ontology_mappings <- list.files(ontologyMappings)

for (currTable in ontology_mappings) {
    
    # TODO: we really only want to load this single table?  then why loop?
    if (currTable == "nhanes_variables_mappings.tsv") {
        path = ontologyMappings
        loaded_data <- read.csv(file = paste0(path, currTable), sep = "\t")
        
        colnames(loaded_data)[which(colnames(loaded_data) == "Table")] <- "TableName"

        # generate SQL table definitions from column types in tibbles
        createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), paste("Ontology", str_extract(currTable, '.*(?=\\.tsv)'), sep="."), loaded_data) # nolint

        # change TEXT to VARCHAR(256)
        createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(512)", fixed = TRUE) # nolint # nolint

        # change DOUBLE to VARCHAR(256)
        createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" VARCHAR(512)", fixed = TRUE)
        
        # remove double quotes, which interferes with the schema specification
        createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)
        
        # create the table in SQL
        SqlTools::dbSendUpdate(cn, createTableQuery)

        # run bulk insert
        insertStatement = paste(sep="",
                                "BULK INSERT Ontology.",
                                str_extract(currTable, '.*(?=\\.tsv)'),
                                " FROM '",
                                paste0(path, currTable),
                                "' WITH (FORMAT=CSV, KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
        )
        SqlTools::dbSendUpdate(cn, insertStatement)

    # keep memory as clean as possible
    rm(loaded_data)
    gc()
 }
}

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")
