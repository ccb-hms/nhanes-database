# Load the NHANES variable codebooks available here:
# git clone https://github.com/ccb-hms/NHANES-metadata.git
# cd NHANES-metadata/
# git checkout tags/1.2.0
#
# run this from the root of the NHANES-metadata repository

persistTextFiles = FALSE
library(glue)
library(stringr)

# this is the location of the combined output file
codebookFile = paste(sep = "/", getwd(), "metadata/nhanes_variables_codebooks.csv")
tablesFile = paste(sep = "/", getwd(), "metadata/nhanes_tables.csv")
variablesFile = paste(sep = "/", getwd(), "metadata/nhanes_variables.csv")
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
    CREATE TABLE NhanesLandingZone.dbo.VariableCodebook (
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
    BULK INSERT NhanesLandingZone.dbo.VariableCodebook FROM '", codebookFile, "'
    WITH (FORMAT='CSV', KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR=',')
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
    WITH (FORMAT='CSV', KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR=',')
")

SqlTools::dbSendUpdate(cn, insertStatement)

# clean up and insert in new table with consistent nomenclature
SqlTools::dbSendUpdate(cn, "
    SELECT 
        Q.Questionnaire, 
        T.TableName AS Description,
        T.DataGroup,
        Q.BeginYear,
        Q.EndYear,
        Q.TableName
    INTO NhanesLandingZone.dbo.QuestionnaireDescriptions
    FROM 
        ##tmp_nhanes_tables T 
        INNER JOIN NhanesLandingZone.dbo.QuestionnaireVariables Q ON
            T.[Table] = Q.Questionnaire
    GROUP BY
        Q.Questionnaire, 
        T.TableName,
        T.DataGroup,
        Q.BeginYear,
        Q.EndYear,
        Q.TableName
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
    WITH (FORMAT='CSV', KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR=',')
")

SqlTools::dbSendUpdate(cn, insertStatement)

# add columns to QuestionnaireVariables table to accommodate additional data
SqlTools::dbSendUpdate(cn, "
    ALTER TABLE NhanesLandingZone.dbo.QuestionnaireVariables 
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
        NhanesLandingZone.dbo.QuestionnaireVariables Q
        INNER JOIN ##tmp_nhanes_variables V ON
            Q.Questionnaire = V.[Table]
            AND Q.Variable = V.Variable

    SELECT * FROM NhanesLandingZone.dbo.QuestionnaireVariables
")

SqlTools::dbSendUpdate(cn, "DROP TABLE ##tmp_nhanes_variables")

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

SqlTools::dbSendUpdate(cn, "UPDATE [NhanesLandingZone].[dbo].[QuestionnaireVariables] SET Description =  'Respondent sequence number', SasLabel =  'Respondent sequence number' WHERE Variable = 'SEQN'")

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
    # generate SQL table definitions from column types in tibbles
    createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), str_extract(currTable, '.*(?=\\.tsv)'), loaded_data) # nolint

    # change TEXT to VARCHAR(256)
    createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(512)", fixed = TRUE) # nolint # nolint

    # change DOUBLE to float
    createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

    # create the table in SQL
    SqlTools::dbSendUpdate(cn, createTableQuery)

    # run bulk insert
    insertStatement = paste(sep="",
                            "BULK INSERT ",
                            str_extract(currTable, '.*(?=\\.tsv)'),
                            " FROM '",
                            paste0(path, currTable),
                            "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
    )
    SqlTools::dbSendUpdate(cn, insertStatement)

    # if we don't want to keep the derived text files, then delete to save disk space
    if (!persistTextFiles) {
      file.remove(paste0(path, currTable))
    }

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
    if (currTable == "nhanes_variables_mappings.tsv") {
    path = ontologyMappings
    loaded_data <- read.csv(file = paste0(path, currTable), sep = "\t")

    # generate SQL table definitions from column types in tibbles
    createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), str_extract(currTable, '.*(?=\\.tsv)'), loaded_data) # nolint

    # change TEXT to VARCHAR(256)
    createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(512)", fixed = TRUE) # nolint # nolint

    # change DOUBLE to VARCHAR(256)
    createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" VARCHAR(512)", fixed = TRUE)

    # create the table in SQL
    SqlTools::dbSendUpdate(cn, createTableQuery)

    # run bulk insert
    insertStatement = paste(sep="",
                            "BULK INSERT ",
                            str_extract(currTable, '.*(?=\\.tsv)'),
                            " FROM '",
                            paste0(path, currTable),
                            "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
    )
    SqlTools::dbSendUpdate(cn, insertStatement)

    # if we don't want to keep the derived text files, then delete to save disk space
    if (!persistTextFiles) {
      file.remove(paste0(path, currTable))
    }

  # keep memory as clean as possible
  rm(loaded_data)
  gc()
 }
}

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")

m = DBI::dbGetQuery(cn, "
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_CATALOG='NhanesLandingZone'
    AND TABLE_NAME != 'QuestionnaireVariables'
    AND TABLE_NAME != 'DownloadErrors'
    AND TABLE_NAME != 'VariableCodebook'
    AND TABLE_NAME != 'QuestionnaireDescriptions'
    AND TABLE_NAME != 'ontology_entailed_edges'
    AND TABLE_NAME != 'ontology_labels'
    AND TABLE_NAME != 'ontology_edges'
    AND TABLE_NAME != 'nhanes_variables_mappings'
    AND TABLE_NAME != 'ontology_dbxrefs'
    ORDER BY TABLE_NAME ASC
")

#update all the questionnaire columns with the correct questionnaire names
for (i in 1:nrow(m)) {
    currTableName = m[i,"TABLE_NAME"]
    DBI::dbGetQuery( cn, paste(sep="", "ALTER table ", currTableName, " add Description varchar(256)" ))
    DBI::dbGetQuery( cn, paste(sep="", "UPDATE ", currTableName, " SET Description = QuestionnaireDescriptions.Description from ", currTableName ," LEFT JOIN QuestionnaireDescriptions ON ", currTableName, ".Questionnaire = QuestionnaireDescriptions.Questionnaire" ))
    DBI::dbGetQuery( cn, paste(sep="", "UPDATE ", currTableName, " SET Description = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Description, ':',''), '-', ''),' ',''),'.',''),'(', ''),')', ''), '&', ''), ',', '')"))
}

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")


m = DBI::dbGetQuery(cn, "
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_CATALOG='NhanesLandingZone'
    AND TABLE_NAME != 'QuestionnaireVariables'
    AND TABLE_NAME != 'DownloadErrors'
    AND TABLE_NAME != 'VariableCodebook'
    AND TABLE_NAME != 'QuestionnaireDescriptions'
    AND TABLE_NAME != 'ontology_entailed_edges'
    AND TABLE_NAME != 'ontology_labels'
    AND TABLE_NAME != 'ontology_edges'
    AND TABLE_NAME != 'nhanes_variables_mappings'
    ORDER BY TABLE_NAME ASC
")


#testing
tryCatch({
    for (i in 1:nrow(m)) {
        
        currTableName = m[i,"TABLE_NAME"]
        questionnaireLables = DBI::dbGetQuery(
            cn, 
            paste(
                sep="", 
                "SELECT Questionnaire FROM ", currTableName, " GROUP BY Questionnaire")
            )
        
        for (j in 1:nrow(questionnaireLables)) {
            currQuestionnaire = questionnaireLables[j, "Questionnaire"]
            tryCatch({
                    DBI::dbSendQuery(cn, paste(sep="", "CREATE VIEW ", currQuestionnaire, " AS SELECT * FROM ", currTableName, " WHERE Questionnaire = '", currQuestionnaire, "'"))
                    },
                    error = function(e) {
                        DBI::dbSendQuery(cn, paste(sep="", "CREATE VIEW ", currQuestionnaire, "_view AS SELECT * FROM ", currTableName, " WHERE Questionnaire = '", currQuestionnaire, "'"))
                    }
                    )
            # shrink transaction log
            SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")
                                                }                                   
                        }
},
error = function(e) {
    print(e)
})

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")