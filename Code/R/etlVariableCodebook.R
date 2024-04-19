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
sqlUserName = "admin"
sqlPassword = "C0lumnStore!"

# loop waiting for SQL database to become available
for (i in 1:12) {
    cn = tryCatch(
        # connect to SQL
        RMariaDB::dbConnect(
          drv=RMariaDB::MariaDB(),
          username=sqlUserName,
          password=sqlPassword,
          host=sqlHost
        )
        , warning = function(e) {
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
        stop("could not connect to SQL database")
    }
})

#-----------------------------------------------------------------
# load the codebook
#-----------------------------------------------------------------

# create the VariableCodebook table in SQL
DBI::dbExecute(cn, "
    CREATE TABLE NhanesMetadata.VariableCodebook (
        Variable varchar(64),
        TableName varchar(64),
        CodeOrValue varchar(64),
        ValueDescription varchar(256),
        Count int,
        Cumulative int,
        SkipToItem varchar(64)
    )
")

insertStatement = paste(sep="", "
    LOAD DATA INFILE '", codebookFile, "' INTO TABLE NhanesMetadata.VariableCodebook FIELDS OPTIONALLY ENCLOSED BY '\"' IGNORE 1 ROWS;"
)
DBI::dbExecute(cn, insertStatement)

#-----------------------------------------------------------------
# load the table descriptions
#-----------------------------------------------------------------

# create the nhanes_tables table in SQL
DBI::dbExecute(cn, "
    CREATE TEMPORARY TABLE NhanesMetadata.tmp_nhanes_tables (
        `Table` varchar(64),
        TableName varchar(1024),
        BeginYear int,
        EndYear int,
        DataGroup varchar(64),
        UseConstraints varchar(1024),
        DocFile varchar(1024),
        DataFile varchar(1024),
        DatePublished varchar(1024)
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    LOAD DATA INFILE '", tablesFile, "' INTO TABLE NhanesMetadata.tmp_nhanes_tables FIELDS OPTIONALLY ENCLOSED BY '\"' IGNORE 1 ROWS;"
)
DBI::dbExecute(cn, insertStatement)

# clean up and insert in new table with consistent nomenclature
DBI::dbExecute(cn, "
    CREATE OR REPLACE TABLE NhanesMetadata.QuestionnaireDescriptions
        SELECT 
            T.TableName AS Description,
            Q.TableName,
            T.BeginYear,
            T.EndYear,
            T.DataGroup,
            T.UseConstraints,
            T.DocFile,
            T.DataFile,
            T.DatePublished
        FROM 
            NhanesMetadata.tmp_nhanes_tables T 
            INNER JOIN NhanesMetadata.QuestionnaireVariables Q ON
                T.`Table` = Q.TableName
        GROUP BY
            T.TableName,
            Q.TableName,
            T.BeginYear,
            T.EndYear,
            T.DataGroup,
            T.UseConstraints,
            T.DocFile,
            T.DataFile,
            T.DatePublished
")

DBI::dbExecute(cn, "DROP TABLE NhanesMetadata.tmp_nhanes_tables")

#-----------------------------------------------------------------
# load the variable descriptions
#-----------------------------------------------------------------

# create the nhanes_tables table in SQL
DBI::dbExecute(cn, "
    CREATE TEMPORARY TABLE NhanesMetadata.tmp_nhanes_variables (
        Variable varchar(64),
        `Table` varchar(64),
        SASLabel varchar(64),
        EnglishText varchar(1024),
        UseConstraints varchar(4096),
        Target varchar(4096),
        Tags varchar(1024),
        ProcessedText varchar(1024),
        VariableID varchar(1024),
        IsPhenotype varchar(1024),
        OntologyMapped varchar(1024)
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    LOAD DATA INFILE '", variablesFile, "' INTO TABLE NhanesMetadata.tmp_nhanes_variables FIELDS OPTIONALLY ENCLOSED BY '\"' IGNORE 1 ROWS;"
)
DBI::dbExecute(cn, insertStatement)

# rename the QuestionnaireVariables -- we are going to recreate it by joining to tmp_nhanes_variables
# to include additional columns 
DBI::dbExecute(cn, "RENAME TABLE NhanesMetadata.QuestionnaireVariables TO NhanesMetadata.QuestionnaireVariables_old")

#TODO: in v0.4.0, for some reason lines 222-223 don't import correctly. When switched they load correctly. 
#May need to update this in future releases.
DBI::dbExecute(cn, "
    CREATE OR REPLACE TABLE NhanesMetadata.QuestionnaireVariables
    SELECT 
        Q.TableName,
        Q.Variable,
        V.EnglishText AS Description,
        V.Target,
        V.SasLabel,
        V.UseConstraints,
        V.ProcessedText,
        V.VariableID,
        V.Tags,
        V.IsPhenotype,
        V.OntologyMapped   
    FROM NhanesMetadata.QuestionnaireVariables_old Q
        INNER JOIN NhanesMetadata.tmp_nhanes_variables V ON
            Q.TableName = V.`Table`
            AND Q.Variable = V.Variable
")

# manually update these
DBI::dbExecute(cn, "UPDATE NhanesMetadata.QuestionnaireVariables SET Description =  'Respondent sequence number', SasLabel =  'Respondent sequence number' WHERE Variable = 'SEQN'")

DBI::dbExecute(cn, "DROP TABLE NhanesMetadata.tmp_nhanes_variables")
DBI::dbExecute(cn, "DROP TABLE NhanesMetadata.QuestionnaireVariables_old")

## Temporarily stop here:

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")

q("no")



# TODO: debugged to here

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

    # change TEXT to VARCHAR(1024)
    createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(MAX)", fixed = TRUE) # nolint # nolint

    # change DOUBLE to float
    createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

    # remove double quotes, which interferes with the schema specification
    createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)

    print(createTableQuery)
    # create the table in SQL
    DBI::dbExecute(cn, createTableQuery)
    print("no problem creating")
    
    # run bulk insert
    insertStatement = paste(sep="",
                            "BULK INSERT Ontology.",
                            sqlTableName,
                            " FROM '",
                            paste0(path, currTable),
                            "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
    )
    DBI::dbExecute(cn, insertStatement)
    print("no problem inserting")
    
  # keep memory as clean as possible
  rm(loaded_data)
  gc()
 }
}

# shrink transaction log
DBI::dbExecute(cn, "DBCC SHRINKFILE(Nhaneslog)")
# issue checkpoint
DBI::dbExecute(cn, "CHECKPOINT")

# --------------------------------------------------------------------------------------------------------
# Add Ontology Mappings
# --------------------------------------------------------------------------------------------------------
ontology_mappings <- list.files(ontologyMappings)

for (currTable in ontology_mappings) {
    
    if (currTable != "README.md" & currTable != "non-mappings") {
        path = ontologyMappings
        loaded_data <- read.csv(file = paste0(path, currTable), sep = "\t")
        
        colnames(loaded_data)[which(colnames(loaded_data) == "Table")] <- "TableName"

        # generate SQL table definitions from column types in tibbles
        createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), paste("Ontology", str_extract(currTable, '.*(?=\\.tsv)'), sep="."), loaded_data) # nolint

        # change TEXT to VARCHAR(512)
        createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(512)", fixed = TRUE) # nolint # nolint

        # change DOUBLE to VARCHAR(512)
        createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" VARCHAR(512)", fixed = TRUE)
        
        # change SMALLINT to VARCHAR(512)
        createTableQuery = gsub(createTableQuery, pattern = "SMALLINT", replace = "VARCHAR(512)", fixed = TRUE)
        
        # remove double quotes, which interferes with the schema specification
        createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)
        
        # create the table in SQL
        DBI::dbExecute(cn, createTableQuery)

        # run bulk insert
        if (currTable == "nhanes_oral_health_mappings.tsv") {
            insertStatement = paste(sep="",
                                "BULK INSERT Ontology.",
                                str_extract(currTable, '.*(?=\\.tsv)'),
                                " FROM '",
                                paste0(path, currTable),
                                "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\r\n')"
        )
        }
        else{
            insertStatement = paste(sep="",
                                "BULK INSERT Ontology.",
                                str_extract(currTable, '.*(?=\\.tsv)'),
                                " FROM '",
                                paste0(path, currTable),
                                "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
        ) 
        }
        
        DBI::dbExecute(cn, insertStatement)

    # keep memory as clean as possible
    rm(loaded_data)
    gc()
 }
}

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")
