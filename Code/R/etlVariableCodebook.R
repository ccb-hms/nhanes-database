# Load the NHANES variable codebooks available here:
# git clone https://github.com/ccb-hms/NHANES-metadata.git
# cd NHANES-metadata/
# git checkout tags/1.2.0
#
# run this from the root of the NHANES-metadata repository

# this is the location of the combined output file
codebookFile = paste(sep = "/", getwd(), "metadata/nhanes_variables_codebooks.csv")
tablesFile = paste(sep = "/", getwd(), "metadata/nhanes_tables.csv")
variablesFile = paste(sep = "/", getwd(), "metadata/nhanes_variables.csv")

# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "sa"
sqlPassword = "yourStrong(!)Password"
sqlDefaultDb = "NhanesLandingZone"

# loop waiting for SQL Server database to become available
for (i in 1:20) {
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
            Sys.sleep(6)
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

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")
