# pull down the NHANES data into a local MariaDB Columnstore database

# coax nhanesA to cooperate
Sys.unsetenv("EPICONDUCTOR_CONTAINER_VERSION")
Sys.unsetenv("EPICONDUCTOR_COLLECTION_DATE")

options(timeout=100)

# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "admin"
sqlPassword = "C0lumnStore!"



## The upstream data source is the CDC website, but eventually we wish
## to move the ETL process to use a pre-downloaded and version-tagged
## GitHub repo. The following function allows us to switch between
## these two options.

## NOTE: Use of nhanesA should be unnecessary if we switch, except for nhanesManifest()

downloadFromCDC <- FALSE
RAWDATASRC <- "https://raw.githubusercontent.com/ccb-hms/nhanes-data/main/Data/"

downloadNhanesRaw <- function(url) {
    if (downloadFromCDC)
        nhanesA::nhanesFromURL(url, translated = FALSE)
    else {
        nhtable <- gsub(".XPT", "", toupper(basename(url)), fixed = TRUE)
        TEMPDATA <- tempfile(fileext = ".csv.xz") # new file to avoid race conditions with mclapply()
        datafile <- paste0(RAWDATASRC, nhtable, ".csv.xz")
        if (download.file(datafile, destfile = TEMPDATA, quiet = TRUE) != 0) {
            cat("\n***   Failed to download ", datafile, " *** \n")
            stop("Error downloading ", url)
        }
        on.exit(unlink(TEMPDATA))
        read.csv(xzfile(TEMPDATA))
    }
}




# loop waiting for SQL database to become available
for (i in 1:60) {
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

# control persistence of downloaded and extracted text files
persistTextFiles = FALSE

outputDirectory = "/NHANES/Data"

# get the list of NHANES files that are publicly available
fileListTable = nhanesA::nhanesManifest("public")
fileListTable = fileListTable[1:200, ]

# read the table of excluded files
excludedTables = read.csv("/NHANES/excluded_tables.tsv", sep='\t')

# TODO: this call to nhanesA::nhanesManifest is throwing an error
# Deepayan says to not pull the limited access table, since everything in 
# "public" is available, but we need to agree on the semantics of what 
# goes into NhanesMetadata.ExcludedTables

# ex <- nhanesA::nhanesManifest("limited")['Table']
# reasons <- rep("Limited Access",length(ex))
# ex <- cbind(ex, reasons)
# colnames(ex)[colnames(ex) == "Table"] ="TableName"
# colnames(ex)[colnames(ex) == "reasons"] ="Reason"
# excludedTables <- rbind(excludedTables, ex) 

# write the table to file with modified column names
write.table(
    excludedTables,
    file = "/NHANES/excluded_tables.tsv",
    sep = "\t",
    na = "",
    row.names = FALSE,
    col.names = FALSE,
    quote = FALSE
)

# remove the files to be excluded from the list that we will process
fileListTable <- 
    fileListTable[
        !grepl(
        paste(excludedTables$TableName, collapse = "|"), 
        fileListTable$'Table'
        ),]

# enumerate distinct data types
fileListTable$"Table" <- strtrim(fileListTable$"Table", 128)
dataTypes = unique(fileListTable$"Table")

# fix case-differing strings
dataTypes = sort(dataTypes)
upperCaseDataTypes = toupper(dataTypes)
uniqueUpper = unique(upperCaseDataTypes)
lapply(uniqueUpper, FUN=function(upperCaseWord){return (max(which(upperCaseDataTypes == upperCaseWord)))})
representativeStringIndex = unlist(lapply(uniqueUpper, FUN=function(upperCaseWord){return (max(which(upperCaseDataTypes == upperCaseWord)))}))
names(uniqueUpper) = dataTypes[representativeStringIndex]
dataTypes = names(uniqueUpper)
names(dataTypes) = uniqueUpper

# create landing zone for the raw data, set recovery mode to simple
DBI::dbExecute(cn, "CREATE DATABASE NhanesRaw")
DBI::dbExecute(cn, "CREATE DATABASE NhanesTranslated")
DBI::dbExecute(cn, "CREATE DATABASE NhanesMetadata")
DBI::dbExecute(cn, "CREATE DATABASE NhanesOntology")

# create the ExcludedTables table in SQL
DBI::dbExecute(cn, "
    CREATE TABLE NhanesMetadata.ExcludedTables (
        TableName varchar(64),
        Reason varchar(64)
    )
")

# run bulk insert
insertStatement = paste(sep="", "
    LOAD DATA INFILE '/NHANES/excluded_tables.tsv' INTO TABLE NhanesMetadata.ExcludedTables;
")
DBI::dbExecute(cn, insertStatement)

# prevent scientific notation
options(scipen = 15)

# track which variables appear in each questionnaire
questionnaireVariables = dplyr::tibble(
    Variable=character(), 
    TableName=character()
)

# track download errors
globalDownloadErrors = dplyr::tibble(
    DataType=character(), 
    FileUrl=character(), 
    Error=character()
)

# This function is intended to be called by mclapply
# and loads a single NHANES table into the DB.
# Return values is a list of lists.  The first entry contains 
# rows to be included in globalDownloadErrors, while the second entry 
# contains rows to be included in questionnaireVariables.
importRawTableToDb <- function(i) {
    
    # since this will get run in a fork'd process, we need to 
    # establish a new connection object, can't share the one
    # from the parent process
    cn = RMariaDB::dbConnect(
            drv=RMariaDB::MariaDB(),
            username=sqlUserName,
            password=sqlPassword,
            host=sqlHost
        )
    
    # track errors that occur in this call   
    downloadErrors = 
        dplyr::tibble(
            DataType=character(), 
            FileUrl=character(), 
            Error=character()
        )
  
    # get the name of the data type
    currDataType = toupper(dataTypes[i])

    # find all rows (should only be one) with URLs that should be relevant to the current data type
    currRow = which(fileListTable[,"Table"] == currDataType)
    
    if (length(currRow) > 1) {
        stop("currRow = which(fileListTable[,\"Table\"] == currDataType) returned multiple values")
    }

    # get the URL for the SAS file pointed to by the current row
    currFileUrl = fileListTable[currRow, "DataURL"]

    # get the date range for this table
    currYears = fileListTable[currRow, "Years"]

    cat("reading ", currFileUrl, " (", i, "/",  length(dataTypes), ")\n")

    # initialize some variables to control the download loop
    downloadedTable = "error"
    nDownloadTries = 0
    maxDownloadTries = 6
    
    # loop while trying to download the file
    while (typeof(downloadedTable)!="list" && (nDownloadTries < maxDownloadTries)) {

        if (nDownloadTries > 0) { # diagnostic message
            cat("** ", currFileUrl, "                ** Attempt: ", nDownloadTries + 1, "\n")
            print(downloadedTable)
            print(downloadErrors)
        }
        nDownloadTries = nDownloadTries + 1
        
        # attempt download
        downloadedTable = tryCatch({
            downloadNhanesRaw(currFileUrl)
        }, warning = function(w) {
            downloadErrors <<- dplyr::bind_cols(
            "DataType" = currDataType, 
            "FileUrl" = currFileUrl,
            "Error" = "warning"
            )
            return("warning")
        }, error = function(e) {
            downloadErrors <<- dplyr::bind_cols(
            "DataType" = currDataType, 
            "FileUrl" = currFileUrl,
            "Error" = "error"
            )
            return("error")
        })
        
        # if that failed, wait a few seconds and try again
        if ((typeof(downloadedTable)!="list") && (downloadedTable == "error")) {
            Sys.sleep(10)
        }
    }
        
    # if we failed after the above attempts, return an error
    # along with an empty questionaireVariables table.
    if (typeof(downloadedTable)!="list" && (downloadedTable == "error")) {
        return(list(downloadErrors, questionnaireVariables))
    }

    ## FIXME: Omit this step? Unnecessary, and makes result incompatible with non-DB nhanesA
    # save the survey years in the demographics tables
    if (length(grep(pattern="DEMO", x=currDataType, fixed=TRUE)) > 0) {
        years = dplyr::tibble("years" = rep(x=currYears, times=nrow(downloadedTable)))
        downloadedTable = dplyr::bind_cols(downloadedTable, years)
    }

    # save the column name / table relationships, will be returned later
    questionnaireVariables = dplyr::bind_cols( 
        "Variable" = toupper(colnames(downloadedTable)),
        "TableName" = rep(currDataType, times = ncol(downloadedTable))
      )

    cat("done reading ", currFileUrl, "\n")

    # create a tibble from the data frame
    # m = tibble::tibble(downloadedTable)
    # rm(downloadedTable)
    gc()

    # if we were able to read a table for this data type
    if (nrow(downloadedTable) > 0) {

        # get a file system location to save the table
        currOutputFileName = paste(sep = "/", outputDirectory, currDataType)

        # get data types for each column in our current table
        columnTypes = sapply(downloadedTable, class)

        # identify columns that contain character data
        ixCharacterColumns = which(columnTypes == "character")

        # if we have any character columns
        if (length(ixCharacterColumns) > 0) {

            # iterate over the character columns
            for (currCharColumn in ixCharacterColumns) {

                # fix any embedded line endings
                downloadedTable[,currCharColumn] = gsub(downloadedTable[,currCharColumn], pattern = "\r\n", replacement = "", useBytes = TRUE, fixed = TRUE)
                downloadedTable[,currCharColumn] = gsub(downloadedTable[,currCharColumn], pattern = "\n", replacement = "", useBytes = TRUE, fixed = TRUE)
            }
        }

        # write the table to file
        write.table(
            downloadedTable,
            file = currOutputFileName,
            sep = "\t",
            na = "\\N",
            row.names = FALSE,
            col.names = FALSE,
            quote = FALSE
        )

        # generate SQL table definitions from column types in tibbles
        createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), paste("NhanesRaw", currDataType, sep="."), row.names=FALSE, downloadedTable)

        # change DOUBLE to float
        createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

        # we know that SEQN and other primary keys should always be an INT
        createTableQuery = gsub(createTableQuery, pattern = "\"SEQN\" float", replace = "\"SEQN\" INT", fixed = TRUE) # nolint
        createTableQuery = gsub(createTableQuery, pattern = "\"SAMPLEID\" float", replace = "\"SAMPLEID\" INT", fixed = TRUE) # nolint
        createTableQuery = gsub(createTableQuery, pattern = "\"DRXFDCD\" float", replace = "\"DRXFDCD\" INT", fixed = TRUE) # nolint
        createTableQuery = gsub(createTableQuery, pattern = "\"DRXMC\" float", replace = "\"DRXMC\" INT", fixed = TRUE) # nolint
        createTableQuery = gsub(createTableQuery, pattern = "\"POOLID\" float", replace = "\"POOLID\" INT", fixed = TRUE) # nolint

        # remove double quotes, which interferes with the schema specification
        createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)

        # make it a columnstore table
        createTableQuery = paste(sep="", createTableQuery, "  ENGINE=ColumnStore")
        
        # calculate the maximum length of strings in all of the character columns
        maxColLengths = unlist(
            lapply(
            X=names(ixCharacterColumns), 
            FUN=function(cname){
                return(
                    max(
                        unlist(
                        lapply(X=downloadedTable[,cname], FUN=function(x){nchar(iconv(x, to="latin1"))})
                        ),
                        na.rm=TRUE
                    )
                )
            }
            )
        )
        
        # replace TEXT specification with VARCHAR(currColLength)
        for (currColLength in maxColLengths) {
            createTableQuery = sub(pattern="TEXT", replacement=paste(sep="", "VARCHAR(", max(1, currColLength), ")"), x=createTableQuery)
        }

        # create the table in SQL
        DBI::dbExecute(cn, createTableQuery)
        
        # run bulk insert
        insertStatement = 
            paste(
                sep="",
                "LOAD DATA INFILE '",
                currOutputFileName,
                "' INTO TABLE NhanesRaw.",
                currDataType, " CHARACTER SET latin1"
            )

        DBI::dbExecute(cn, insertStatement)
        
        # if we don't want to keep the derived text files, then delete to save disk space
        if (!persistTextFiles) {
            file.remove(currOutputFileName)
        }
    }

    DBI::dbDisconnect(cn)
    return(list(downloadErrors, questionnaireVariables, i = i, url = currFileUrl))
}

# import in parallel using all available cores
parResultList = 
    parallel::mclapply(
        FUN=importRawTableToDb, 
        X=1:length(dataTypes), 
        mc.cores=parallel::detectCores()*2
    )

status_ok = vapply(parResultList, is.list, TRUE)
if (any(!status_ok)) {
    cat("NOTE: Unexpected (non-list) components in 'parResultList', for URLs:\n")
    cat(paste0("NOTE: \t", fileListTable$DataURL[!status_ok]), sep = "\n")
    str(parResultList[!status_ok])
    cat("NOTE: Expect problems\n")
}


# unwind the globalDownloadErrors from the return value
globalDownloadErrors = 
    dplyr::bind_rows(
        lapply(
        X=parResultList, 
        FUN=function(x){return(x[[1]])}
        )
    )

# unwind the questionnaireVariables from the return value
questionnaireVariables = 
    dplyr::bind_rows(
        lapply(
        X=parResultList, 
        FUN=function(x){return(x[[2]])}
        )
    )

# generate CREATE TABLE statement for the questionnaireVariables
createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), "NhanesMetadata.QuestionnaireVariables", questionnaireVariables)

# fix TEXT column types
createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(256)", fixed = TRUE)

# change DOUBLE to float
createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

# remove double quotes
createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)

# create the table
DBI::dbExecute(cn, createTableQuery)

# generate file name for temporary output
currOutputFileName = paste(sep = "/", outputDirectory, "QuestionnaireVariables.txt")

# write questionnaireVariables table to disk
write.table(
    questionnaireVariables,
    file = currOutputFileName,
    sep = "\t",
    na = "",
    append=TRUE,
    row.names = FALSE,
    col.names = FALSE,
    quote = FALSE
)

insertStatement = 
    paste(sep="",
        "LOAD DATA INFILE '",
        currOutputFileName,
        "' INTO TABLE NhanesMetadata.QuestionnaireVariables CHARACTER SET latin1"
    )
                              
DBI::dbExecute(cn, insertStatement)

# create a table to hold records of the failed file downloads
DBI::dbExecute(cn, "CREATE TABLE NhanesMetadata.DownloadErrors (DataType varchar(1024), FileUrl varchar(1024), Error varchar(256))")

# generate file name for temporary output
currOutputFileName = paste(sep = "/", outputDirectory, "DownloadErrors.txt")

# write failed file downloads table to disk
write.table(
    globalDownloadErrors,
    file = currOutputFileName,
    sep = "\t",
    na = "",
    append=TRUE,
    row.names = FALSE,
    col.names = FALSE,
    quote = FALSE
)

# issue BULK INSERT
insertStatement = 
    paste(sep="",
        "LOAD DATA INFILE '",
        currOutputFileName,
        "' INTO TABLE NhanesMetadata.DownloadErrors CHARACTER SET latin1"
    )

DBI::dbExecute(cn, insertStatement)

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")
DBI::dbExecute(cn, "PURGE BINARY LOGS BEFORE DATE_ADD(NOW(), INTERVAL 1 DAY)")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")
