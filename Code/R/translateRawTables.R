# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "admin"
sqlPassword = "C0lumnStore!"

# loop waiting for SQL database to become available
for (i in 1:60) {
    cn = tryCatch(
        # connect to SQL
        RMariaDB::dbConnect(
          drv=RMariaDB::MariaDB(),
          username=sqlUserName,
          password=sqlPassword,
          load_data_local_infile = TRUE, 
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

source("/NHANES/translate-table.R")

tableList = DBI::dbGetQuery(cn, "SHOW TABLES FROM NhanesRaw")[[1]]

## check that we have all info
tableList = sort(tableList)

insertTranslatedTableToDB <- function(i) {
    
    # since this will get run in a fork'd process, we need to 
    # establish a new connection object, can't share the one
    # from the parent process
    cn = RMariaDB::dbConnect(
            drv=RMariaDB::MariaDB(),
            username=sqlUserName,
            password=sqlPassword,
            host=sqlHost
        )

    
    # get the name of the current table to be translated
    currRawTableName = tableList[i]
    cat("Translating ", currRawTableName, " (", i, "/", length(tableList), ")", "\n")

    # translate the table
    translatedTable =
        translate_table(name = currRawTableName, con = cn,
                        x = paste0("NhanesRaw.", currRawTableName),
                        qv = "NhanesMetadata.QuestionnaireVariables",
                        vc = "NhanesMetadata.VariableCodebook",
                        cleanse_numeric = TRUE)
        
    outputDirectory = "/NHANES/Data"
    currOutputFileName = paste(sep = "/", outputDirectory, currRawTableName)
    
    write.table(
        translatedTable,
        file = currOutputFileName,
        sep = "\t",
        na = "\\N",
        row.names = FALSE,
        col.names = FALSE,
        quote = FALSE
    )
    
    # set database context
    DBI::dbExecute(cn, "USE NhanesTranslated")

    # generate SQL table definition from the R table
    createTableQuery = 
        DBI::sqlCreateTable(
            DBI::ANSI(), 
            paste("`NhanesTranslated`.`", currRawTableName, "`", sep=""), 
            row.names=FALSE, 
            translatedTable
        )

    # change DOUBLE to float
    createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

    # remove double quotes
    createTableQuery = gsub(createTableQuery, pattern = '"', replace = "", fixed = TRUE)

    # make it a columnstore table
    createTableQuery = paste(sep="", createTableQuery, "  ENGINE=ColumnStore")
    
    # get data types for each column in our current table
    columnTypes = sapply(translatedTable, class)

    # identify columns that contain character data
    ixCharacterColumns = which(columnTypes == "character")

    # calculate the maximum length of strings in all of the character columns
    maxColLengths = 
        unlist(
            lapply(
                X = names(ixCharacterColumns), 
                FUN = function(cname) {
                    return(
                        max(
                            unlist(
                                lapply(
                                    X = translatedTable[,cname], 
                                    FUN = function(x) {
                                        nchar(x)
                                    }
                                )
                            ),
                            na.rm=TRUE
                        )
                    )
                }
            )
        )
    
    # why +4?
    # there is a problem loading WHQMEC_E, column WHQ510P at row 34
    if (currRawTableName == "WHQMEC_E") {
        maxColLengths = maxColLengths + 4
    }
    
    # replace TEXT specification with VARCHAR(currColLength)
    for (currColLength in maxColLengths) {
        createTableQuery = sub(pattern="TEXT", replacement=paste(sep="", "VARCHAR(", max(1, currColLength), ")"), x=createTableQuery)
    }
    
    # create the table in SQL
    DBI::dbExecute(cn, paste0("DROP TABLE IF EXISTS `NhanesTranslated`.`", currRawTableName, "`;"))
    DBI::dbExecute(cn, createTableQuery)
    
    # run bulk insert
    insertStatement = 
        paste(
            sep="",
            "LOAD DATA INFILE '",
            currOutputFileName,
            "' INTO TABLE NhanesTranslated.",
            currRawTableName, " CHARACTER SET latin1"
        )

    res = 
        try({
            DBI::dbExecute(cn, insertStatement)
        })
    
    if (inherits(res, "try-error"))
        cat(
            "Error: ", 
            conditionMessage(attr(res, "condition")),
            "\n"
        )

    # delete intermediate text file to save disk space
    file.remove(currOutputFileName)
    
    # disconnect from the database server
    DBI::dbDisconnect(cn)
}

# translate tables in parallel using all available cores
parResultList = 
    parallel::mclapply(
        FUN=insertTranslatedTableToDB, 
        X=1:length(tableList),
        mc.cores=parallel::detectCores()
    )

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")
DBI::dbExecute(cn, "PURGE BINARY LOGS BEFORE DATE_ADD(NOW(), INTERVAL 1 DAY)")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")
