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

str(tableList)

for (i in seq_len(length(tableList))) {
    
    currRawTableName = tableList[i]
    cat("Translating ", currRawTableName)

    translatedTable =
        translate_table(name = currRawTableName, con = cn,
                        x = paste0("NhanesRaw.", currRawTableName),
                        qv = "NhanesMetadata.QuestionnaireVariables",
                        vc = "NhanesMetadata.VariableCodebook",
                        cleanse_numeric = TRUE)
    cat(sprintf(":\t %d x %d ", nrow(translatedTable), ncol(translatedTable)))
    
    ## Eventually, for bulk insert:
    ## ## Write to file and do all the fancy mariaDB column store stuff
    ##
    ## outputDirectory = "/NHANES/Data"
    ## ## FIXME: may overwite if persistTextFiles = TRUE
    ## currOutputFileName = paste(sep = "/", outputDirectory, currRawTableName)
    ##
    ## write.table(
    ##     translatedTable,
    ##     file = currOutputFileName,
    ##     sep = "\t",
    ##     na = "\\N",
    ##     row.names = FALSE,
    ##     col.names = FALSE,
    ##     quote = FALSE
    ##   )
    ## 
    ## ...

    ## for quick and dirty testing --- no primary key, not null etc
##    destTableName = paste0("NhanesTranslated.", currRawTableName)

    DBI::dbGetQuery(cn, "USE NhanesTranslated")
    destTableName = currRawTableName # in default database?
    ##cat("Trying to insert table ", destTableName, "\n")

    res = 
        try(
        {
            fieldTypes = RMariaDB::dbDataType(cn, translatedTable)
            typeFreq = table(fieldTypes)
            cat("[",
                paste(names(typeFreq), " = ", typeFreq,
                      collapse = ", "),
                "]\n")
            DBI::dbWriteTable(cn, destTableName,
                              translatedTable,
                              field.types = fieldTypes,
                              row.names = FALSE,
            ## ## gives 'no database selected' error when trying to insert          
                              safe = FALSE)
        }, silent = TRUE)

    if (inherits(res, "try-error"))
        cat("Error: ", conditionMessage(attr(res, "condition")),
            "\n")
    ## else cat("Success!\n")
}

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")
DBI::dbExecute(cn, "PURGE BINARY LOGS BEFORE DATE_ADD(NOW(), INTERVAL 1 DAY)")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")
