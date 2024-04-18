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

## This will fail because VariableCodebook is not yet available in the DB

for (i in 1:1) {
    
    currRawTableName = tableList[i]
    cat("Checking ", currRawTableName, fill = TRUE)

    ## get raw data: OK
    cat("====================\n")
    raw_data <-
        DBI::dbGetQuery(cn,
                        sprintf("SELECT * FROM NhanesRaw.%s;", currRawTableName))
    str(raw_data)
    ## get QuestionnaireVariables: OK
    cat("--------------------\n")
    qv <-
        DBI::dbGetQuery(cn,
                        sprintf("SELECT * FROM NhanesMetadata.QuestionnaireVariables where TableName = '%s';", currRawTableName))
    str(qv)
    ## get VariableCodebook: NOT OK
    cat("--------------------\n")
    vc <-
        DBI::dbGetQuery(cn,
                        sprintf("SELECT * FROM NhanesMetadata.VariableCodebook where TableName = '%s';", currRawTableName))
    str(vc)
    
}


## Once this is fixed, the following _should_ work


for (i in seq_len(length(tableList))) {
    
    currRawTableName = tableList[i]
    cat("Translating ", currRawTableName)

    translatedTable =
        translate_table(name = currRawTableName, con = cn,
                        x = paste0("NhanesRaw.", nhtable),
                        qv = "NhanesMetadata.QuestionnaireVariables",
                        vc = "NhanesMetadata.VariableCodebook",
                        cleanse_numeric = TRUE)
    cat(sprintf(":\t %d x %d\n", nrow(translatedTable), ncol(translatedTable)))

    ## Write to file and do all the fancy mariaDB column store stuff

    outputDirectory = "/NHANES/Data"
    ## FIXME: may overwite if persistTextFiles = TRUE
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

    ## ...
}

# shrink transaction log
DBI::dbExecute(cn, "FLUSH BINARY LOGS")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")


