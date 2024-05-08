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

# create the table in NhanesMetadata
DBI::dbExecute(cn, "
    CREATE TABLE NhanesMetadata.Versions (
        Name varchar(2048), 
        Value varchar(2048)
    )
")

# read versions from files
collectionDate = readr::read_lines("/EPICONDUCTOR_COLLECTION_DATE.txt")[1]
version = readr::read_lines("/EPICONDUCTOR_CONTAINER_VERSION.txt")[1]

# insert into database
DBI::dbExecute(cn, 
    paste0("
        INSERT INTO NhanesMetadata.Versions  VALUES (
            'EPICONDUCTOR_COLLECTION_DATE', 
            '", collectionDate, "'
        )"
    )
)
DBI::dbExecute(cn, 
    paste0("
        INSERT INTO NhanesMetadata.Versions  VALUES (
            'EPICONDUCTOR_CONTAINER_VERSION', 
            '", version, "'
        )"
    )
)


# shrink transaction log
DBI::dbExecute(cn, "PURGE BINARY LOGS BEFORE NOW")

# shutdown the database engine cleanly
DBI::dbExecute(cn, "SHUTDOWN")
