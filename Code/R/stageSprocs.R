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

sproc = paste(collapse="\n", readLines("spTranslateTable.sql"))
SqlTools::dbSendUpdate(cn, sproc)

# shrink transaction log
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(NhanesLandingZone_log)")

# issue checkpoint
SqlTools::dbSendUpdate(cn, "CHECKPOINT")

# shrink tempdb
SqlTools::dbSendUpdate(cn, "USE tempdb")

tempFiles = DBI::dbGetQuery(cn, "
                        SELECT name FROM TempDB.sys.sysfiles
                        ")

for (i in 1:nrow(tempFiles)) {    
    currTempFileName = tempFiles[i,1]
    SqlTools::dbSendUpdate(cn, paste("DBCC SHRINKFILE(",currTempFileName,", 8)", sep=''))
}

# shutdown the database engine cleanly
SqlTools::dbSendUpdate(cn, "SHUTDOWN")