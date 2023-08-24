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
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev2, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev3, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev4, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev5, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(tempdev6, 8)")
SqlTools::dbSendUpdate(cn, "DBCC SHRINKFILE(templog, 8)")

# shutdown the database engine cleanly
SqlTools::dbSendUpdate(cn, "SHUTDOWN")