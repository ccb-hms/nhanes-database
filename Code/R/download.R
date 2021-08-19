# pull down the NHANES data

# run in container (must build nhanes-workbench first from this project's Dockerfile):
# docker \
#     run \
#         --rm \
#         --name nhanes-workbench \
#         -d \
#         -v /tmp:/HostData \
#         -p 8787:8787 \
#         -p 2200:22 \
#         -p 1433:1433 \
#         -e 'CONTAINER_USER_USERNAME=test' \
#         -e 'CONTAINER_USER_PASSWORD=test' \
#         -e 'ACCEPT_EULA=Y' \
#         -e 'SA_PASSWORD=yourStrong(!)Password' \
#         nhanes-workbench

# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "sa"
sqlPassword = "yourStrong(!)Password"
sqlDefaultDb = "master"

# control persistence of downloaded and extracted text files
persistTextFiles = FALSE

dir.create("/home/test/NhanesDownload")
outputDirectory = "/home/test/NhanesDownload"

# try using the comprehesive listing
comprehensiveHtmlDataList = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx"
htmlFileList = readLines(comprehensiveHtmlDataList)
htmlTableStartLine = grep(x = htmlFileList, pattern = "<table")
htmlTableEndLine = grep(x = htmlFileList, pattern = "</table")

if (length(htmlTableStartLine) != 1 || length(htmlTableEndLine) != 1 ) {
    stop(
        paste(
            "The original HTML file listing at", 
            comprehensiveHtmlDataList, 
            "contained only one table.  You will need to do some investigation and debugging."
        )
    )
}

# convert the HTML table to a data frame so we can iterate on the rows
htmlObj = xml2::read_html(paste(collapse="\n", htmlFileList[htmlTableStartLine : htmlTableEndLine]))
fileListTable = dplyr::`%>%`(htmlObj, rvest::html_table())[[1]]

# extract URLs for the SAS data files
trace(
    rvest:::html_table.xml_node, quote({
        values <- lapply(lapply(cells, html_node, "a"), html_attr, name = "href")
        values[[1]] <- html_text(cells[[1]])
    }), at = 14
)
urlListTable = dplyr::`%>%`(htmlObj, rvest::html_table())[[1]]
untrace(rvest:::html_table.xml_node)

# fix the URLs in the table
fileListTable[, "Data File"] = 
    paste(
        sep = "",
        "https://wwwn.cdc.gov",
        urlListTable[, "Data File"]
    )

# clean up data type names
fileListTable[,"Data File Name"] = 
    unlist(
        lapply(
            X = fileListTable[,"Data File Name"],
            FUN = function(x) {
                return(
                    gsub(
                        gsub(
                            gsub(
                                gsub(
                                    gsub(
                                        gsub(
                                            gsub(
                                                gsub(
                                                    gsub(
                                                        gsub(
                                                            gsub(
                                                                gsub(
                                                                    gsub(
                                                                        gsub(
                                                                            gsub(
                                                                                x = x, 
                                                                                pattern = " and ", 
                                                                                replace = " And ", 
                                                                                fixed = TRUE
                                                                            ),
                                                                            pattern = "/", 
                                                                            replace = "", 
                                                                            fixed = TRUE
                                                                        ), 
                                                                        pattern = " ", 
                                                                        replace = "", 
                                                                        fixed=TRUE
                                                                    ), 
                                                                    pattern = "-", 
                                                                    replace = "", 
                                                                    fixed=TRUE
                                                                ), 
                                                                pattern = "'", 
                                                                replace="", 
                                                                fixed=TRUE
                                                            ),
                                                            pattern = ",", 
                                                            replace = "", 
                                                            fixed = TRUE
                                                        ),
                                                        pattern = "&",
                                                        replace = "And",
                                                        fixed = TRUE
                                                    ),
                                                    pattern = ".",
                                                    replace = "",
                                                    fixed = TRUE
                                                ),
                                                pattern = "–",
                                                replace = "",
                                                fixed = TRUE
                                            ),
                                            pattern = ":",
                                            replace = "",
                                            fixed = TRUE
                                        ),
                                        pattern = "(",
                                        replace = "",
                                        fixed = TRUE
                                    ),
                                    pattern = ")",
                                    replace = "",
                                    fixed = TRUE
                                ),
                                pattern = ";",
                                replace = "",
                                fixed = TRUE
                            ),
                            pattern = "+",
                            replace = "",
                            fixed = "TRUE"
                        ),
                        pattern = "_",
                        replace = "",
                        fixed = TRUE
                    )
                )
            }
        )
    )

# enumerate distinct data types
fileListTable[,"Data File Name"] = strtrim(fileListTable[,"Data File Name"], 128)
dataTypes = unique(fileListTable[,"Data File Name"])

# fix case-differing strings
dataTypes = sort(dataTypes)
upperCaseDataTypes = toupper(dataTypes)
uniqueUpper = unique(upperCaseDataTypes)
lapply(uniqueUpper, FUN=function(upperCaseWord){return (max(which(upperCaseDataTypes == upperCaseWord)))})
representativeStringIndex = unlist(lapply(uniqueUpper, FUN=function(upperCaseWord){return (max(which(upperCaseDataTypes == upperCaseWord)))}))
names(uniqueUpper) = dataTypes[representativeStringIndex]
dataTypes = names(uniqueUpper)
names(dataTypes) = uniqueUpper

cnames = colnames(fileListTable)
fileListTable = cbind(fileListTable,  dataTypes[toupper(fileListTable[,"Data File Name"])])
colnames(fileListTable) = c(cnames, "ScrubbedDataType")

# connect to SQL
cn = MsSqlTools::connectMsSqlSqlLogin(
    server = sqlHost, 
    user = sqlUserName, 
    password = sqlPassword, 
    database = sqlDefaultDb
)

# # create new DB
# SqlTools::dbSendUpdate(cn, "CREATE DATABASE NhanesTest")
# SqlTools::dbSendUpdate(cn, "USE NhanesTest")

# # generate dummy code to check validity of table names
# stmts = paste(sep="", "CREATE TABLE [", dataTypes, "] (id int);")

# # attempt to create all of the dummy tables
# for (currStmt in stmts) {
#     SqlTools::dbSendUpdate(cn, currStmt)
# }

# # if that worked, drop the DB
# SqlTools::dbSendUpdate(cn, "USE master")
# SqlTools::dbSendUpdate(cn, "DROP DATABASE NhanesTest")

#--------------------------------------------------------------------------------------------------------
# performance notes for large XPTs:
# 14.6G for a single PAXMIN
# 10G after gc()
# baloons to 32G after second read
# 20G after gc()
# 40 G during bind_rows
# 20 G after rm XPTs and gc()
# 12G file 
#--------------------------------------------------------------------------------------------------------

# create landing zone for the raw data
SqlTools::dbSendUpdate(cn, "CREATE DATABASE NhanesLandingZone")
SqlTools::dbSendUpdate(cn, "USE NhanesLandingZone")

# prevent scientific notation
options(scipen = 15)

# enable restart
i = 1
for (i in i:length(dataTypes)) {

    # get the name of the data type
    currDataType = dataTypes[i]

    # find all rows with URLs that should be relevant to the current data type
    rowsForCurrDataType = which(fileListTable[,"ScrubbedDataType"] == currDataType)

    # assemble a list containing all of the subtables for this data type
    dfList = list()

    # pull all of the SAS files for this data type
    for (currRow in rowsForCurrDataType) {

        # get the URL for the SAS file pointed to by the current row
        currFileUrl = fileListTable[currRow, "Data File"]
        
        # there are a few that will require one-off handling
        if (
            currFileUrl != "https://wwwn.cdc.govNA"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/Dxa/Dxa.aspx"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/2005-2006/PAXRAW_D.ZIP"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/2003-2004/PAXRAW_C.ZIP"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/2007-2008/SPXRAW_E.ZIP"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/2009-2010/SPXRAW_F.ZIP"
            && currFileUrl != "https://wwwn.cdc.gov/Nchs/Nhanes/2011-2012/SPXRAW_G.ZIP"
        ) {
            cat("reading ", currFileUrl, "\n")

            # loop to catch errors in transfer and re-run after brief wait
            result = "error"
            while (result == "error" || result == "warning") {

                urlSplit = strsplit(x = currFileUrl, split = "/", fixed = TRUE)[[1]]
                fileName  = urlSplit[length(urlSplit)]

                result = tryCatch({
                    currTemp = tempfile()
                    utils::download.file(
                        url = currFileUrl, 
                        destfile = currTemp
                    )
                    z = haven::read_xpt(currTemp)
                    file.remove(currTemp)
                    z
                }, warning = function(w) {
                    print(w)
                    Sys.sleep(2)
                    return("warning")
                }, error = function(e) {
                    print(e)
                    Sys.sleep(2)
                    return("error")
                })
            }

            dfList[[length(dfList) + 1]] = result
            rm(result)
            gc()

            cat("done reading ", currFileUrl, "\n")
        }
    }

    # fix inconsistent types in PSA age variable
    # there are actually two versions of the age variable, 'KID221' and KIQ221
    # not clear whether one or the other is supposed to be double / char from
    # the NHANES documentation
    if (currDataType == "ProstateSpecificAntigenPSA") {
        for (j in 1:length(dfList)) {
            if ("KID221" %in% colnames(dfList[[j]])) {
                dfList[[j]][,"KID221"] = as.character(dfList[[j]][,"KID221"])
            }
        }
    }

    # combine the rows from all of the SAS files for this data type
    m = dplyr::bind_rows(dfList)
    rm(dfList)
    gc()

    # if we were able to read a table for this data type
    if (nrow(m) > 0) {

        # get a file system location to save the table
        currOutputFileName = paste(sep = "/", outputDirectory, currDataType)

        # get data types for each column in our current table
        columnTypes = sapply(m, class)

        # identify columns that contain character data
        ixCharacterColumns = which(columnTypes == "character")

        # if we have any character columns
        if (length(ixCharacterColumns) > 0) {

            # iterate over the character columns
            for (currCharColumn in ixCharacterColumns) {

                # fix any embedded line endings
                m[,currCharColumn] = gsub(pattern = "[\r\n]", replacement = "", x = m[,currCharColumn][[1]])
            }
        }

        # write the table to file
        write.table(
            m,
            file = currOutputFileName,
            sep = "\t",
            na = "",
            row.names = FALSE,
            col.names = FALSE
        )

        # generate SQL table definitions from column types in tibbles
        createTableQuery = DBI::sqlCreateTable(DBI::ANSI(), currDataType, m)

        # change TEXT to VARCHAR(256)
        createTableQuery = gsub(createTableQuery, pattern = "\" TEXT", replace = "\" VARCHAR(256)", fixed = TRUE)

        # change DOUBLE to float
        createTableQuery = gsub(createTableQuery, pattern = "\" DOUBLE", replace = "\" float", fixed = TRUE)

        # we know that SEQN should always be an INT
        createTableQuery = gsub(createTableQuery, pattern = "\"SEQN\" float", replace = "\"SEQN\" INT", fixed = TRUE)

        # create the table in SQL
        SqlTools::dbSendUpdate(cn, createTableQuery)

        # run bulk insert
        insertStatement = paste(sep="",
            "BULK INSERT ",
            currDataType,
            " FROM '",
            currOutputFileName,
            "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=1, FIELDTERMINATOR='\t')"
        )

        SqlTools::dbSendUpdate(cn, insertStatement)

        # TODO: should parameterize the bulk insert to allow remote SQL Server instance, like this:
        # https://www.easysoft.com/support/kb/kb01017.html
        # would need to allow SMB shares out of the container...

        # if we don't want to keep the derived text files, then delete to save disk space
        if (!persistTextFiles) {
            file.remove(currOutputFileName)
        }
    }

    # keep memory as clean as possible
    rm(m)
    gc()
}
