# pull down the NHANES data

# run in container:
# docker run --rm --name workbench -d \
#     -v /Users/nathanpalmer/Projects/Databases/NHANES:/HostData \
#     -p 8787:8787 \
#     -p 2200:22 \
#     -e CONTAINER_USER_USERNAME=test \
#     -e CONTAINER_USER_PASSWORD=test \
#     --privileged \
#     nhanesdownload

# sudo mount -t cifs -o user=npp10_adm,domain=MED.HARVARD.EDU,cruid=test,gid=test,uid=test,sec=ntlmssp //dbmihdswvfsp01.med.harvard.edu/secure-data$ /mnt/DataLake

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

# TODO: step through and make sure this is doing the right thing.
#   -verify "&" files get correctly grouped

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

# can execute the code in generateSqlTableStatements.R at this point to make sure
# that all dataTypes are legit MS SQL table names

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

# enable restart
i = 1
for (i in i:length(dataTypes)) {

    # get the name of the data type
    currDataType = dataTypes[i]

    # find all rows with URLs that should be relevant to the current data type
    rowsForCurrDataType = which(fileListTable[,"ScrubbedDataType"] == currDataType)

    # assemble a list containing all of the subtables for this data type
    dfList = list()

    for (currRow in rowsForCurrDataType) {

        currFileUrl = fileListTable[currRow, "Data File"]
        
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

                result = tryCatch({
                    haven::read_xpt(currFileUrl)
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

            cat("done reading ", currFileUrl, "\n")
        }
    }

    m = dplyr::bind_rows(dfList)

    if (nrow(m) > 0) {
        write.table(
            m,
            file = paste(sep = "/", outputDirectory, currDataType),
            sep = "\t",
            na = ""
        )
    }
}
