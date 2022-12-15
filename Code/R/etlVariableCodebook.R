# Load the NHANES variable codebooks available here:
# git clone https://github.com/ccb-hms/NHANES-metadata.git
# cd NHANES-metadata/
# git checkout tags/1.2.0
#
# run this from the root of the NHANES-metadata repository

# enumerate files to be processed
fileList = list.files("metadata/variable_codebooks", full.names=TRUE)

# this is the location of the combined output file
outFile = paste(sep = "/", getwd(), "VariableCodebook.txt")

# remove any existing output
if (file.exists(outFile)) {
    file.remove(outFile)
}

file.create(outFile)

# for each of the input files
for (x in fileList) {
    
    # read the table
    tab = read.table(x, header=TRUE, sep=",")
    
    # the SEQN files are broken
    if (ncol(tab) == 1) {
        next
    }
    
    if (ncol(tab) != 5) {
        stop(paste0("The file \n", x, "\ndid not contain 5 columns"))
    }
    
    # split the full file name on directory separator
    splits = strsplit(x, split="/")[[1]]
    
    # parse out the table and variable names from the file name
    fName = splits[length(splits)]
    tableName = strsplit(fName, "_[A-Za-z0-9]+\\.csv", fixed=FALSE)[[1]]
    variableName = gsub(gsub(x = fName, pattern = paste0(tableName, "_"), replacement = ""), pattern = ".csv", replacement = "")
    
    # append columns containing the table and variable names
    tab = cbind(rep(variableName, times = nrow(tab)), tab)
    tab = cbind(rep(tableName, times = nrow(tab)), tab)
    
    # append to the output file
    write.table(x = tab, append = TRUE, sep = "\t", row.names = FALSE, col.names = FALSE, file = outFile, quote=FALSE)
}

# parameters to connect to SQL
sqlHost = "localhost"
sqlUserName = "sa"
sqlPassword = "yourStrong(!)Password"
sqlDefaultDb = "master"

# connect to SQL
cn = MsSqlTools::connectMsSqlSqlLogin(
    server = sqlHost, 
    user = sqlUserName, 
    password = sqlPassword, 
    database = sqlDefaultDb
)

# create the table in SQL
SqlTools::dbSendUpdate(cn, "
    CREATE TABLE NhanesLandingZone.dbo.VariableCodebook (
        Questionnaire varchar(64),
        Variable varchar(64),
        CodeOrValue varchar(64),
        ValueDescription varchar(256),
        Count int,
        Cumulative int,
        SkipToItem varchar(64)
    )
")

# run bulk insert
insertStatement = paste(sep="",
    "BULK INSERT NhanesLandingZone.dbo.VariableCodebook FROM '",
    outFile,
    "' WITH (KEEPNULLS, TABLOCK, ROWS_PER_BATCH=2000, FIRSTROW=1, FIELDTERMINATOR='\t')"
)

SqlTools::dbSendUpdate(cn, insertStatement)
