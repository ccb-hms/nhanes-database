

## Main function: translate_table(name, con, x, qv, vc, cleanse_numeric), where
##
## - name is the name of a NHANES table (needed to match raw data and codebook)
## - con is a DBI-compatible connection
## - x is the name of a DB table containing raw data
## - qv is the name of the DB table 'Metadata.QuestionnaireVariables'
## - vc is the name of the DB table 'Metadata.VariableCodebook'
## - cleanse_numeric is logical, whether to turn some special numeric codes to NA
##
## This is done without using the nhanesA package

translate_table <-
    function(name, con, x,
             qv = "Metadata.QuestionnaireVariables",
             vc = "Metadata.VariableCodebook",
             cleanse_numeric = TRUE)
{
    sql_data <- sprintf("SELECT * FROM %s", x)
    raw_data <- DBI::dbGetQuery(con, sql_data)
    cb <- codebookFromDB(con, name, qv, vc)
    d <- raw2translated(raw_data, cb, cleanse_numeric = FALSE)
    d
}

## Construct codebook from information in database

.dbqTableVars <- paste0(
    "SELECT Variable FROM %s ", # Metadata.QuestionnaireVariables
    "WHERE TableName = '%s'"
)

.dbqTableCodebook <- paste0(
    "SELECT ",
    "Variable, ",
    "CodeOrValue AS 'Code.or.Value', ",
    "ValueDescription AS 'Value.Description' ",
    "FROM %s ", # Metadata.VariableCodebook
    "WHERE TableName = '%s'"
)

codebookFromDB <- function(con, table, qv, vc)
{
    tvars <- DBI::dbGetQuery(con, sprintf(.dbqTableVars, qv, table))
    tcb <- DBI::dbGetQuery(con, sprintf(.dbqTableCodebook, vc, table))
    tcb_list <- split(tcb[-1], tcb$Variable)
    cb <- split(tvars, ~ Variable) |> lapply(as.list)
    vnames <- names(cb)
    for (i in seq_along(cb)) {
        iname <- vnames[[i]]
        cb[[i]][[iname]] <- tcb_list[[iname]]
    }
    cb
}

raw2translated <- function (rawdf, codebook, cleanse_numeric = TRUE) 
{
    vars <- names(rawdf)
    vars <- vars[!(vars %in% c("SEQN", "SAMPLEID"))]
    if (!is.null(codebook)) 
        names(codebook) <- toupper(names(codebook))
    for (v in vars) {
        if (is.null(codebook) && is.character(rawdf[[v]])) {
            warning("Skipping translation for character variable with missing codebook: ", v)
        }
        else if (is.null(codebook[[v]])) {
            warning("Variable not found in codebook, skipping translation for variable: ", v)
        }
        else {
            names(codebook[[v]]) <- toupper(names(codebook[[v]]))
            if (!is.list(codebook[[v]]) || is.null(codebook[[v]][[v]])) {
                warning("Missing codebook table, skipping translation for variable: ", v)
            }
            else {
                rawdf[[v]] <- translateVariable(rawdf[[v]], codebook[[v]][[v]], 
                                                cleanse_numeric = cleanse_numeric)
            }
        }
    }
    rawdf
}

translateVariable <- function (x, cb, cleanse_numeric = TRUE) 
{
    colnames(cb) <- make.names(colnames(cb))
    if ("Range of Values" %in% cb$Value.Description) 
        code2numeric(x, cb, cleanse = cleanse_numeric)
    else if ("Value was recorded" %in% cb$Value.Description) 
        char2categorical(x, cb)
    else code2categorical(x, cb)
}


code2numeric <- function (x, cb, cleanse = TRUE) 
{
    if (isFALSE(cleanse)) return(x)
    cb <- subset(cb, !(Value.Description %in% c("Range of Values", "Missing")))
    if (nrow(cb) == 0) return(x)
    map <- with(cb, structure(as.numeric(Code.or.Value), names = Value.Description))
    missingDesc <- names(which(specialNumericCodes == "NA"))
    categoricalDesc <- names(which(specialNumericCodes == "categorical"))
    wmissing <- names(map) %in% missingDesc
    if (any(wmissing)) {
        x[x %in% map[wmissing]] <- NA_real_
    }
    if (any(names(map) %in% categoricalDesc)) {
        warning("non-numeric descriptions found in apparently numeric variable: ", 
            paste(map[names(map) %in% categoricalDesc], collapse = ", "))
    }
    x
}

char2categorical <- function (x, cb) 
{
    if (!is.character(x)) 
        stop("Expected character, found ", typeof(x))
    map <- with(cb, structure(Value.Description, names = as.character(Code.or.Value)))
    i <- which(x %in% names(map))
    x[i] <- map[x[i]]
    x
}

code2categorical <- function (x, cb) 
{
    map <- with(cb, structure(Value.Description, names = as.character(Code.or.Value)))
    map[as.character(x)]
}

specialNumericCodes <- 
    c(Missing = "NA", `Don't know` = "NA", Refused = "NA", `0` = "unknown", 
      `No Lab Result` = "unknown", `Since birth` = "unknown", Refuse = "NA", 
      `Fill Value of Limit of Detection` = "unknown", None = "unknown", 
      Never = "unknown", `No lab specimen` = "unknown", `Compliance <= 0.2` = "unknown", 
      `Could not obtain` = "NA", `900 +` = "censored", `Less than 1 month` = "censored", 
      `Day 1 dietary recall not done/incomplete` = "unknown",
      `Day 2 dietary recall not done/incomplete` = "unknown", 
      `95 cigarettes or more` = "censored", `Below Limit of Detection` = "unknown", 
      `Provider did not specify goal` = "unknown", `2000 or more` = "censored", 
      `1 cigarette or less` = "censored", `Never on a daily basis` = "unknown", 
      `Participants 6+ years with no lab specimen` = "unknown", `3 or More` = "censored", 
      `Don't Know` = "NA", `Value greater than or equal to 5.00` = "censored", 
      `7 or more` = "censored", `80 years or older` = "censored", `1-14 minutes` = "interval", 
      `70 or more` = "censored", `8400 and over` = "censored",
      `First Below Detection Limit Fill Value` = "unknown", 
      `Never smoked cigarettes regularly` = "unknown", `No modification` = "unknown", 
      `No time spent outdoors` = "unknown", `Non-Respondent` = "NA", 
      `Second Below Detection Limit Fill Value` = "unknown", `Still breastfeeding` = "unknown", 
      `Still drinking formula` = "unknown", `100 or more` = "censored", 
      `Below Detection Limit Fill Value` = "unknown",
      `More than 21 meals per week` = "censored", 
      `No Lab Specimen` = "NA", `3 or more` = "censored", `DON'T KNOW` = "NA", 
      `Less than 1 year` = "censored", `Less than one hour` = "censored", 
      `1 month or less` = "censored", `13 pounds or more` = "censored", 
      `20 or more times` = "censored", `6 years or less` = "censored", 
      `7 or more people in the Household` = "censored", `85 years or older` = "censored", 
      `Don't know/not sure` = "NA", `First Fill Value of Limit of Detection` = "unknown", 
      `Second Fill Value of Limit of Detection` = "unknown", `100 +` = "censored", 
      `11 or more` = "censored", `11 years or under` = "censored", 
      `19 years or under` = "censored", `60 years or older` = "censored", 
      `At or below detection limit fill value` = "unknown", `Dont Know` = "NA", 
      `More than 1095 days (3-year) old` = "unknown", `Never had cholesterol test` = "unknown", 
      `Never heard of LDL` = "unknown", `Never smoked a whole cigarette` = "unknown", 
      `Participants 12+ years with no lab specimen` = "unknown",
      `12 hours or more` = "censored", 
      `20 or more` = "censored", `6 times or more` = "censored",
      `6 years or under` = "censored", 
      `At work or at school 9 to 5 seven days a week` = "unknown", 
      `Does not work or go to school` = "unknown", `Hasn't started yet` = "unknown", 
      REFUSED = "NA", `12 years or younger` = "censored", `13 or more` = "censored", 
      `40 or more` = "censored", `7 or more people in the Family` = "censored", 
      `Current HH FS benefits recipient last receive` = "unknown", 
      `Less than weekly` = "unknown", `More than 90 times in 30 days` = "censored", 
      `Non-current HH FS benefits recipient last rec` = "unknown", 
      `PIR value greater than or equal to 5.00` = "censored", `Since Birth` = "unknown", 
      Ungradable = "unknown", `11 or More` = "censored", `40 or More` = "censored", 
      `50 years or more` = "censored", `85 or older` = "censored", 
      `95 or more` = "censored", English = "categorical", `English and Spanish` = "categorical", 
      `Less than one year` = "unknown", `More than 1 year unspecified` = "unknown", 
      `Never smoked a pipe regularly` = "unknown", `Never smoked cigars regularly` = "unknown", 
      `Never used chewing tobacco regularly` = "unknown",
      `Never used snuff regularly` = "unknown", 
      `No Lab Result or Not Fasting for 8 to <24 hou` = "NA",
      `No lab samples` = "NA", 
      `Not MEC Examined` = "NA", Other = "NA", Spanish = "categorical", 
      `Unable to do activity (blind)` = "NA", `11 pounds or more` = "censored", 
      `13 or More` = "censored", `14 hours or more` = "censored",
      `15 drinks or more` = "censored", 
      `20 years or older` = "censored", `3 pounds or less` = "censored", 
      `480 Months or more` = "censored", `500 mg or higher` = "censored", 
      `60 minutes or more` = "censored", `600 Months or more` = "censored", 
      `70 to 150` = "interval", `80 Hours or more` = "censored",
      `85 or greater years` = "censored", 
      `Below First Limit of Detection` = "unknown",
      `Below Second Limit of Detection` = "unknown", 
      `Don't know what is 'whole grain'` = "unknown", `Less than monthly` = "unknown", 
      `Less then 3 hours` = "censored", `More than $1000` = "censored", 
      `More than 21` = "censored", `More than 300 days` = "censored", 
      `More than 365 days (1-year) old` = "unknown",
      `More than 730 days (2-year) old` = "unknown", 
      `Never heard of A1C test` = "unknown", `No Lab samples` = "unknown", 
      `Not tested in last 12 months` = "unknown",
      `Participants 3+ years with no lab specimen` = "unknown", 
      refused = "NA", `Single person family` = "unknown", `0-5 Months` = "interval", 
      `1 year or less` = "censored", `1-5 Hours` = "interval", `20 days or more` = "censored", 
      `20 to 150` = "interval", `4 or more` = "censored", `400 and over` = "censored", 
      `60 or more months` = "censored", `7 years or less` = "censored", 
      `80 or greater years` = "censored", `9 or fewer` = "censored", 
      `Less than 10 years of age` = "censored", `Less than one day` = "censored", 
      `More than 20 times a month` = "censored", `More than 21 times per week` = "censored", 
      `No lab result` = "NA", `No lab Result` = "NA",
      `Participants 3+ years with no Lab Result` = "unknown", 
      `Participants 3+ years with no surplus lab spe` = "unknown", 
      `Participants 6+ years with no Lab Result` = "unknown",
      `Participants 6+ years with no lab specimen.` = "unknown", 
      `Third Fill Value of Limit of Detection` = "unknown")

### test

if (FALSE)
{

    library(DBI)
    library(odbc)
    options(warn = 1)

    conn <-
        DBI::dbConnect(
                 odbc::odbc(),
                 uid = Sys.getenv("EPICONDUCTOR_DB_UID", unset = "sa"),
                 pwd = Sys.getenv("SA_PASSWORD", unset = "yourStrong(!)Password"),
                 server = Sys.getenv("EPICONDUCTOR_DB_SERVER", unset = "localhost"),
                 port = as.integer(Sys.getenv("EPICONDUCTOR_DB_PORT", unset = "1433")),
                 database = Sys.getenv("EPICONDUCTOR_DB_DATABASE", unset = "NhanesLandingZone"),
                 driver = Sys.getenv("EPICONDUCTOR_DB_DRIVER",
                                     unset = "ODBC Driver 17 for SQL Server"))

    for (nhtable in c("DEMO_E", "AUXAR_J", "POOLTF_D")) {
        d <-
            translate_table(name = nhtable, con = conn, x = paste0("Raw.", nhtable),
                            qv = "Metadata.QuestionnaireVariables",
                            vc = "Metadata.VariableCodebook",
                            cleanse_numeric = TRUE)
        cat(sprintf("%s:\t %d x %d\n", nhtable, nrow(d), ncol(d)))
    }

    
}
