sqlHost = "localhost"
sqlUserName = "sa"
sqlPassword = "yourStrong(!)Password"
sqlDefaultDb = "NhanesLandingZone"

# connect to SQL
cn = MsSqlTools::connectMsSqlSqlLogin(
    server = sqlHost, 
    user = sqlUserName, 
    password = sqlPassword, 
    database = sqlDefaultDb
)

m = DBI::dbGetQuery(cn, "
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE 
        TABLE_TYPE = 'BASE TABLE' 
        AND TABLE_CATALOG='NhanesLandingZone'
        AND TABLE_NAME != 'QuestionnaireVariables'
")

for (i in 1:nrow(m)) {
    
    currTableName = m[i,"TABLE_NAME"]
    questionnaireLables = DBI::dbGetQuery(
        cn, 
        paste(
            sep="", 
            "SELECT Questionnaire FROM ", currTableName, " GROUP BY Questionnaire")
        )
    
    for (j in 1:nrow(questionnaireLables)) {
        
        currQuestionnaire = questionnaireLables[j, "Questionnaire"]
        DBI::dbGetQuery(
            cn, 
            paste(sep="", "CREATE VIEW ", currQuestionnaire, " AS SELECT * FROM ", currTableName, " WHERE Questionnaire = '", currQuestionnaire, "'"))
    }
}