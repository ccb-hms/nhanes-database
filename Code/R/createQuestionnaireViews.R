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
    AND TABLE_NAME != 'DownloadErrors'
    AND TABLE_NAME != 'VariableCodebook'
    AND TABLE_NAME != 'QuestionnaireDescriptions'
    AND TABLE_NAME != 'ontology_entailed_edges'
    AND TABLE_NAME != 'ontology_labels'
    AND TABLE_NAME != 'ontology_edges'
    AND TABLE_NAME != 'nhanes_variables_mappings'
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