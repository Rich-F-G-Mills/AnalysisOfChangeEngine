
# Connect to a specific postgres database i.e. Heroku
con <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = 'analysis_of_change', 
    host = 'localhost',
    port = 5432L,
    user = 'postgres',
    password = 'internet',
    options = '-c search_path=ob_wol'
  )

dbTables <-
  RPostgres::dbListTables(con)
  

DBI::dbDisconnect(con)

rm(con)