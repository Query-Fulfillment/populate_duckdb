library(DBI)
library(duckdb)
library(jsonlite)

path_to_csv <- '/data/temp/chop_pcornet_v56/'
path_to_db <- '/data/temp/chop_pcornet_v56.duckdb'


all_configs <- jsonlite::read_json('configs.json')
column_configs <- all_configs$columns
read_configs <- all_configs
read_configs$columns <- NULL

con <- dbConnect(duckdb::duckdb(), dbdir = path_to_db)

read_configs_str <- paste(
  names(read_configs),
  paste0("'", gsub("'", "''", unlist(read_configs)), "'"),
  sep = "=",
  collapse = ", "
)

for (table_name in c('demographic', 'condition', 'diagnosis', 'encounter') ){
  file_path <- file.path(path_to_csv, paste0(table_name, '.csv'))
  column_configs_str <- jsonlite::toJSON(column_configs[[table_name]], auto_unbox=T)
  sql <- sprintf("DROP TABLE IF EXISTS %s; 
                 CREATE TABLE %s AS SELECT * FROM read_csv(
                     '%s', 
                     %s, 
                     columns=%s
                 );", table_name, table_name, file_path, read_configs_str, column_configs_str)
  print(sql)
  dbExecute(con, sql)
}

dbDisconnect(con)
