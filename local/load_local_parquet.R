library(DBI)
library(duckdb)
library(dplyr)

# local path to store duckdb file
path_to_db <- '/data/myData.duckdb'

# list of cdm_tables to load to duckdb
cdm_tables_to_load <- c('demographic', 'diagnosis')


#####################################################################################
con <- dbConnect(duckdb::duckdb(), dbdir = path_to_db)

for (table_name in cdm_tables_to_load ) {
  file_pattern <- sprintf('/data/%s.parquet',table_name) # change file pattern here
  sql <- sprintf("DROP TABLE IF EXISTS %s; 
                 CREATE TABLE %s AS 
                 SELECT * FROM '%s';
                 ", table_name, table_name, file_pattern)
  print(paste0('Loading ', table_name , '...'))
  dbExecute(con, sql)
  row_count <- dbGetQuery(con, sprintf('SELECT COUNT(*) FROM %s ;', table_name))
  print(paste0(table_name, ' Loaded. Row Count: ', row_count))
}
dbDisconnect(con)