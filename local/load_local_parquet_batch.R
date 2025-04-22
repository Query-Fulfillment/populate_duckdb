library(DBI)
library(duckdb)
library(dplyr)

# local path to store duckdb file
path_to_db <- '/data/myData.duckdb'

# list of cdm_tables to load to duckdb
cdm_tables_to_load <- c('demographic', 'diagnosis', 'encounter', 'lab_result_cm', 'prescribing', 'procedures', 'med_admin', 'provider')
#####################################################################################
con <- dbConnect(duckdb::duckdb(), dbdir = path_to_db)
for (table_name in cdm_tables_to_load ) {
  file_pattern <- sprintf('/data/temp/chop_pcornet_v56_parquet/chop_pcornet_dcc_v56_parquet.db/%s/*',table_name) # change file pattern here
  
  parquet_files <- sort(Sys.glob(file_pattern))
  print(paste0('Loading ', table_name , '...'))
  print(paste0('Total files found: ', length(parquet_files)))
  i <- 1 
  last_count <- 0
  for (f in parquet_files){
    print(sprintf('Loading %s (%s/%s): %s', table_name, i, length(parquet_files), f))
    if (i == 1){
      # First batch create table
      dbExecute(
        con,
        sprintf("DROP TABLE IF EXISTS %s; 
                  CREATE TABLE %s AS SELECT * FROM read_parquet('%s');
                  ", table_name, table_name, f)
      )
    }
    else {
      dbExecute(
        con,
        sprintf("INSERT INTO %s SELECT * FROM read_parquet('%s');
          ", table_name, f)
      )
    }
    current_count <- dbGetQuery(con, sprintf('SELECT COUNT(*) FROM %s ;', table_name))
    print(sprintf('File Loaded (%s/%s file, %s rows): %s', i, length(parquet_files), current_count - last_count, f))
    last_count <- current_count
    i <- i+1
  }
  print(sprintf("%s Done. %s loaded", table_name, current_count))
  
  
}
dbDisconnect(con)