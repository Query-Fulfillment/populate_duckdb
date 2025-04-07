library(DBI)
library(duckdb)
library(jsonlite)
library(dplyr)

# local path to store duckdb file
path_to_db <- '/data/temp/chop_pcornet_v56.duckdb'

# list of cdm_tables to load to duckdb
cdm_tables_to_load <- c('demographic', 'diagnosis')

# file formats will be {s3_base_path}/{cdm_table_name}.{file_extension}
s3_base_path <- 's3://chop-snowflake/duckdb_load'
file_extension <- 'parquet'

# S3 secret. Other options for aws auth is here: https://duckdb.org/docs/stable/extensions/httpfs/s3api.html#credential_chain-provider
s3_secret_sql <- "CREATE OR REPLACE SECRET secret (
  TYPE s3,
  KEY_ID 'your_s3_key_id',
  SECRET 'your_s3_secret_access_key',
  SESSION_TOKEN 'your_s3_session_token. Remove this option if your aws s3 auth doesn't require session_token,
  REGION 'your_aws_region_id. e.g. us-east-1'
);"


#####################################################################################
con <- dbConnect(duckdb::duckdb(), dbdir = path_to_db)
dbExecute(con, 'INSTALL httpfs; LOAD httpfs;')
dbExecute(con, s3_secret_sql)

for (table_name in cdm_tables_to_load ) {
  sql <- sprintf("DROP TABLE IF EXISTS %s; 
                 CREATE TABLE %s AS 
                 SELECT * FROM '%s/%s.%s';
                 ", table_name, table_name, s3_base_path, table_name, file_extension)
  print(paste0('Loading ', table_name , '...'))
  dbExecute(con, sql)
}