library(DBI)
library(duckdb)
library(jsonlite)
library(dplyr)

all_configs <- jsonlite::read_json('configs.json')

path_to_csv <- all_configs$csv_dir_path
path_to_db <- all_configs$duckdb_path

column_configs <- all_configs$csv$columns
read_configs <- all_configs$csv
read_configs$columns <- NULL
column_case <- all_configs$column_case

if(column_case == "upper") {
  names(column_configs) <- toupper(names(column_configs))

column_configs <- lapply(column_configs, function(x) {
  if (is.list(x)) names(x) <- toupper(names(x))
  x
})
  all_configs$cdm_tables_to_load  <- toupper(all_configs$cdm_tables_to_load )
}

con <- dbConnect(duckdb::duckdb(), dbdir = path_to_db)

read_configs_str <- paste(
  names(read_configs),
  paste0("'", gsub("'", "''", unlist(read_configs)), "'"),
  sep = "=",
  collapse = ", "
)

for (table_name in all_configs$cdm_tables_to_load ){

  print(sprintf("Loading Table: %s", table_name))
  table_column_config <- column_configs[[table_name]]
  file_path <- file.path(path_to_csv, paste0(table_name, '.csv'))
  # get header column from csv
  header_line <- readLines(file_path, n = 1)
  header_column_names <- strsplit(header_line, coalesce(read_configs[['delim']], ','), fixed=T)[[1]]
  header_column_names <- gsub(coalesce(read_configs[['quote']], '"'), '', header_column_names)    # remove optional quotes
  # get column from config
  config_column_names <- names(table_column_config)
  # compare column
  column_only_in_file <- setdiff(header_column_names, config_column_names)
  column_only_in_config <- setdiff(config_column_names, header_column_names)
  if (!is.na(column_only_in_file[1])){
    warning(sprintf('Extra columns detected in csv files. These columns will be loaded as VARCHAR. Cols: %s. ', paste0(column_only_in_file)))
    table_column_config[column_only_in_file] <- "VARCHAR"
  }
  if (!is.na(column_only_in_config[1])){
    warning(sprintf('Extra columns detected in configs. Ignoring these columns when loading. Cols: %s. ', paste0(column_only_in_config)))
    table_column_config[column_only_in_config] <- NULL
  }
  table_column_config_str <- jsonlite::toJSON(table_column_config, auto_unbox=T)
  sql <- sprintf("DROP TABLE IF EXISTS %s; 
                 CREATE TABLE %s AS SELECT * FROM read_csv(
                     '%s', 
                     %s, 
                     columns=%s
                 );", table_name, table_name, file_path, read_configs_str, table_column_config_str)
  #print(sql)
  dbExecute(con, sql)
  row_count <- dbGetQuery(con, sprintf('SELECT COUNT(*) FROM %s ;', table_name))
  print(paste0(table_name, ' Loaded. Row Count: ', row_count))
}

dbDisconnect(con)
