
tmpdir <- tempdir()
usethis::create_from_github("annakrystalli/debugging-arrow", destdir = tmpdir,
                            fork = FALSE, open = FALSE)

library(dplyr)
origin_path <- file.path(tmpdir, "debugging-arrow/simple/model-output/")

fs::dir_tree(origin_path)

# Open one dataset foe each format excluding invalid files
formats <- c("csv", "parquet")

conns <- purrr::map(purrr::set_names(formats),
                    ~arrow::open_dataset(
                        origin_path, format = .x,
                        partitioning = "team",
                        factory_options = list(exclude_invalid_files = TRUE)))

arrow::open_dataset(conns)
arrow::open_dataset(conns, unify_schemas = TRUE)
arrow::open_dataset(conns, unify_schemas = FALSE)
# Problem arising form mismatched int fields between the two datasets
conns


# Functions ----
# Function to get schema for fields in y not present in x
unify_conn_schema <- function(x, y) {
    setdiff(y$schema$names, x$schema$names) %>%
        purrr::map(~y$schema$GetFieldByName(.x)) %>%
        arrow::schema() %>%
        arrow::unify_schemas(x$schema)
}

# Get schema for fields in a dataset connection from a unified schema
get_unified_schema <- function(x, unified_schema) {
    new_schema <- x$schema$names %>%
        purrr::map(~unified_schema$GetFieldByName(.x)) %>%
        arrow::schema()
}

# Get unified schema across all datasets
unified_schema <- purrr::reduce(conns, unify_conn_schema)
unified_schema

# Get schema for each connection from unified schema
conn_schema <- purrr::map(conns, ~get_unified_schema(.x,
                                                     unified_schema))

conn_schema


# Replacing the active binding via assignment doesn't seem to work
purrr::map2(conns, conn_schema,
            function(x, y){
                x$schema <- y})

# reconnect using appropriate schema for each connection
conns_unified <- purrr::map2(names(conns), conn_schema,
                     ~arrow::open_dataset(
                         origin_path, format = .x,
                         partitioning = "team",
                         factory_options = list(exclude_invalid_files = TRUE),
                         schema = .y))

conns_unified

# All schema are equal!
all.equal(conns[[1]]$schema, conns_unified[[1]]$schema, conn_schema[[1]])

# opening dataset from unified connections does not return data form csv file
arrow::open_dataset(conns_unified)  %>%
    filter(origin_date == "2022-10-08") %>%
    collect()

# Only data from the single parquet file returned
arrow::open_dataset(conns_unified)  %>%
    filter(origin_date == "2022-10-15") %>%
    collect()
