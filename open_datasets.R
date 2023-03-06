library(dplyr)
tmpdir <- tempdir()
usethis::create_from_github()


origin_path <- "simple/model-output/"

# Open dataset excluding invalid files
formats <- c("csv", "parquet")

conns <- purrr::map(purrr::set_names(formats),
                    ~arrow::open_dataset(
                        origin_path, format = .x,
                        partitioning = "team",
                        factory_options = list(exclude_invalid_files = TRUE)))


arrow::open_dataset(conns, unify_schemas = FALSE)
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

# Get schema for each connection from unified schema
conn_schema <- purrr::map(conns, ~get_unified_schema(.x,
                                                     unified_schema))
# reconnect using appropriate schema for each connection
conns <- purrr::map2(names(conns), conn_schema,
                     ~arrow::open_dataset(
                         origin_path, format = .x,
                         partitioning = "team",
                         factory_options = list(exclude_invalid_files = TRUE),
                         schema = .y))

conns

arrow::open_dataset(conns)
