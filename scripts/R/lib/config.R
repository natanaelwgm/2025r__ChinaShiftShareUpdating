load_config <- function(config_dir = "config") {
  globals_path <- file.path(config_dir, "globals.yaml")
  params_path <- file.path(config_dir, "params.yaml")
  schemas_path <- file.path(config_dir, "schemas.yaml")

  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required. Please install it before running the orchestrator.")
  }

  globals <- yaml::read_yaml(globals_path)
  params <- yaml::read_yaml(params_path)
  schemas <- yaml::read_yaml(schemas_path)

  list(
    globals = globals,
    params = params,
    schemas = schemas
  )
}

