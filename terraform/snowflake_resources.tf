resource "snowflake_database" "db" {
  name    = "UK_REAL_ESTATE"
  comment = "Project database for UK housing analytics"
}

resource "snowflake_warehouse" "ingest" {
  name           = "INGEST_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_warehouse" "transform" {
  name           = "TRANSFORM_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.db.name
  name     = "BRONZE"
}

resource "snowflake_role" "de_role" {
  name = "DATA_ENGINEER"
}

resource "snowflake_database_grant" "db_grant" {
  database_name = snowflake_database.db.name
  privilege     = "USAGE"
  roles         = [snowflake_role.de_role.name]
}
