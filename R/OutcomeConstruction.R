createOutcomeProportions <- function(connection,
                                     cohortDatabaseSchema,
                                     cohortStagingTable,
                                     cohortTable,
                                     cohortOutcomeTable,
                                     targetIds, 
                                     oracleTempSchema,
                                     incremental,
                                     incrementalFolder) {
  packageName <- getThisPackageName()
  outcomeIds <- getOutcomes()$cohortId
  outcomeTimeWindows <- getOutcomeTimeWindows()
  otwTempTableSql <- outcomeWindowsTempTableSql(connection, outcomeTimeWindows, oracleTempSchema)
  sql <- SqlRender::loadRenderTranslateSql(dbms = attr(connection, "dbms"),
                                           sqlFilename = "CreateOutcomeProportions.sql",
                                           packageName = packageName,
                                           oracleTempSchema = oracleTempSchema,
                                           warnOnMissingParameters = TRUE,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_staging_table = cohortStagingTable,
                                           cohort_table = cohortTable,
                                           cohort_outcome_table = cohortOutcomeTable,
                                           outcome_ids = outcomeIds,
                                           outcome_time_window_table_create = otwTempTableSql$create,
                                           outcome_time_window_table_drop = otwTempTableSql$drop)
  
  cohortId <- 1000 # Represents the range of outcome cohorts used in this process
  recordKeepingFile <- "OutcomeProportions.csv"
  if (!incremental || isTaskRequired(cohortId = cohortId,
                                     checksum = computeChecksum(sql),
                                     recordKeepingFile = recordKeepingFile)) {

    ParallelLogger::logInfo("Compute outcome proportions for all target and strata")
    DatabaseConnector::executeSql(connection, sql)
    if (incremental) {
      recordTasksDone(cohortId = cohortId, checksum = computeChecksum(sql), recordKeepingFile = recordKeepingFile)
    }
  }
}

exportOutcomeProportions <- function(connection,
                                     cohortDatabaseSchema,
                                     cohortOutcomeTable) {
  packageName <- getThisPackageName()
  sql <- SqlRender::loadRenderTranslateSql(dbms = attr(connection, "dbms"),
                                           sqlFilename = "GetOutcomeProportions.sql",
                                           packageName = packageName,
                                           warnOnMissingParameters = TRUE,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_outcome_table = cohortOutcomeTable)
  
  return(DatabaseConnector::querySql(connection, sql))
}

outcomeWindowsTempTableSql <- function(connection, outcomeWindows, oracleTempSchema) {
  sql <- "WITH data AS (
            @unions
          ) 
          SELECT window_id, window_start, window_end
          INTO #outcome_windows
          FROM data;"
  unions <- "";
  for(i in 1:nrow(outcomeWindows)) {
    stmt <- paste0("SELECT ", outcomeWindows$windowId[i], " window_id, ", 
                   outcomeWindows$windowStart[i], " window_start, ", 
                   outcomeWindows$windowEnd[i], " window_end")
    unions <- paste(unions, stmt, sep="\n")
    if (i < nrow(outcomeWindows)) {
      unions <- paste(unions, "UNION ALL", sep="\n")
    }
  }
  
  sql <- SqlRender::render(sql, unions = unions)
  sql <- SqlRender::translate(sql = sql, 
                              targetDialect = attr(connection, "dbms"),
                              oracleTempSchema = oracleTempSchema)
  
  dropSql <- "TRUNCATE TABLE #outcome_windows;\nDROP TABLE #outcome_windows;\n\n"
  return(list(create = sql, drop = dropSql))
}

getOutcomes <- function() {
  packageName <- getThisPackageName()
  pathToCsv <- system.file("settings/CohortsToCreateOutcome.csv", package = packageName)
  outcomes <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(outcomes)
}

getOutcomeTimeWindows <- function() {
  packageName <- getThisPackageName()
  pathToCsv <- system.file("settings/outcomeTimeWindows.csv", package = packageName)
  outcomeTimeWindows <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(outcomeTimeWindows)
}