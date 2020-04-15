#' @export
runStudy <- function(connectionDetails = NULL,
                     connection = NULL,
                     cdmDatabaseSchema,
                     oracleTempSchema = NULL,
                     cohortDatabaseSchema,
                     cohortStagingTable = "cohort_stg",
                     cohortTable = "cohort",
                     cohortOutcomeTable = "cohort_out",
                     cohortIds = NULL,
                     cohortGroups = getUserSelectableCohortGroups(),
                     exportFolder,
                     databaseId,
                     databaseName = databaseId,
                     databaseDescription = "",
                     minCellCount = 5,
                     incremental = TRUE,
                     incrementalFolder = file.path(exportFolder, "RecordKeeping")) {

  start <- Sys.time()

  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  if (!is.null(getOption("fftempdir")) && !file.exists(getOption("fftempdir"))) {
    warning("fftempdir '", getOption("fftempdir"), "' not found. Attempting to create folder")
    dir.create(getOption("fftempdir"), recursive = TRUE)
  }
  
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  # Instantiate cohorts -----------------------------------------------------------------------
  cohorts <- getCohortsToCreate()
  targetCohortIds <- cohorts[cohorts$cohortType %in% cohortGroups$cohortGroup, "cohortId"][[1]]
  strataCohortIds <- cohorts[cohorts$cohortType == "strata", "cohortId"][[1]]
  outcomeCohortIds <- cohorts[cohorts$cohortType == "outcome", "cohortId"][[1]]
  
  # Start with the target cohorts
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo("  ---- Creating target cohorts ---- ")
  ParallelLogger::logInfo("**********************************************************")
  instantiateCohortSet(connectionDetails = connectionDetails,
                       connection = connection,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortStagingTable,
                       cohortIds = targetCohortIds,
                       minCellCount = minCellCount,
                       createCohortTable = TRUE,
                       generateInclusionStats = FALSE,
                       incremental = incremental,
                       incrementalFolder = incrementalFolder,
                       inclusionStatisticsFolder = exportFolder)

  # Next do the strata cohorts
  ParallelLogger::logInfo("******************************************")
  ParallelLogger::logInfo("Creating strata cohorts")
  ParallelLogger::logInfo("******************************************")
  instantiateCohortSet(connectionDetails = connectionDetails,
                       connection = connection,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortStagingTable,
                       cohortIds = strataCohortIds,
                       minCellCount = minCellCount,
                       createCohortTable = FALSE,
                       generateInclusionStats = FALSE,
                       incremental = incremental,
                       incrementalFolder = incrementalFolder,
                       inclusionStatisticsFolder = exportFolder)

  # Create the outcome cohorts
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Creating outcome cohorts ---- ")
  ParallelLogger::logInfo("**********************************************************")
  instantiateCohortSet(connectionDetails = connectionDetails,
                       connection = connection,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortStagingTable,
                       cohortIds = outcomeCohortIds,
                       minCellCount = minCellCount,
                       createCohortTable = FALSE,
                       generateInclusionStats = FALSE,
                       incremental = incremental,
                       incrementalFolder = incrementalFolder,
                       inclusionStatisticsFolder = exportFolder)

  # Create the stratified cohorts
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Creating stratified target cohorts ---- ")
  ParallelLogger::logInfo("**********************************************************")
  createBulkStrata(connection = connection,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   cohortDatabaseSchema = cohortDatabaseSchema,
                   cohortStagingTable = cohortStagingTable,
                   targetIds = targetCohortIds,
                   oracleTempSchema = oracleTempSchema,
                   incremental = incremental,
                   incrementalFolder = incrementalFolder)

  # Copy and censor cohorts to the final table
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Copy cohorts to main table ---- ")
  ParallelLogger::logInfo("**********************************************************")
  copyAndCensorCohorts(connection = connection,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortStagingTable = cohortStagingTable,
                       cohortTable = cohortTable,
                       minCellCount = minCellCount,
                       targetIds = targetCohortIds,
                       oracleTempSchema = oracleTempSchema,
                       incremental = incremental,
                       incrementalFolder = incrementalFolder)
  
  # Compute the outcomes
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Create outcome proportions ---- ")
  ParallelLogger::logInfo("**********************************************************")
  createOutcomeProportions(connection = connection,
                           cohortDatabaseSchema = cohortDatabaseSchema,
                           cohortStagingTable = cohortStagingTable,
                           cohortTable = cohortTable,
                           cohortOutcomeTable = cohortOutcomeTable,
                           targetIds = targetCohortIds,
                           oracleTempSchema = oracleTempSchema,
                           incremental = incremental,
                           incrementalFolder = incrementalFolder)
  
  cohortsForExport <- loadCohortsForExportFromPackage(cohortIds = cohortIds)
  writeToCsv(cohortsForExport, file.path(exportFolder, "cohort.csv"))

  if (incremental) {
    recordKeepingFile <- file.path(incrementalFolder, "CreatedAnalyses.csv")
  }

  ParallelLogger::logInfo("Saving database metadata")
  database <- data.frame(databaseId = databaseId,
                         databaseName = databaseName,
                         description = databaseDescription,
                         isMetaAnalysis = 0)
  writeToCsv(database, file.path(exportFolder, "database.csv"))

  # Counting staging cohorts ---------------------------------------------------------------
  ParallelLogger::logInfo("Counting staging cohorts")
  subset <- subsetToRequiredCohorts(cohorts = loadCohortsFromPackage(cohortIds),
                                    task = "getStagingCohortCounts",
                                    incremental = incremental,
                                    recordKeepingFile = recordKeepingFile)
  if (nrow(subset) > 0) {
    counts <- getCohortCounts(connection = connection,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortStagingTable,
                              cohortIds = subset$cohortId)
    if (nrow(counts) > 0) {
      counts$databaseId <- databaseId
      counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
      counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
    }
    writeToCsv(counts, file.path(exportFolder, "cohort_staging_count.csv"), incremental = incremental, cohortId = subset$cohortId)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "getStagingCohortCounts",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
  }
  
  # Counting cohorts -----------------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohorts")
  counts <- getCohortCounts(connection = connection,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            cohortTable = cohortTable)
  if (nrow(counts) > 0) {
    counts$databaseId <- databaseId
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  writeToCsv(counts, file.path(exportFolder, "cohort_count.csv"))
  recordTasksDone(cohortId = subset$cohortId,
                  task = "getCohortCounts",
                  checksum = NULL,
                  recordKeepingFile = recordKeepingFile,
                  incremental = incremental)

  # Read in the cohort counts
  counts <- readr::read_csv(file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))

  # Extract outcome counts -----------------------------------------------------------------------
  ParallelLogger::logInfo("Extract outcome counts")
  outcomeProportions <- exportOutcomeProportions(connection = connection,
                                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                                 cohortOutcomeTable = cohortOutcomeTable)
  if (nrow(outcomeProportions) > 0) {
    outcomeProportions$databaseId <- databaseId
    outcomeProportions <- enforceMinCellValue(counts, "outcome_count", minCellCount)
  }
  writeToCsv(counts, file.path(exportFolder, "outcome_proportions.csv"))

  # Read in the cohort counts
  counts <- readr::read_csv(file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))
  
  # Subset the cohorts to the target/strata for running feature extraction
  featureExtractionCohorts <- cohortsForExport[cohortsForExport$cohortId %in% counts$cohortId, ]
  
  # Cohort characterization ---------------------------------------------------------------
  runCohortCharacterization <- function(row, covariateSettings, settingsDescription) {
    ParallelLogger::logInfo("- Creating characterization for cohort: ", row$cohortName, " (settings: ", settingsDescription, ")")
    data <- getCohortCharacteristics(connection = connection,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     oracleTempSchema = oracleTempSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     cohortId = row$cohortId,
                                     covariateSettings = covariateSettings)
    if (nrow(data) > 0) {
      data$cohortId <- row$cohortId
    }
    return(data)
  }

  # Baseline Cohort characterization ---------------------------------------------------------------
  ParallelLogger::logInfo("******************************************")
  ParallelLogger::logInfo("Creating baseline cohort characterizations")
  ParallelLogger::logInfo("******************************************")
  baselineCovariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE,
                                                                          useDemographicsAgeGroup = TRUE,
                                                                          useConditionGroupEraShortTerm = TRUE,
                                                                          useConditionGroupEraLongTerm = TRUE,
                                                                          useDrugGroupEraShortTerm = TRUE,
                                                                          useDrugGroupEraLongTerm = TRUE,
                                                                          longTermStartDays = -365,
                                                                          shortTermStartDays = -30,
                                                                          endDays = -1)
  task <- "runBaselineCohortCharacterization"
  settingsDescription <- paste0(baselineCovariateSettings$endDays, " to ", baselineCovariateSettings$shortTermStartDays, " start days")
  subset <- subsetToRequiredCohorts(cohorts = featureExtractionCohorts,
                                    task = task,
                                    incremental = incremental,
                                    recordKeepingFile = recordKeepingFile)
  if (nrow(subset) > 0) {
    data <- lapply(split(subset, subset$cohortId), runCohortCharacterization, covariateSettings = baselineCovariateSettings, settingsDescription = settingsDescription)
    data <- do.call(rbind, data)
    covariates <- formatCovariates(data)
    writeToCsv(covariates, file.path(exportFolder, "covariate.csv"), incremental = incremental, covariateId = covariates$covariateId)
    data <- formatCovariateValues(data, counts, minCellCount)
    writeToCsv(data, file.path(exportFolder, "covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
    recordTasksDone(cohortId = subset$cohortId,
                    task = task,
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
  }

  # Post-index Cohort characterization ---------------------------------------------------------------
  ParallelLogger::logInfo("********************************************")
  ParallelLogger::logInfo("Creating post-index cohort characterizations")
  ParallelLogger::logInfo("********************************************")
  task <- "runPostIndexCohortCharacterization"
  postIndexCovariateSettings1 <- FeatureExtraction::createCovariateSettings(useConditionGroupEraShortTerm = TRUE,
                                                                            useDrugGroupEraStartShortTerm = TRUE,
                                                                            shortTermStartDays = 0,
                                                                            endDays = 0)
  settingsDescription1 <- paste0(postIndexCovariateSettings1$shortTermStartDays, " to ", postIndexCovariateSettings1$endDays, " days")
  postIndexCovariateSettings2 <- FeatureExtraction::createCovariateSettings(useConditionGroupEraLongTerm = TRUE,
                                                                            useDrugGroupEraStartLongTerm = TRUE,
                                                                            longTermStartDays = 0,
                                                                            endDays = 30)
  settingsDescription2 <- paste0(postIndexCovariateSettings2$longTermStartDays, " to ", postIndexCovariateSettings1$endDays, " days")
  
  subset <- subsetToRequiredCohorts(cohorts = featureExtractionCohorts,
                                    task = task,
                                    incremental = incremental,
                                    recordKeepingFile = recordKeepingFile)
  if (nrow(subset) > 0) {
    data1 <- lapply(split(subset, subset$cohortId), runCohortCharacterization, covariateSettings = postIndexCovariateSettings1, settingsDescription = settingsDescription1)
    data1 <- do.call(rbind, data1)
    data2 <- lapply(split(subset, subset$cohortId), runCohortCharacterization, covariateSettings = postIndexCovariateSettings2, settingsDescription = settingsDescription2)
    data2 <- do.call(rbind, data2)
    data <- rbind(data1, data2)
    covariates <- formatCovariates(data)
    writeToCsv(covariates, file.path(exportFolder, "post_index_covariate.csv"), incremental = incremental, covariateId = covariates$covariateId)
    data <- formatCovariateValues(data, counts, minCellCount)
    writeToCsv(data, file.path(exportFolder, "post_index_covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
    recordTasksDone(cohortId = subset$cohortId,
                    task = task,
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
  }

  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Running study took",
                                signif(delta, 3),
                                attr(delta, "units")))
  
}

#' @export
getUserSelectableCohortGroups <- function() {
  cohortGroups <- getCohortGroups()
  return(cohortGroups[cohortGroups$userCanSelect == TRUE, ])
}

getThisPackageName <- function() {
  return("Covid19TargetAndOutcomeCharacterization")
}


formatCovariates <- function(data) {
  # Drop covariates with mean = 0 after rounding to 4 digits:
  data <- data[round(data$mean, 4) != 0, ]
  covariates <- unique(data[, c("covariateId", "covariateName", "analysisId")])
  colnames(covariates)[[3]] <- "covariateAnalysisId"
  return(covariates)
}

formatCovariateValues <- function(data, counts, minCellCount) {
  data$covariateName <- NULL
  data$analysisId <- NULL
  if (nrow(data) > 0) {
    data$databaseId <- databaseId
    data <- merge(data, counts[, c("cohortId", "cohortEntries")])
    data <- enforceMinCellValue(data, "mean", minCellCount/data$cohortEntries)
    data$sd[data$mean < 0] <- NA
    data$cohortEntries <- NULL
    data$mean <- round(data$mean, 3)
    data$sd <- round(data$sd, 3)
  }
  return(data)  
}

getCohortGroups <- function () {
  packageName <- getThisPackageName()
  pathToCsv <- system.file("settings/CohortGroups.csv", package = packageName)
  cohortGroups <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(cohortGroups);
}

getCohortsToCreate <- function() {
  packageName <- getThisPackageName()
  cohortGroups <- getCohortGroups()
  cohorts <- data.frame()
  for(i in 1:nrow(cohortGroups)) {
    c <- readr::read_csv(system.file(cohortGroups$fileName[i], package = packageName), col_types = readr::cols())
    c$cohortType <- cohortGroups$cohortGroup[i]
    cohorts <- rbind(cohorts, c)
  }
  return(cohorts)  
}

loadCohortsFromPackage <- function(cohortIds) {
  packageName = getThisPackageName()
  cohorts <- getCohortsToCreate()
  cohorts$atlasId <- NULL
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }
  if ("atlasName" %in% colnames(cohorts)) {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "atlasName")
  } else {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "fullName")
  }
  
  getSql <- function(name) {
    pathToSql <- system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }
  cohorts$sql <- sapply(cohorts$cohortName, getSql)
  getJson <- function(name) {
    pathToJson <- system.file("cohorts", paste0(name, ".json"), package = packageName)
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }
  cohorts$json <- sapply(cohorts$cohortName, getJson)
  return(cohorts)
}

loadCohortsForExportFromPackage <- function(cohortIds, packageName) {
  packageName = getThisPackageName()
  cohorts <- getCohortsToCreate()
  cohorts$atlasId <- NULL
  cohorts$targetId <- 0
  cohorts$targetName <- ""
  cohorts$strataId <- 0
  cohorts$strataName <- ""
  if ("atlasName" %in% colnames(cohorts)) {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "atlasName")
  } else {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "fullName")
  }
  
  # Get the stratified cohorts for the study
  # and join to the cohorts to create to get the names
  strataCohorts <- getAllStrata()
  targetStrataXref <- getTargetStrataXref()
  targetStrataXrefWithNames <- dplyr::inner_join(strataCohorts, 
                                                 targetStrataXref, 
                                                 by = c("cohortId" = "strataId"))
  targetStrataXrefWithNames$atlasId <- NULL
  targetStrataXrefWithNames <- dplyr::rename(targetStrataXrefWithNames, strataId = "cohortId", strataName = "name", cohortId = "cohortId.y")
  targetStrataXrefWithNames <- dplyr::inner_join(cohorts[,c("cohortId","cohortName")], 
                                                 targetStrataXrefWithNames, 
                                                 by = c("cohortId" = "targetId"))
  
  targetStrataXrefWithNames <- dplyr::rename(targetStrataXrefWithNames, targetId = "cohortId", targetName = "cohortName", cohortId = "cohortId.y")
  targetStrataXrefWithNames$cohortName <- paste(targetStrataXrefWithNames$targetName,
                                                ifelse(targetStrataXrefWithNames$cohortType == "TwS", "with", "without"),
                                                targetStrataXrefWithNames$strataName)
  targetStrataXrefWithNames$cohortFullName <- targetStrataXrefWithNames$cohortName
  
  cols <- names(cohorts)
  cohorts <- rbind(cohorts, targetStrataXrefWithNames[cols])
    
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }

  return(cohorts)
}

writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    params$data = data
    params$fileName = fileName
    do.call(saveIncremental, params)
  } else {
    readr::write_csv(data, fileName)
  }
}

enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(data[, fieldName]) & data[, fieldName] < minValues & data[, fieldName] != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor)/nrow(data), 1)
    ParallelLogger::logInfo("   censoring ",
                            sum(toCensor),
                            " values (",
                            percent,
                            "%) from ",
                            fieldName,
                            " because value below minimum")
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}

getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortCounts.sql",
                                           packageName = getThisPackageName(),
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = cohortIds)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Counting cohorts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
  
}

subsetToRequiredCohorts <- function(cohorts, task, incremental, recordKeepingFile) {
  if (incremental) {
    tasks <- getRequiredTasks(cohortId = cohorts$cohortId,
                              task = task,
                              checksum = cohorts$checksum,
                              recordKeepingFile = recordKeepingFile)
    return(cohorts[cohorts$cohortId %in% tasks$cohortId, ])
  } else {
    return(cohorts)
  }
}

getRequiredTasks <- function(..., checksum, recordKeepingFile, verbose = TRUE) {
  tasks <- list(...)
  if (file.exists(recordKeepingFile) && length(tasks[[1]]) > 0) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, col_types = readr::cols())
    tasks$checksum <- checksum
    tasks <- tibble::as_tibble(tasks)
    if (all(names(tasks) %in% names(recordKeeping))) {
      idx <- getKeyIndex(recordKeeping[, names(tasks)], tasks)
    } else {
      idx = c()
    }
    tasks$checksum <- NULL
    if (length(idx) > 0) {
      text <- paste(sprintf("%s = %s", names(tasks), tasks[idx,]), collapse = ", ")
      ParallelLogger::logInfo("Skipping ", text, " because unchanged from earlier run")
      tasks <- tasks[-idx, ]
    }
  }
  return(tasks)
}

getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 || length(key[[1]]) == 0 || !all(names(key) %in% names(recordKeeping))) {
    return(c())
  } else {
    key <- unique(tibble::as_tibble(key))
    recordKeeping$idxCol <- 1:nrow(recordKeeping)
    idx <- merge(recordKeeping, key)$idx
    return(idx)
  }
}

recordTasksDone <- function(..., checksum, recordKeepingFile, incremental = TRUE) {
  if (!incremental) {
    return()
  }
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(recordKeepingFile)) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, col_types = readr::cols())
    idx <- getKeyIndex(list(...), recordKeeping)
    if (length(idx) > 0) {
      recordKeeping <- recordKeeping[-idx, ]
    }
  } else {
    recordKeeping <- tibble::tibble()
  }
  newRow <- tibble::as_tibble(list(...))
  newRow$checksum <- checksum
  newRow$timeStamp <-  Sys.time()
  recordKeeping <- dplyr::bind_rows(recordKeeping, newRow)
  readr::write_csv(recordKeeping, recordKeepingFile)
}

saveIncremental <- function(data, fileName, ...) {
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(fileName)) {
    previousData <- readr::read_csv(fileName, col_types = readr::cols())
    idx <- getKeyIndex(list(...), previousData)
    if (length(idx) > 0) {
      previousData <- previousData[-idx, ] 
    }
    data <- dplyr::bind_rows(previousData, data)
  } 
  readr::write_csv(data, fileName)
}
