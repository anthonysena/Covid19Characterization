# # Make sure to install all dependencies (not needed if already done) -------------------------------
# 
# # Prevents errors due to packages being built for other R versions: 
# Sys.setenv("R_REMOTES_NO_ERRORS_FROM_WARNINGS" = TRUE)
# 
# # First, it probably is best to make sure you are up-to-date on all existing packages. 
# # Important: This code is best run in R, not RStudio, as RStudio may have some libraries 
# # (like 'rlang') in use.
# update.packages(ask = "graphics")
# 
# # When asked to update packages, select '1' ('update all') (could be multiple times)
# # When asked whether to install from source, select 'No' (could be multiple times)
# install.packages("devtools")
# devtools::install_github("ohdsi-studies/Covid19TargetAndOutcomeCharacterization")

# Running the package -------------------------------------------------------------------------------
library(Covid19TargetAndOutcomeCharacterization)

# Optional: specify where the temporary files (used by the ff package) will be created:
fftempdir <- if (Sys.getenv("FFTEMP_DIR") == "") "~/fftemp" else Sys.getenv("FFTEMP_DIR")
options(fftempdir = fftempdir)

# Details for connecting to the server:
dbms = Sys.getenv("DBMS")
user <- if (Sys.getenv("DB_USER") == "") NULL else Sys.getenv("DB_USER")
password <- if (Sys.getenv("DB_PASSWORD") == "") NULL else Sys.getenv("DB_PASSWORD")
server = Sys.getenv("DB_SERVER")
port = Sys.getenv("DB_PORT")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = password,
                                                                port = port)

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

# Details specific to the database:
databaseId <- "MDCR_TEST"
databaseName <- "MDCR_TEST"
databaseDescription <- "MDCR_TEST"

# Details for connecting to the CDM and storing the results
outputFolder <- file.path("E:/Covid19TargetAndOutcomeCharacterization", databaseId)
cdmDatabaseSchema <- "CDM_IBM_MDCR_V942.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- paste0("AS_cov19_full_", databaseId)
cohortStagingTable <- paste0(cohortTable, "_stg")
cohortOutcomeTable <- paste0(cohortTable, "_out")

# For uploading the results. You should have received the key file from the study coordinator:
keyFileName <- "c:/home/keyFiles/study-data-site-covid19.dat"
userName <- "study-data-site-covid19"

# Use this to run the study. The results will be stored in a zip file called 
# 'AllResults_<databaseId>.zip in the outputFolder. 
runStudy(connectionDetails = connectionDetails,
         cdmDatabaseSchema = cdmDatabaseSchema,
         cohortDatabaseSchema = cohortDatabaseSchema,
         cohortStagingTable = cohortStagingTable,
         cohortTable = cohortTable,
         cohortOutcomeTable = cohortOutcomeTable,
         oracleTempSchema = cohortDatabaseSchema,
         exportFolder = outputFolder,
         databaseId = databaseId,
         databaseName = databaseName,
         databaseDescription = databaseDescription,
         #cohortGroups = c("influenza"),
         incremental = TRUE,
         minCellCount = 5) 

#CohortDiagnostics::preMergeDiagnosticsFiles(outputFolder)
#CohortDiagnostics::launchDiagnosticsExplorer(outputFolder)

# Upload results to OHDSI SFTP server:
#uploadResults(outputFolder, keyFileName, userName)
