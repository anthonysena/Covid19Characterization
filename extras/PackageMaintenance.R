# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of Covid19TargetAndOutcomeCharacterization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Format and check code ---------------------------------------------------
OhdsiRTools::formatRFolder()
OhdsiRTools::checkUsagePackage("Covid19TargetAndOutcomeCharacterization")
OhdsiRTools::updateCopyrightYearFolder()

# Create manual -----------------------------------------------------------
unlink("extras/Covid19TargetAndOutcomeCharacterization.pdf")
shell("R CMD Rd2pdf ./ --output=extras/Covid19TargetAndOutcomeCharacterization.pdf")

pkgdown::build_site()


# Insert cohort definitions from ATLAS into package -----------------------
cohortGroups <- read.csv("inst/settings/CohortGroups.csv")
for (i in 1:nrow(cohortGroups)) {
  ParallelLogger::logInfo("* Importing cohorts in group: ", cohortGroups$cohortGroup[i], " *")
  ROhdsiWebApi::insertCohortDefinitionSetInPackage(fileName = file.path("inst", cohortGroups$fileName[i]),
                                                   baseUrl = Sys.getenv("baseUrl"),
                                                   insertTableSql = TRUE,
                                                   insertCohortCreationR = FALSE,
                                                   generateStats = FALSE,
                                                   packageName = "Covid19TargetAndOutcomeCharacterization")
}
unlink("inst/cohorts/InclusionRules.csv")

# Create the list of combinations of T, TwS, TwoS for the combinations of strata
colNames <- c("name", "cohortId") # Use this to subset to the columns of interest
# Target cohorts
covidCohorts <- read.csv("inst/settings/CohortsToCreateCovid.csv")
influenzaCohorts <- read.csv("inst/settings/CohortsToCreateInfluenza.csv")
targetCohorts <- rbind(covidCohorts, influenzaCohorts)
targetCohorts <- targetCohorts[, match(colNames, names(targetCohorts))]
# Strata cohorts
bulkStrata <- read.csv("inst/settings/BulkStrata.csv")
bulkStrata <- bulkStrata[, match(colNames, names(bulkStrata))]
atlasCohortStrata <- read.csv("inst/settings/CohortsToCreateStrata.csv")
atlasCohortStrata <- atlasCohortStrata[, match(colNames, names(atlasCohortStrata))]
strata <- rbind(bulkStrata, atlasCohortStrata)
# Get all of the unique combinations of target + strata
targetStrataCP <- do.call(expand.grid, lapply(list(targetCohorts$cohortId, strata$cohortId), unique))
names(targetStrataCP) <- c("targetId", "strataId")
targetStrataCP$cohortId <- (targetStrataCP$targetId * 1000000) + (targetStrataCP$strataId*10)
tWithS <- targetStrataCP
tWithoutS <- targetStrataCP
tWithS$cohortId <- tWithS$cohortId + 1
tWithS$cohortType <- "TwS"
tWithoutS$cohortId <- tWithoutS$cohortId + 2
tWithoutS$cohortType <- "TwoS"
targetStrataXRef <- rbind(tWithS, tWithoutS)
# Write out the final targetStrataXRef
readr::write_csv(targetStrataXRef, "inst/settings/targetStrataXref.csv")


# Store environment in which the study was executed -----------------------
OhdsiRTools::insertEnvironmentSnapshotInPackage("Covid19TargetAndOutcomeCharacterization")