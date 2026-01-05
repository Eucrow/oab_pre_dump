# Check the monthly data of on-board sampling and generate the files to dump it
# in SIRENO database.
#
# The script import the files obtained from the subcontracted company, check
# the data finding errors and fix them when is possible. Then, it generates the
# files to dump in SIRENO database.

# INSTRUCTIONS -----------------------------------------------------------------
# To use this script:
# - The default folder organization is:
#   - data
#     - YYYY (year with four digits)
#       - YYYY_MM (MM is the month with two digits: 01, 02, ..., 12)
#          - originals (folder with the files obtained from the subcontracted
#                       company)
#          - to_sireno (folder with the final files created with this script
#                       ready to dump them SIRENO)
#          - errors (folder with the errors found in the data only if there are)
#          - backup (folder with the backup of the scripts, files used in the
#                     process and final files)
# The files obtained from the subcontracted company must be stored in the
# YYYY_MM/originals folder.
# - The script will generate a excel file ready to dump in SIRENO database. It
# will be stored in the same folder as before ./data/YYYY/YYYY_MM.
# - In the same folder, a subfolder "errors" will be created with the errors
# found in the data, and a subfolder "backup" with the backup of the scripts,
# files used in the process and final files.
# - Before to run the script, change variables in "YOU HAVE ONLY TO CHANGE THIS
# VARIABLES" section.
# - Run all the script

# ► PACKAGES -------------------------------------------------------------------

library(dplyr)
library(gdata) # to use with write.fwf(), in OAB_IPD_to_SIRENO_export_functions.R
# library(devtools)
# install_github("Eucrow/sapmuebase")
library(sapmuebase)
library(sf)

# ---- install archive package
# install.packages("archive")
library(archive)

# ► FUNCTIONS ------------------------------------------------------------------
# Get the complete path for all function's files 
function_files <- list.files(file.path(getwd(), 
                                       "R"), 
                             full.names = TRUE, 
                             recursive = TRUE)

# Import all functions 

sapply(function_files,
       function(x){
         tryCatch({
           
           source(x)
           
         }, error = function(e){
           
           cat("An error occurred:", conditionMessage(e), "\n")
           
         }, warning = function(w){
           
           cat("A warning occurred:", conditionMessage(w), "\n")
           
         }
         )
       })


# ► YOU HAVE ONLY TO CHANGE THIS VARIABLES -------------------------------------

# MONTH in numeric format
MONTH <- 10

# YEAR with four digits
YEAR <- 2025


# list with the file names
files <- list(
  ACCIDENTALS = "accidentales__.txt",
  GEARS = "artes__.txt",
  LITTER = "basura__.txt",
  CATCHES = "capturas__.txt",
  MANDATORY_LANDINGS = "desembarcos_obligatorios__.txt",
  HAULS = "lances__.txt",
  TRIPS = "mareas__.txt",
  SAMPLES_GROUPED = "muestreos_agrupados__.txt",
  RETAIN_LENGTHS = "tallas_muestra_retenida__.txt",
  DISCARD_LENGTHS = "tallas_muestra_descartado__.txt"
)

# this variable is to check if there are any trips already in SIRENO
# OAB_TRIPS_FILE_NAME <- "IEODESMAREAMARCO.TXT"


# ► GLOBAL VARIABLES -----------------------------------------------------------

# month as character
MONTH_AS_CHARACTER <- sprintf("%02d", MONTH)

# list with the ".rar" extension file name

rar_names <- list(lectura = paste0("lectura_ICES_",
                                   MONTH_AS_CHARACTER,
                                   "_",
                                   YEAR,
                                   ".rar"),
                  txt = paste0("txt_ICES_",
                               MONTH_AS_CHARACTER,
                               ".rar"))

# Suffix to path folder (useful when the data of the same month is received
# in different files). If it is not necessary, use NULL, NA or "".
# FOLDER_SUFFIX <- "b"
FOLDER_SUFFIX <- "TEST"

# Path to the data folder
DATA_FOLDER <- file.path(getwd(), "data")

# Path of the base folder
FOLDER_SUFFIX <- ifelse(is.null(FOLDER_SUFFIX) | is.na(FOLDER_SUFFIX) | FOLDER_SUFFIX == "", "", paste0("_", FOLDER_SUFFIX))
BASE_FOLDER <- file.path(getwd(), "data", YEAR, paste0(YEAR, "_", MONTH_AS_CHARACTER, FOLDER_SUFFIX))

# Path where rar files are stored by default 

STORE_DEFAULT_FOLDER <- "C:/Users/alberto.candelario/Downloads"

# Path where a copy of all files used and created will be stored in a share folder

# PATH_SHARE_FOLDER <- "C:/Users/alberto.candelario.ST/Documents/local_nextCloud/SAP_OAB/OAB_data_review/pre_dump_data"

PATH_SHARE_FOLDER <- "C:/Users/alberto.candelario/Documents/personal_nextCloud/SAP_OAB/OAB_data_review/pre_dump_data"

PATH_STORE_SHARED_FILES <- file.path(PATH_SHARE_FOLDER, 
                                    YEAR, 
                                    paste0(YEAR, "_", MONTH_AS_CHARACTER, FOLDER_SUFFIX))

# Create work folders

folder_names <- list(originals = c("originals"),
                     finals = c("finals"),
                     errors = c("errors"), 
                     backup = c("backup"))

folders_path <- lapply(folder_names, 
                       manage_work_folder,
                       BASE_FOLDER)


# Path of the files to import
PATH_IMPORT_FILES <- folders_path[["originals"]]
# Path where the final files are created
PATH_EXPORT_FILES <- folders_path[["finals"]]
# Path where the error files are generated
PATH_ERRORS_FILES <- folders_path[["errors"]]

# Path where the backup files are stored
PATH_BACKUP_FILES <- folders_path[["backup"]]

# Move work files to originals' folder

lapply(rar_names,
       move_file,
       STORE_DEFAULT_FOLDER,
       PATH_IMPORT_FILES)

# Extrat the files inside the compressed file

COMPRESSED_FILE_PATH <- file.path(PATH_IMPORT_FILES, 
                                  rar_names[["txt"]])

archive_extract(COMPRESSED_FILE_PATH,
                PATH_IMPORT_FILES)

# list with all errors found in data frames:
err <- list()


# ► LOAD DATASETS --------------------------------------------------------------
razon <- importCsvSAPMUE("discard_cause.csv")
grouped_species <- importCsvSAPMUE("grouped_species.csv")
metier_target_species <- importCsvSAPMUE("metier_target_species.csv")


# dataset with the Type File and the name of the table of the IPD database
# TODO: create a csv file with this information, store in data folder and
# create a function to create it in data-raw folder.
NAME_FILE_VS_TYPE_FILE <- createNameVsTypeDataset()

# ► IMPORT FILES ---------------------------------------------------------------
oab_ipd <- importOabIpdFiles(files, path = PATH_IMPORT_FILES)
# list2env(oab_ipd, .GlobalEnv)


# FILTER BY MONTH --------------------------------------------------------------
# Only use when IPD send us various months in the same files
# oab_ipd <- filter_by_month(oab_ipd, MONTH_AS_CHARACTER)

# FILTER BY TRIP
# Just in case
# oab_ipd <- filter_by_trips(oab_ipd, "DESNOR24003")

# LIST ID TRIPS
id_trips <- unique(oab_ipd$TRIPS$acronimo)[order(unique(oab_ipd$TRIPS$acronimo))]

# ► FIX FILES ------------------------------------------------------------------
# ╚► Fix discards all measured without subsample ----
#' 1) When a sample is all measured and it isn't subsampled, the variables
#' peso_muestra_total and peso_sub_muestra must be 0.
#' 2) When a sample is all measured and it is subsampled, the variable
#' peso_sub_muestra must be 0.
#' 3) When a sample is not all measured and isn't subsampled, the
#' variable peso_sub_muestra must be equal to peso_muestra_total
#' In the dump process, when this variables are equal to 0, they are updated
#' with the SOP value.
oab_ipd$DISCARD_LENGTHS <- fix_discards_weights(oab_ipd$CATCHES, oab_ipd$DISCARD_LENGTHS)

# ╚► Fix catches all measured ----
#' 1) the field 'peso_muestra' must be always 0
#' 2) When a sample is all measured, the field 'peso' must be 0.
#' In the dump process, when this variables are equal to 0, they are updated
#' with the SOP value.
oab_ipd$CATCHES <- fix_catches_weights(oab_ipd$CATCHES)

# ╚► Fix total weight discarded in not measured hauls ----
#' When a haul is not measured, the total weight discarded must be empty (and
#' not 0)
oab_ipd$HAULS <- fix_discarded_weights_not_measured_haul(oab_ipd$HAULS)

# ╚► Fill variable metier_ieo ----
# TODO: THIS IS NOT LONGER USED BECAUSE THE VARIABLE IS FILLED BY THE
# CONTRACTED COMPANY. DELETE IT.
# TODO: MAYBE MAKE A COHERENCIE CHECK OF especie_objetivo, ESTRATO_RIM AND
# metier_ieo?
#' In HAULS table, the field metier_ieo is not filled by the contracted company.
#' The value of this field depend of 'especie_objetivo' and 'ESTRATO_RIM'. A
#' data set with this combinations is metier_target_species
# WARNING: if there are lots of metier_ieo data as NA, maybe is need to be fixed
# by the contracted company:
# oab_ipd$HAULS <- add_metier_ieo_variable(oab_ipd$HAULS)
#' Example to fix problems:
# levels(oab_ipd$HAULS$especie_objetivo) <- c(levels(oab_ipd$HAULS$especie_objetivo), "VLB")
# oab_ipd$HAULS[oab_ipd$HAULS$ESTRATO_RIM == "BACA_CN" & oab_ipd$HAULS$especie_objetivo == "GAR", "especie_objetivo"] <- "VBL"


# ╚► Number of individuals discarded without weights and lengths ----
# In some hauls, discarded species has number of measured individuals but the weight
# sampled, weight sub-sampled and number of individuals by length is 0. It can
# be an error in the sampling, an error in the keyed process or the SOP is 0.
# This species must be removed because the discards process break down if this
# errors exists.
# An errors file is exported to share with the Area Coordinators in order to
# check it.
oab_ipd$DISCARD_LENGTHS <- error_lengths_discards(oab_ipd$DISCARD_LENGTHS)
#### SEND THE FILE species_to_check_ TO COORDINATORS!!!!!


# ╚► Add ICES statistical rectangle hauls. ----
# The IPD file contains a empty variable called "cuadrícula" which is filled
# with this function, using data obtained from ICES.
oab_ipd$HAULS <- add_ices_rectangle(oab_ipd$HAULS)
oab_ipd$HAULS[which(is.na(oab_ipd$HAULS$cuadricula)), ]

# ► PRE-DUMP CHECKING ----------------------------------------------------------
# ╚► GROUPED SPECIES TABLE WITH DUPLICATED LENGTHS ----
# Find species of a haul with duplicated lengths in gropued species table
# ATENTION: THIS ERROR MUST BE FIXED BEFORE THE UPLOAD. When an error is
# detected, the subcontracted company must be notified to correct it and send
# it back to us fixed.
err_duplicate_lengtsh_in_grouped_species <- duplicate_lengtsh_in_grouped_species(oab_ipd$SAMPLES_GROUPED)

# In case of errors, export to file
# exportCsvSAPMUEBASE(err_duplicate_lengtsh_in_grouped_species,
#                     paste0("err_duplicate_lengtsh_in_grouped_species_", YEAR,
#                            "_", MONTH_AS_CHARACTER,".csv"),
#                     PATH_ERRORS_FILES
#                     )

# ╚► FIELD sub_muestra MUST BE Y ----
# For us, right now this field must be as Y.
# TEST: why? this field is not used in this script. Delete check?
# TODO: explain with the Ricardo's answer by email.
# err$err_sub_muestra_field <- sub_muestra_field(oab_ipd$DISCARD_LENGTHS)

# ╚► CONFORMITY WITH MASTERS ----
err_variable_with_masters <- lapply(oab_ipd, function(x, y) {
  if (variable_exists_in_df("ESTRATO_RIM", x) == TRUE) {
    y$ESTRATO_RIM <- check_variable_with_master(x, "ESTRATO_RIM", "OAB")
  }

  if (variable_exists_in_df("COD_PUERTO", x) == TRUE) {
    y$COD_PUERTO <- check_variable_with_master(x, "COD_PUERTO", "OAB")
  }

  if (variable_exists_in_df("COD_ORIGEN", x) == TRUE) {
    y$COD_ORIGEN <- check_variable_with_master(x, "COD_ORIGEN", "OAB")
  }

  if (variable_exists_in_df("COD_ARTE", x) == TRUE) {
    y$COD_ARTE <- check_variable_with_master(x, "COD_ARTE", "OAB")
  }
}, err)

err$err_variable_with_masters <- bind_rows(err_variable_with_masters)

# ╚► ACRONIM EQUAL IN ALL THE TABLES ----
err_acronyms_in_all_tables <- acronyms_in_all_tables(oab_ipd)
err$err_acronyms_in_all_tables <- bind_rows(err_acronyms_in_all_tables)


# ╚► CHECK GRUOPED SPECIES IN SAMPLES_GROUPED TABLE ----
err$err_grouped_species <- species_in_grouped_species(oab_ipd)

# ╚► MONTH OF THE TRIPS ----
# Maybe is not necessary detect this error in pre dump?
err$err_month <- check_all_month(oab_ipd, MONTH)

# ╚► YEAR ----
# Maybe is not necessary detect this error in pre dump?
err$err_year <- check_all_year(oab_ipd, YEAR)

# ╚► TRIP IS ALREADY SAVED IN SIRENO ----
# err$err_trips_already_in_sireno <- trips_already_in_sireno(oab_ipd$TRIPS)

# ╚► WEIGHTS CATCHES ----
# More detail in errors_weights_flowchart.pdf
# Detect errors in weight caught and sampled weight caught when the sample
# has been all measured
# all catches measured' (todo_medido_captura) means that all the catch of a
# species has been measured. In this cases, the fields 'peso' and 'peso_muestra'
# must be 0.
err$error_weights_catches <- error_weights_catches(oab_ipd$CATCHES)

# ╚► WEIGHTS DISCARDS ----
# More detail in errors_weights_flowchart.pdf
# Detect errors in  discarded weight and sampled discarded weight when the
# sample has been all measured.
# The variable 'todo_medido_descarte' means that all the discard of a
# species has been measured. In this cases, the fields 'peso_descarte' and
# peso_muestra_descarte' must be 0. An exception is when there is a subsample,
# in this cases only 'peso_muestra_descarte' must be 0.
# err$err_weights_discards <- error_weights_discards(oab_ipd$CATCHES, oab_ipd$DISCARD_LENGTHS)
# WARNING: There are a lot of errors of "The discard is not subsampled and is
# not all measured, but the peso_sub_muestra variable is equal to 0". In this
# cases, I assume that there are a mistake in the field "todo_medido_descarte"
# which should be TRUE instead of FALSE. Anyway, this must be checked in
# oab_post_dump


# ╚► DUPLICATED LITTER ----
# Find duplicated litter and measure type in a haul of the survey.
# ATENTION: THIS ERROR MUST BE FIXED BEFORE THE UPLOAD. When an error is
# detected, the subcontracted company must be notified to correct it and send
# it back to us fixed.
err$error_duplicated_litter <- error_duplicated_litter(oab_ipd$LITTER)
# In case of errors, export to file
# exportCsvSAPMUEBASE(err$error_duplicated_litter,
#                     paste0("error_duplicated_litter_", YEAR,
#                            "_", MONTH_AS_CHARACTER,".csv"),
#                     PATH_ERRORS_FILES
#                     )


# ► COMBINE IN ONE DATAFRAME ---------------------------------------------------
# TODO: if there aren't errors, don't combine it in order to avoid an error
errors <- Reduce(bind_rows, err)


# ► EXPORT FILES TO UPLOAD IN SIRENO -------------------------------------------
exportOabListToSireno(oab_ipd, path = PATH_EXPORT_FILES)

# ► SAVE THIS SCRIPT -----------------------------------------------------------
rstudioapi::documentSave()

# ► MAKE BACKUP OF THIS AND RELATED FILES IN MONTHLY FOLDER --------------------
files_to_backup <- c(
  "discard_cause.csv",
  "grouped_species.csv",
  "metier_target_species.csv",
  "format_variables_oab.csv",
  "oab_pre_dump.R",
  "oab_pre_dump_add_ices_rectangle_functions.R",
  "oab_pre_dump_general_functions.R",
  "oab_pre_dump_fix_data_functions.R",
  "oab_pre_dump_export_functions.R"
)

# TODO: create a function to automate this process
files_to_backup_from <- file.path(getwd(), files_to_backup)
files_to_backup_to <- file.path(PATH_BACKUP_FILES, files_to_backup)

# Create backup subfolder in case it doesn't exists:
if (!file.exists(file.path(PATH_BACKUP_FILES))) {
  dir.create(file.path(PATH_BACKUP_FILES))
}
file.copy(files_to_backup_from, files_to_backup_to, overwrite = TRUE)

# ► COPY FILES AND FOLDERS INTO THE SHARE FOLDER -------------------------------

copy_files(BASE_FOLDER, PATH_STORE_SHARED_FILES)

