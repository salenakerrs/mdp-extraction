#!/bin/bash

##############################################################
#Objective : Main Script for control workflow move file from eban to MDF
#Command : sh mdp_extraction_foundation.sh <JOB_NAME> <YYYY-MM-DD>
#Update  : 2024-03-18
##############################################################

####====Input Parameter====####
JOB_NAME=$1
POST_DT=$2 # Date format : YYYY-MM-DD
####=======================####

echo "########============================  START RUN MAIN : $0 ============================########"

###----Valid Date Format----###
if [[ ! $POST_DT =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR : The input POST_DT $POST_DT is NOT a valid date string in the YYYY-MM-DD format."
  exit 1 # STOP RUN SCRIPT !!
fi
###-------------------------###

######====Define PATH files====######
## CONFIG RUN WITH JOB
CONFIG_FILE_WITH_JOBNM_PATH="/app_mdp/mdp/config/extraction/eban_extraction_config.txt"
## Extraction
EXTRACTION_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/extraction_eban_file.sh"
## Transfer files in directory
TRANSFER_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/transfer_data_to_cloud.sh"
## Validate
VALIDATE_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/validate_file.py"
## Control-file
CONTROL_FILE_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/extraction_control_file.py"
## Transfer 1 file to cloud
TRANSFER_FILE_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/transfer_file_to_cloud.sh"
## Remove files in path AZ
REMOVE_FILE_AZ_SCRIPT_PATH="/app_mdp/mdp/script/extraction/foundation/remove_file_on_cloud.py"
## Python Interpreter Path
PYTHON_PATH="/app/script_file/env/py39/bin/python3"

###====READ MDP CONFIG FILE AND DEFINE VAR====##
source /app_mdp/mdp/config/extraction/mdp_extraction_config.cfg # USE : [ $KEYTAB , $SERVICE , $LOCAL_STORE_MAIN_PATH , $CLOUD_STORE_MAIN_PATH ]

#####====READ JOB DETAIL FROM JOB_CONFIG_FILE====#####

## Read and convert to array
CONFIG_VALUE_ARRAY=( $(grep -m1 -w $JOB_NAME $CONFIG_FILE_WITH_JOBNM_PATH | tr '|' "\n") )

## Check if CONFIG_VALUE_ARRAY is not null
if [ ${#CONFIG_VALUE_ARRAY[@]} -eq 0 ]; then
    echo "Error: No values found for job name '$JOB_NAME' in the config file."
    exit 1
fi
#####============================================#####

### Add create log to /app_log_mdp

###---SHOW CONFIG VALUE BY JOB_NAME---###
echo "######------- SHOW CONFIG VALUE FROM CONFIG_FILE BY JOB_NAME : $JOB_NAME , POST_DT : $POST_DT ---######"
echo "JOB_NAME : ${CONFIG_VALUE_ARRAY[0]}"
echo "SCHEMA_IN  : ${CONFIG_VALUE_ARRAY[1]}"
echo "SCHEMA_OUT  : ${CONFIG_VALUE_ARRAY[2]}"
echo "TABLE_NAME_IN  : ${CONFIG_VALUE_ARRAY[3]}"
echo "TABLE_NAME_OUT  : ${CONFIG_VALUE_ARRAY[4]}"
echo "HDFS_LOCATION_IN : ${CONFIG_VALUE_ARRAY[5]}"
echo "HDFS_LOCATION_OUT : ${CONFIG_VALUE_ARRAY[6]}"
echo "MODE : ${CONFIG_VALUE_ARRAY[7]}"
echo "DATE_FORMAT : ${CONFIG_VALUE_ARRAY[8]}"
echo "FILE_NAME (optional) : ${CONFIG_VALUE_ARRAY[9]}"
echo "SYSTEM_AREA : ${CONFIG_VALUE_ARRAY[10]}"
echo "###---Main Path---###"
echo "LOCAL_STORE_MAIN_PATH : ${LOCAL_STORE_MAIN_PATH}"
echo "CLOUD_STORE_MAIN_PATH : ${CLOUD_STORE_MAIN_PATH}"
echo "######----------------------------------------------------------------------------------------------######"
###-----------------------------------###

####====DEFINE FUNCTIONS====####
####========================####

#########=============================================PROCESS=============================================#########

echo "#######============================ START PROCESS : MAIN SCRIPT ============================#######"

####=========================EXTRACTION=========================####

echo "#####=============== CALL SCRIPT : $EXTRACTION_SCRIPT_PATH ===============#####"
## RUN kinit
kinit -k -t "$KEYTAB" "$SERVICE"

## Call Extract script
RESULT_FROM_EXTRACTION=$(sh ${EXTRACTION_SCRIPT_PATH} ${CONFIG_VALUE_ARRAY[1]} ${CONFIG_VALUE_ARRAY[2]} ${CONFIG_VALUE_ARRAY[3]} ${CONFIG_VALUE_ARRAY[4]} ${CONFIG_VALUE_ARRAY[5]} ${CONFIG_VALUE_ARRAY[6]} ${CONFIG_VALUE_ARRAY[7]} ${POST_DT} ${CONFIG_VALUE_ARRAY[8]} ${CONFIG_VALUE_ARRAY[9]} ${LOCAL_STORE_MAIN_PATH} ${CONFIG_VALUE_ARRAY[10]} )

## Exit_code form EXTRACTION_SCRIPT
EXIT_CODE_EXTRACTION=$?
# echo "run-a-program returned $EXIT_CODE_EXTRACTION"


## CHECK ERROR FROM EXTRACTION SCRIPT
if [ $EXIT_CODE_EXTRACTION -eq 0 ]; then

    # Display output from Extraction script
    echo "$RESULT_FROM_EXTRACTION"
    # Display the returned variable from extract_script
    LAST_RESULT_FROM_EXTRACTION=$(echo "$RESULT_FROM_EXTRACTION" | tail -n 1)
    echo "DATA LOACTION ON STAGING: $LAST_RESULT_FROM_EXTRACTION"
    echo "#####====================== END SCRIPT : EXTRACTION ======================#####"

else
    # Display output from Extraction script
    echo "$RESULT_FROM_EXTRACTION"
    echo "!!!ERROR : EXTRACTION !!!"
    echo "#####====================== END SCRIPT : EXTRACTION ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi
####============================================================####

####=========================TRANSFER=========================####
## Create PATH ON_CLOUD (MODE : IN ONLY)
PATH_ON_CLOUD="$CLOUD_STORE_MAIN_PATH/${CONFIG_VALUE_ARRAY[10],,}"

###============REMOVE FILES ON AZ============###
echo "###======= START RUN REMOVE FILES ON AZ=======###"
echo "#####=============== CALL SCRIPT : $REMOVE_FILE_AZ_SCRIPT_PATH ===============#####"
## Call Remove files on AZ script
RESULT_RESULT_FROM_REMOVE_FILE_ON_AZ=$(${PYTHON_PATH} ${REMOVE_FILE_AZ_SCRIPT_PATH} ${PATH_ON_CLOUD} ${CONFIG_VALUE_ARRAY[9]} ${POST_DT} ${CONFIG_VALUE_ARRAY[8]})

## Exit_code form REMOVE_FILE_AZ_SCRIPT
EXIT_CODE_REMOVE_FILES_ON_AZ=$?

## CHECK ERROR FROM EXTRACTION SCRIPT
if [ $EXIT_CODE_REMOVE_FILES_ON_AZ -eq 0 ]; then

    # Display output from Extraction script
    echo "$RESULT_RESULT_FROM_REMOVE_FILE_ON_AZ"
    echo "#####====================== END SCRIPT : REMOVE FILE ON AZ ======================#####"

else
    # Display output from Extraction script
    echo "$RESULT_RESULT_FROM_REMOVE_FILE_ON_AZ"
    echo "ERROR : VALIDATE SCRIPT ERROR!! "
    echo "#####====================== END SCRIPT : REMOVE FILE ON AZ ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi

echo "#####=============== CALL SCRIPT : $TRANSFER_SCRIPT_PATH ===============#####"
###=========================TRANSFER DATA=========================###
## Call Transfer script
RESULT_FROM_TRANSFER_SCRIPT=$(sh ${TRANSFER_SCRIPT_PATH} ${LAST_RESULT_FROM_EXTRACTION} $PATH_ON_CLOUD ${CONFIG_VALUE_ARRAY[7]} )

## Exit_code form TRANSFER_SCRIPT
EXIT_CODE_TRANSFER=$?
# echo "EXIT_CODE_TRANSFER: $EXIT_CODE_TRANSFER"

## CHECK ERROR FROM EXTRACTION SCRIPT
if [ $EXIT_CODE_TRANSFER -eq 0 ]; then

    # Display output from Extraction script
    echo "$RESULT_FROM_TRANSFER_SCRIPT"
    # Display the returned variable from extract_script
    LAST_RESULT_FROM_TRANSFER_SCRIPT=$(echo "$RESULT_FROM_TRANSFER_SCRIPT" | tail -n 1)
    echo "DATA LOACTION ON CLOUD: $LAST_RESULT_FROM_TRANSFER_SCRIPT"
    echo "#####====================== END SCRIPT : TRANSFER ======================#####"

else
    # Display output from Extraction script
    echo "$RESULT_FROM_TRANSFER_SCRIPT"
    echo "ERROR : TRANSFER SCRIPT ERROR!! "
    echo "#####====================== END SCRIPT : TRANSFER ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi
####==========================================================####

####=========================VALIDATE FILE BETWEEN ON-PREM AND ON-CLOUD=========================####
#####------VALIDATE : Compare file between On_Cloud and On_Prem------####
echo "#####=============== CALL SCRIPT : $VALIDATE_SCRIPT_PATH ===============#####"
## Call Validate script
RESULT_FROM_VALIDATE_SCRIPT=$(${PYTHON_PATH} ${VALIDATE_SCRIPT_PATH} ${LAST_RESULT_FROM_EXTRACTION} ${LAST_RESULT_FROM_TRANSFER_SCRIPT} )

## Exit_code form VALIDATE_SCRIPT
EXIT_CODE_VALIDATE=$?
# echo "run-a-program returned (VALIDATE) $EXIT_CODE_VALIDATE"
# echo "EXIT_CODE_VALIDATE: $EXIT_CODE_VALIDATE"

## CHECK ERROR FROM EXTRACTION SCRIPT
if [ $EXIT_CODE_VALIDATE -eq 0 ]; then

    # Display output from Extraction script
    echo "$RESULT_FROM_VALIDATE_SCRIPT"
    echo "#####====================== END SCRIPT : VALIDATE ======================#####"

else
    # Display output from Extraction script
    echo "$RESULT_FROM_VALIDATE_SCRIPT"
    echo "ERROR : VALIDATE SCRIPT ERROR!! "
    echo "#####====================== END SCRIPT : VALIDATE ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi
#########=================================================================================================#########

####=========================GENERATE CONTROL-FILE=========================####
echo "#####=============== CALL SCRIPT : $CONTROL_FILE_SCRIPT_PATH ===============#####"
## RUN kinit
kinit -k -t "$KEYTAB" "$SERVICE"

###--Create control-file path in local

## Create file name (Control-file)
# Check if the string contains 'YYYYMMDD'
if [[ ${CONFIG_VALUE_ARRAY[9]} == *YYYYMMDD* ]]; then
    # Remove everything after'YYYYMMDD' in string and concat string date format
    FILE_NAME_CONTROL_FILE="${CONFIG_VALUE_ARRAY[9]%%YYYYMMDD*}YYYYMMDD"

else
    # Remove type file from file name
    FILE_NAME_CONTROL_FILE="${CONFIG_VALUE_ARRAY[9]%.*}"
fi

# Create path and file_name
PATH_CONTROL_FILE_ON_LOCAL="${LOCAL_STORE_MAIN_PATH}/${CONFIG_VALUE_ARRAY[10],,}/${FILE_NAME_CONTROL_FILE}"

## Call Generate Control-file script
RESULT_FROM_CONTROL_FILE_SCRIPT=$(${PYTHON_PATH} ${CONTROL_FILE_SCRIPT_PATH} ${CONFIG_VALUE_ARRAY[1]} ${CONFIG_VALUE_ARRAY[3]} ${POST_DT} ${CONFIG_VALUE_ARRAY[8]} ${PATH_CONTROL_FILE_ON_LOCAL} ${LAST_RESULT_FROM_EXTRACTION} )

## Exit_code form VALIDATE_SCRIPT
EXIT_CODE_CONTROL_FILE=$?

## CHECK ERROR FROM CONTROL_FILE SCRIPT
if [ $EXIT_CODE_CONTROL_FILE -eq 0 ]; then

    # Display output from Gen control-file script
    echo "$RESULT_FROM_CONTROL_FILE_SCRIPT"
    # Display the returned variable from Gen control-file
    LAST_RESULT_FROM_CONTROL_FILE_SCRIPT=$(echo "$RESULT_FROM_CONTROL_FILE_SCRIPT" | tail -n 1)
    echo "CONTROL-FILE PATH: $LAST_RESULT_FROM_CONTROL_FILE_SCRIPT"
    echo "#####====================== END SCRIPT : GENERATE CONTROL-FILE ======================#####"

else
    # Display output from Gen control-file
    echo "$RESULT_FROM_CONTROL_FILE_SCRIPT"
    echo "ERROR : CONTROL-FILE SCRIPT ERROR!! "
    echo "#####====================== END SCRIPT : GENERATE CONTROL-FILE ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi
#########=================================================================================================#########

####=========================UPLOAD CONTROL-FILE TO CLOUD=========================####
echo "###=============== CALL SCRIPT : $TRANSFER_FILE_SCRIPT_PATH FOR UPLOAD CONTROL-FILE ===============###"

## Create control-file path
PATH_CONTROL_FILE_ON_CLOUD="${CLOUD_STORE_MAIN_PATH}/${CONFIG_VALUE_ARRAY[10],,}"

## Call Script Upload file
RESULT_FROM_TRANSFER_CONTROL_FILE_SCRIPT=$(sh ${TRANSFER_FILE_SCRIPT_PATH} ${LAST_RESULT_FROM_CONTROL_FILE_SCRIPT} ${PATH_CONTROL_FILE_ON_CLOUD}/ )

## Exit_code form TRANSFER_CONTROL_FILE_SCRIPT
EXIT_CODE_TRANSFER_CONTROL_FILE=$?

## CHECK ERROR FROM EXTRACTION SCRIPT
if [ $EXIT_CODE_TRANSFER_CONTROL_FILE -eq 0 ]; then

    # Display output from Upload Control-file script
    echo "$RESULT_FROM_TRANSFER_CONTROL_FILE_SCRIPT"
    echo "#####====================== END SCRIPT : TRANSFER CONTROL-FILE TO CLOUD ======================#####"

else
    # Display output from Upload Control-file script
    echo "$RESULT_FROM_TRANSFER_CONTROL_FILE_SCRIPT"
    echo "ERROR : TRANSFER CONTROL-FILE ERROR!! "
    echo "#####====================== END SCRIPT : TRANSFER CONTROL-FILE TO CLOUD ======================#####"
    exit 1 # STOP RUN SCRIPT !!
fi
