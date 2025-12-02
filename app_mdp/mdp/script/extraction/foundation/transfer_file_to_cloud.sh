#!/bin/bash

##############################################################
#Objective : transfer 1 file from Staging (Local) to AZ (CLOUD) with azcopy .
#Command Execution : sh transfer_file_to_cloud.sh <LOCAL_PATH> <CLOUD_PATH>
#Created Date : 2024-02-12
##############################################################


### Input Variable ###
LOCAL_PATH=$1
CLOUD_PATH=$2

###----CONFIG----###

## Read Config file
source /app_mdp/mdp/config/extraction/azure_azcopy_config.cfg  ## [ $AZ_CONTAINER_URL, $AZ_CONTAINER_NAME, $AZ_SAS_TOKEN, $TRANSFER_RATE ]
###--------------###

####==========================================PROCESS==========================================####

echo "###======= START RUN : TRANSFER FILE TO CLOUD SCRIPT =======###"

## Check file on Local Staging
if [ -f "$LOCAL_PATH" ]; then

    ###---Upload file by using a SAS token---###

    # Upload All file TO CLOUD #
    echo "Uplaod File in : ${LOCAL_PATH} (ON LOCAL) TO ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH} (ON CLOUD)"
    OUTPUT_AZCOPY_CP=$(azcopy cp "${LOCAL_PATH}" "${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH}?$AZ_SAS_TOKEN" --cap-mbps=2000)
    echo "$OUTPUT_AZCOPY_CP"
    ###-----------------------------------------------------###

    ## Exit_code form azcopy cp
    EXIT_CODE_AZCOPY_CP=$?
    # echo "EXIT_CODE_AZCOPY_CP : $EXIT_CODE_AZCOPY_CP"

    ### CHECK EXIT CODE FROM RUN : azcopy cp
    if [ $EXIT_CODE_AZCOPY_CP -eq 0 ]; then
        echo "UPLOAD TO CLOUD SUCCESS"
        echo "AZ FULL PATH : ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH}"
        exit 0
    else
        echo "ERROR : UPLOAD FILE FAILED !"
        exit 1
    fi
    ###-------------------------------------###
else
    echo "ERROR : [ $LOCAL_PATH ]  DOES NOT exists on Local Staging !"
    exit 1
fi
####===========================================================================================####
