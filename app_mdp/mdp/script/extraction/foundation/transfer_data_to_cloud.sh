#!/bin/bash

##############################################################
#Objective : transfer file in directory from Staging (Local) to AZ (CLOUD) with azcopy .
#Command Execution : sh transfer_eban_file.sh <LOCAL_PATH> <CLOUD_PATH> <MODE>
#Created Date : 2024-03-26
##############################################################


### Input Variable ###
LOCAL_PATH=$1
CLOUD_PATH=$2
MODE=$3

###----CONFIG----###

## Read Config file
source /app_mdp/mdp/config/extraction/azure_azcopy_config.cfg  ## [ $AZ_CONTAINER_URL, $AZ_CONTAINER_NAME, $AZ_SAS_TOKEN, $TRANSFER_RATE ]
###--------------###

####====DEFINE FUNCTIONS====####
###----Check Status azcopy transfer----###
check_upload_status() {
    local output_azcopy_cp_string="$1"

    ##----Check transfer complete from log (terminal)----##
    local AZCOPY_CP_Total_Transfers=$(echo "$output_azcopy_cp_string" | grep "Total Number of Transfers" | grep -o -E '[0-9]+')
    # echo "Total_Transfers : $AZCOPY_CP_Total_Transfers"

    local AZCOPY_CP_Total_Transfers_File=$(echo "$output_azcopy_cp_string" | grep "Number of File Transfers Completed" | grep -o -E '[0-9]+')
    # echo "Total_Transfers : $AZCOPY_CP_Total_Transfers_File"

    local AZCOPY_CP_Total_Transfers_Folder=$(echo "$output_azcopy_cp_string" | grep "Number of Folder Transfers Completed" | grep -o -E '[0-9]+')
    # echo "Total_Transfers : $AZCOPY_CP_Total_Transfers_Folder"

    # Calculate Transfer complete (NOTE: Total Number of Transfers - (Number of File Transfers Completed + Number of Folder Transfers Completed) = 0 is correct)
    local CAL_RESULT_TRANSFER=$(echo "$AZCOPY_CP_Total_Transfers - ($AZCOPY_CP_Total_Transfers_File +  $AZCOPY_CP_Total_Transfers_Folder)" | bc)
    ##--------------------------------------------------##

    ##--Return Number of Transfer complete--##
    echo $CAL_RESULT_TRANSFER
}
###------------------------------------###
####==========================================PROCESS==========================================####

echo "###======= START RUN TRANSFER FILES IN DIR(LOCAL) TO CLOUD SCRIPT : $0 =======###"

echo "##=== TRANSFER FILES FROM LOCAL TO CLOUD BY MODE : $MODE ===##"

# ## Check DIR on Local Staging
# if [ -d "$LOCAL_PATH" ]; then

if [ $MODE == "IN_APPEND" ]; then
    echo "MODE : $MODE"

    ## Convert the $LOCAL_PATH into an array
    delimiter="," # Delimiter
    IFS="$delimiter" read -r -a ARRAY_PATH_FILE_LIST <<< "$LOCAL_PATH"

    ## Loop upload file to cloud
    for file_path in "${ARRAY_PATH_FILE_LIST[@]}"; do

        ## Check DIR on Local Staging
        if [ -f "$file_path" ]; then

            # Edit PATH for ON CLOUD
            file_name=$(basename "$file_path")
            NEW_FILE_PATH_FOR_ON_CLOUD="$CLOUD_PATH/$file_name"
            echo "CONVERT PATH_LOCAL TO ON_CLOUD : $NEW_FILE_PATH_FOR_ON_CLOUD"

            # Upload file to cloud
            echo "Uplaod File : ${file_path} (ON LOCAL) TO ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${NEW_FILE_PATH_FOR_ON_CLOUD} (ON CLOUD)"
            OUTPUT_AZCOPY_CP=$(azcopy cp "${file_path}" "${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${NEW_FILE_PATH_FOR_ON_CLOUD}?$AZ_SAS_TOKEN" --cap-mbps=$TRANSFER_RATE)

            # Display log azcopy
            echo "$OUTPUT_AZCOPY_CP"

            # Exit_code form azcopy cp
            EXIT_CODE_AZCOPY_CP=$?

            ##===Check status azcopy cp===##
            if [ $EXIT_CODE_AZCOPY_CP -eq 0 ]; then
                ##--Check Number of Transfer complete--##

                # Call Func. cal number of transfer
                CAL_RESULT_TRANSFER=$(check_upload_status "$OUTPUT_AZCOPY_CP")

                # Check number of transfer
                if [ $CAL_RESULT_TRANSFER -eq 0 ]; then
                    echo "UPLOAD TO CLOUD SUCCESS"
                    echo "AZ FULL PATH : ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${NEW_FILE_PATH_FOR_ON_CLOUD}"
                else
                    echo "ERROR : UPLOAD FAILED !"
                    exit 1
                fi
                ##-------------------------------------##
            else
                echo "ERROR : UPLOAD FAILED"
                exit 1
            fi
            ##============================##

        else
            echo "ERROR : [ $file_path ]  DOES NOT exists on Local Staging !"
            exit 1
        fi

    done

    ## Return value ##
    echo "$CLOUD_PATH"
    exit 0

elif [ $MODE == "IN_REPLACE" ]; then

    echo "MODE : $MODE"
    ## Check DIR on Local Staging
    if [ -f "$LOCAL_PATH" ]; then

        # Upload All file in DIR (LOCAL) TO CLOUD #
        echo "Uplaod File in : ${LOCAL_PATH} (ON LOCAL) TO ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH}/ (ON CLOUD)"
        OUTPUT_AZCOPY_CP=$(azcopy cp "${LOCAL_PATH}" "${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH}/?$AZ_SAS_TOKEN" --cap-mbps=$TRANSFER_RATE) # Note : Use the wildcard symbol (*) to upload the contents without copying the containing directory itself

        # Display log azcopy
        echo "$OUTPUT_AZCOPY_CP"

        # Get Exit code form azcopy cp
        EXIT_CODE_AZCOPY_CP=$?

        ##===Check status azcopy cp===##
        if [ $EXIT_CODE_AZCOPY_CP -eq 0 ]; then
            ##--Check Number of Transfer complete--##

            # Call Func. cal number of transfer
            CAL_RESULT_TRANSFER=$(check_upload_status "$OUTPUT_AZCOPY_CP")

            # Check number of transfer
            if [ $CAL_RESULT_TRANSFER -eq 0 ]; then
                echo "UPLOAD TO CLOUD SUCCESS"
                echo "AZ FULL PATH : ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}/${CLOUD_PATH}"

                ## Return value ##
                echo "${CLOUD_PATH}"
                exit 0

            else
                echo "ERROR : UPLOAD FAILED !"
                exit 1
            fi
            ##-------------------------------------##
        else
            echo "ERROR : UPLOAD FAILED"
            exit 1
        fi
        ##============================##
    else
        echo "ERROR : [ $LOCAL_PATH ]  DOES NOT exists on Local Staging !"
        exit 1
    fi

else
    ####--------------------MODE : OUT--------------------####
    echo "MODE : $MODE"

    ## Check DIR on Local Staging
    if [ -d "$LOCAL_PATH" ]; then

        ##---Remove file before Upload---##
        # Remove file in Container
        echo "##--RUN AZCOPY REMOVE : ${AZ_CONTAINER_NAME}${CLOUD_PATH} (ON CLOUD)...--##"
        azcopy rm "${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}${CLOUD_PATH}?$AZ_SAS_TOKEN" --recursive=true # Remove All file in DIR
        # echo "exit code rm : $?"
        echo "##----------------------------------------------------------------------------------##"
        ##-------------------------------##

        ##---Upload an entire directory by using a SAS token---##
        # Upload All file in DIR (LOCAL) TO CLOUD #
        echo "Uplaod File in : ${LOCAL_PATH} (ON LOCAL) TO ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}${CLOUD_PATH} (ON CLOUD)"
        OUTPUT_AZCOPY_CP=$(azcopy cp "${LOCAL_PATH}/*" "${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}${CLOUD_PATH}?$AZ_SAS_TOKEN" --recursive=true --cap-mbps=$TRANSFER_RATE) # Note : Use the wildcard symbol (*) to upload the contents without copying the containing directory itself
        echo "$OUTPUT_AZCOPY_CP"
        ##-----------------------------------------------------##

        # Get Exit code form azcopy cp
        EXIT_CODE_AZCOPY_CP=$?

        ##===Check status azcopy cp===##
        if [ $EXIT_CODE_AZCOPY_CP -eq 0 ]; then
            ##--Check Number of Transfer complete--##

            # Call Func. cal number of transfer
            CAL_RESULT_TRANSFER=$(check_upload_status "$OUTPUT_AZCOPY_CP")

            # Check number of transfer
            if [ $CAL_RESULT_TRANSFER -eq 0 ]; then
                echo "UPLOAD TO CLOUD SUCCESS"
                echo "AZ FULL PATH : ${AZ_CONTAINER_URL}/${AZ_CONTAINER_NAME}${CLOUD_PATH}"

                ## Return value ##
                echo "${CLOUD_PATH}"
                exit 0

            else
                echo "ERROR : UPLOAD FAILED !"
                exit 1
            fi
            ##-------------------------------------##
        else
            echo "ERROR : UPLOAD FAILED"
            exit 1
        fi
        ##============================##
    else
        echo "ERROR : [ $LOCAL_PATH ]  DOES NOT exists on Local Staging !"
        exit 1

    fi
fi
####===========================================================================================####
