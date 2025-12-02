#!/bin/bash

##############################################################
## Objective : Copy file from HDFS to local staging
## Command   : sh extraction_eban_file.sh <SCHEMA_NAME_IN> <SCHEMA_NAME_OUT> <LOCATION_TABLE_IN> <LOCATION_TABLE_OUT> <MODE> <POST_DATE> <DATE_FORMAT> <FILE_NAME_IN> <LOCAL_STORE_MAIN_PATH>
## Return    : Exit_code( 0 or 1) and Target PATH in Local staging
## Update    : 2024-03-26
##############################################################

####====Input Parameter====####

## SCHEMA NAME
SCHEMA_NAME_IN=$1
SCHEMA_NAME_OUT=$2

## TABLE NAME
TABLE_NAME_IN=$3
TABLE_NAME_OUT=$4

## HDFS PATH
LOCATION_TABLE_IN=$5
LOCATION_TABLE_OUT=$6

## RUN MODE
<<comment
----NOTE----
##--OUT--##
- Table Partition mode [INITIAL, YEAR, MONTH, APPEND (DAY)]
- Table Non-Partition mode [ REPALCE ]

##--IN--##
- Mode : [ IN_REPALCE , IN_APPEND ]

comment

MODE=$7

## DATE : YYYY-MM-DD
DATE=$8 # POST_DT

## FORMAT DATE Ex. YYYY-M-D , YYYYMMDD , YYYY-MM
DATE_FORMAT=$9

## File type for IN Mode
FILE_NAME_IN=${10}

## PATH ON LOCAL FOR STORE DATA FROM HDFS
LOCAL_STORE_MAIN_PATH=${11}

## System area
SYSTEM_AREA=${12}
####=================####
###### SHOW PARAMETER ######
# echo "===========SHOW PARAMETER================="
# echo "SCHEMA_NAME_IN : $SCHEMA_NAME_IN"
# echo "SCHEMA_NAME_OUT : $SCHEMA_NAME_OUT"
# echo "TABLE_NAME_IN : $TABLE_NAME_IN"
# echo "TABLE_NAME_OUT : $TABLE_NAME_OUT"
# echo "LOCATION_TABLE_OUT : $LOCATION_TABLE_IN"
# echo "LOCATION_TABLE_OUT : $LOCATION_TABLE_OUT"
# echo "MODE : $MODE"
# echo "DATE : $DATE"
# echo "FILE_NAME_IN : $FILE_NAME_IN"
# echo "=========================================="
######----------------######

####====DEFINE FUNCTIONS====####

##-------------Function : Extract Year by Format date-------------##
extract_year() {
    local date_input="$1"
    local date_format="$2"

    if [[ $date_format =~ Y{4} ]]; then
        ## 'YYYY'
        local month=$(date -d "$date_input" "+%Y")
        echo "$month"
    else
        ## No 'YYYY' found in date_format
        # Return a default value or an empty string
        echo "" # Return empty string
    fi
}
## Function : Extract Month by Format date ##
extract_month() {
    local date_input="$1"
    local date_format="$2"

    if [[ $date_format =~ M{2} ]]; then
        ## 'MM'
        local month=$(date -d "$date_input" "+%m")
        echo "$month"
    elif [[ $date_format =~ M ]]; then
        ## 'M'
        local month=$(date -d "$date_input" "+%-m")
        echo "$month"
    else
        ## No 'MM' or 'M' found in date_format
        # Return a default value or an empty string
        echo "" # Return empty string
    fi
}
## Function : Extract Day by Format date ##
extract_day() {
    local date_input="$1"
    local date_format="$2"

    if [[ $date_format =~ D{2} ]]; then
        ##'DD'
        local day=$(date -d "$date_input" "+%d")
        echo "$day"
    elif [[ $date_format =~ D ]]; then
        ##'D'
        local day=$(date -d "$date_input" "+%-d")
        echo "$day"
    else
        ## No 'DD' or 'D' found in date_format
        # Return a default value or an empty string
        echo "" # Return empty string
    fi
}
##----------------------------------------------------------------##
## Function : Clear And Create Destination DIR ##
function clearAndcreateDIR {

    if [ -d "$1" ]; then  # ลบเฉพาะ partition ที่เอาลงใหม่

        # Remove Target DIR
        rm -Rf $1
        echo "REMOVE DIR : $1 SUCCESS"

        # Create Target DIR
        mkdir -p "$1"
        echo "CREATE DIR : $1 SUCCESS"
    else

        # Create Target DIR
        mkdir -p "$1"
        echo "CREATE DIR : $1 SUCCESS"

    fi

}
####========================####

####==========================================PROCESS==========================================####

echo "####======= START RUN EXTRACTION SCRIPT : $0 =======####"

##---Define Year , Month , Day by Format date---##

# Call func. Extract year
YEAR_VALUE=$(extract_year "$DATE" "$DATE_FORMAT")

# Call func. Extract month
MONTH_VALUE=$(extract_month "$DATE" "$DATE_FORMAT")

# Call func. Extract day
DAY_VALUE=$(extract_day "$DATE" "$DATE_FORMAT")

##----------------------------------------------##

echo "###=== Copy FROM HDFS TO LOCAL BY MODE : $MODE ===###"

#### IN ####
if [ $MODE == "IN_APPEND" ]; then

    # Define New Date by format date
    NEW_DATE=$(echo "$DATE_FORMAT" | sed -e "s/YYYY/$YEAR_VALUE/g" -e "s/MM/$MONTH_VALUE/g" -e "s/M/$MONTH_VALUE/g" -e "s/DD/$DAY_VALUE/g" -e "s/D/$DAY_VALUE/g" )

    # Edit FILE_NAME_IN with DATE for ls file in HDFS
    FILE_NAME_IN_WITH_DATE=${FILE_NAME_IN//YYYYMMDD/$NEW_DATE}  # NOTE : To replace all occurrences -> ${parameter//pattern/string} , To replace the first occurrence -> ${parameter/pattern/string}
    echo "NEW FILE_NAME WITH DATE : $FILE_NAME_IN_WITH_DATE"

    # Check HDFS files before Run cmd copy to local
    if hdfs dfs -test -f $LOCATION_TABLE_IN/$FILE_NAME_IN_WITH_DATE; then

        # Initialize array to store target local store paths ##
        TARGET_LOCAL_STORE_PATHS=()

        # Define Destination target path
        TARGET_LOCAL_STORE_PATH="${LOCAL_STORE_MAIN_PATH}/${SYSTEM_AREA,,}"

        # Create destination DIR
        if ! mkdir -p "$TARGET_LOCAL_STORE_PATH"; then
            echo "CRATE DIR : $TARGET_LOCAL_STORE_PATH SUCCESS"
        else
            echo "DIR : $TARGET_LOCAL_STORE_PATH Already EXISTS"
        fi

        # List file in HDFS with $FILE_NAME_IN_WITH_DATE
        for FILE_PATH in $(hdfs dfs -ls -C $LOCATION_TABLE_IN/$FILE_NAME_IN_WITH_DATE | grep "${LOCATION_TABLE_IN}"); do

            # Extract file name from FILE_PATH (HDFS)
            file_name_hdfs=$(basename "$FILE_PATH")

            # Create full path on_local
            TARGET_LOCAL_STORE_FILE_PATH="$TARGET_LOCAL_STORE_PATH/$file_name_hdfs"

            # CopyFile to Local
            hdfs dfs -get -f $FILE_PATH $TARGET_LOCAL_STORE_PATH
            echo "Copy FROM HDFS : $FILE_PATH TO LOCAL : $TARGET_LOCAL_STORE_FILE_PATH SUCCESS"

            # Add target local store path to array
            TARGET_LOCAL_STORE_PATHS+=("$TARGET_LOCAL_STORE_FILE_PATH")

        done

        ## Convert array to string for return value
        # Join array elements with the delimiter
        delimiter="," # Delimiter
        TARGET_LOCAL_STORE_PATHS_ARRAY_TO_STRING=$(IFS="$delimiter"; echo "${TARGET_LOCAL_STORE_PATHS[*]}")

        ## Return Result and Exit code.
        echo "${TARGET_LOCAL_STORE_PATHS_ARRAY_TO_STRING}"
        exit 0
    else
        ## Return Error message and Exit code.
        echo "ERROR : [ ${LOCATION_TABLE_IN}/${FILE_NAME_IN_WITH_DATE} ] DOES NOT exists on HDFS !"
        exit 1
    fi

elif [ $MODE == "IN_REPLACE" ]; then

    # Check HDFS files before Run cmd copy to local
    if hdfs dfs -test -f $LOCATION_TABLE_IN/$FILE_NAME_IN; then # edit check file_name_date  <-f>

        # Define Destination target path
        TARGET_LOCAL_STORE_PATH="${LOCAL_STORE_MAIN_PATH}/${SYSTEM_AREA,,}"

        # Create destination DIR
        if ! mkdir -p "$TARGET_LOCAL_STORE_PATH"; then
            echo "CRATE DIR : $TARGET_LOCAL_STORE_PATH SUCCESS"
        else
            echo "DIR : $TARGET_LOCAL_STORE_PATH Already EXISTS"
        fi

        # List file in HDFS with $FILE_NAME_IN
        for FILE_PATH in $(hdfs dfs -ls -C $LOCATION_TABLE_IN/$FILE_NAME_IN | grep "${LOCATION_TABLE_IN}"); do

            # CopyFile to Local
            hdfs dfs -get -f $FILE_PATH $TARGET_LOCAL_STORE_PATH
            echo "Copy FROM HDFS : $FILE_PATH TO LOCAL : $TARGET_LOCAL_STORE_PATH/$FILE_NAME_IN SUCCESS"

        done

        ## Return Result and Exit code.
        echo "$TARGET_LOCAL_STORE_PATH/$FILE_NAME_IN"
        exit 0
    else
        ## Return Error message and Exit code.
        echo "ERROR : [ ${LOCATION_TABLE_IN}/${FILE_NAME_IN} ] DOES NOT exists on HDFS !"
        exit 1
    fi

else
    ## Return Error message and Exit code.
    echo "ERROR : MODE is invalid !"
    exit 1
fi
####===========================================================================================####
