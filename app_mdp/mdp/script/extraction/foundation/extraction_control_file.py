#######======IMPORT LIBRARY======#######
# import: standard
import os
import re
import sys
from datetime import date

# import: external
from impala.dbapi import connect

# import logging
#######==========================#######


#####=============DEFINE FUNCTION=============#####
####-----FUNC : Read Config file-----####
def read_config_file(path_file):
    try:
        ## Read config file and convert to dict ##
        config_obj = {}  # define empty dict
        with open(path_file) as fp:
            for line in fp.readlines():
                tokens = line.strip().replace('"', "").split("=")
                config_obj[tokens[0]] = tokens[1]
        ## Return config (dict) ##
        return config_obj
    except Exception as err:
        print("ERROR FROM SCRIPT : CONTROL-FILES (Read Config file.) \n", str(err))
        sys.exit(1)


####---------------------------------####
####-----FUNC : Write Count total record of target_table to a .ctl file-----####
def write_ctl_file(output_path, content_dict):
    try:
        # Extract directory path from output_path
        directory_ctl = os.path.dirname(output_path)

        # Create directory if it doesn't exist
        if not os.path.exists(directory_ctl):
            print(f"Create DIR CONTROL-FILE : {directory_ctl}")
            os.makedirs(directory_ctl)

        # Open file in write mode and write content
        with open(output_path, mode="w") as ctl_file:
            # Write column headers
            ctl_file.write("|".join(content_dict.keys()) + "\n")

            # Write data
            ctl_file.write("|".join(str(val) for key, val in content_dict.items()))

        print(f"CONTROL-FILE created successfully : {output_path}")
    except Exception as err:
        print("ERROR FROM SCRIPT : CONTROL-FILES (create control-file)\n", str(err))
        sys.exit(1)


####-----------------------------------------------####
####-----FUNC : Create database connection obj-----####
def getimpalaConnection(config):
    ## Create connection obj ##
    CONN = connect(
        host=config["IMPALA_HOST"],
        port=config["IMPALA_PORT"],
        auth_mechanism="GSSAPI",
        kerberos_service_name="impala",
    )

    ## Return connection obj ##
    return CONN


####-----------------------------------------------####
####-----FUNC : Query Data from Database with impala-----####
def query_impala(query_string, impala_conn):
    # Create cursor
    impala_cursor = impala_conn.cursor()

    try:
        # Execute the query
        impala_cursor.execute(query_string)

        # Fetch all results
        results = impala_cursor.fetchall()

        return results
    finally:
        # Close cursor and connection
        impala_cursor.close()


####-----------------------------------------------####
####-----------------------------------------------####
def add_quotes_by_type(value, type_value):
    if "INT" in type_value.upper():
        return str(int(value))
    else:
        return f"'{value}'"


####-----------------------------------------------####
####-----FUNC : Generate where condition----####
def generateConditionForPartitionTableWithDate(list_partition_detail, list_date):
    list_partition_and_value = []

    for partition_detail in list_partition_detail:
        ## Convert partition_name to lower case ##
        partition_name_lower = partition_detail[0].lower()

        ## Map date_columns and date ##
        if partition_name_lower == "pos_dt":
            partition_Condition = (
                f"{partition_detail[0]}='{list_date[0]}-{list_date[1]}-{list_date[2]}'"
            )
            list_partition_and_value.append(partition_Condition)
        elif partition_name_lower == "pos_yr":
            partition_Condition = (
                f"{partition_detail[0]}={add_quotes_by_type(list_date[0],partition_detail[1])}"
            )
            list_partition_and_value.append(partition_Condition)
        elif partition_name_lower == "pos_mo":
            partition_Condition = (
                f"{partition_detail[0]}={add_quotes_by_type(list_date[1],partition_detail[1])}"
            )
            list_partition_and_value.append(partition_Condition)
        else:
            print(f"{partition_detail[0]} is not support and Skip generate condition")

    ## Join to WHERE Condition ##
    if len(list_partition_and_value) > 1:
        where_Condition = "WHERE " + " AND ".join(list_partition_and_value)
    elif len(list_partition_and_value) == 1:
        where_Condition = f"WHERE {list_partition_and_value[0]}"
    else:
        where_Condition = ""

    ## Return String WHERE Condition by partition ##
    return where_Condition


####-----------------------------------------------####
####-----------------------------------------------####


#####-----FUNC : MAIN-----#####
def main(schema_name, table_name, post_dt, format_date, config_obj, path_output, path_file_name):
    try:
        #### Split Date from STR ####
        date_split = post_dt.split("-")  # Split YEAR-MONTH-DAY ---> [ 'YEAR' , 'MONTH' , 'DAY' ]

        #### Create connection to Impala
        impala_conn = getimpalaConnection(config_obj)

        ####---------Define SQL quey count record by Mode-----------####
        ## Query : SHOW CREATE TABLE for Check partition table ##
        result_show_createTable = query_impala(
            f"SHOW CREATE TABLE {schema_name}.{table_name}", impala_conn
        )

        ###--Define SQL Query count record--###

        ## Find all occurrences of strings starting with "pos_"
        regex_pattern_check_partition = r"\bpos_\w+\s+\w+\b"

        # Check if "pos_" columns exist using walrus operator
        if list_columns_for_where := re.findall(
            regex_pattern_check_partition, result_show_createTable[0][0]
        ):
            # Split column_name and type to list
            list_columns_split_name_and_type = [
                col_detail.split() for col_detail in list_columns_for_where
            ]

            ## Create SQL query with where condition ##
            where_Condition = generateConditionForPartitionTableWithDate(
                list_columns_split_name_and_type, date_split
            )

            # Concat query with <where_Condition>
            sql_query_count = f"SELECT COUNT(*) FROM {schema_name}.{table_name} {where_Condition}"
            ##---------------------------------------##
        else:
            ## Not have date_column ##
            sql_query_count = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"  # Count All record
        ####--------------------------------------------------------####

        ####-----Run Query Count Record-----####
        print(f"QUERY COUNT TOTAL RECORD : {sql_query_count}")
        result_count_record = query_impala(sql_query_count, impala_conn)

        ### Disply Total Record from query ###
        print(f"RESULT TOTAL RECORD : {result_count_record[0][0]}")

        ####-----------Create Control-file-----------####
        ## Control-file detail ##
        date_file_with_format_date = (
            format_date.replace("YYYY", date_split[0])
            .replace("MM", date_split[1])
            .replace("M", str(int(date_split[1])))
            .replace("DD", date_split[2])
            .replace("D", str(int(date_split[2])))
        )
        full_path_control_file = (
            f'/{path_output.strip("/").replace("YYYYMMDD" ,date_file_with_format_date )}.ctl'
        )

        ## Write control-file ##
        # Define header and value in control-file
        data_detail_in_control_file = {
            "date_of_data": f"{date_split[0]}{date_split[1]}{date_split[2]}",
            "date_of_generated_data": date.today().strftime("%Y%m%d"),
            "number_of_records": result_count_record[0][0],
            "source_file_name": ",".join(
                [os.path.basename(path) for path in path_file_name.split(",")]
            ),
        }
        print("##---Show detail value in Control-file---##")
        print("\n".join([f"{key}: {value}" for key, value in data_detail_in_control_file.items()]))
        print("##---------------------------------------##")
        # Call Func. write file
        write_ctl_file(full_path_control_file, data_detail_in_control_file)
        ####-----------------------------------------####

        ## Return output ##
        print("##---PATH CONTROL-FILE---##")
        print(full_path_control_file)
    except Exception as err:
        print("ERROR FROM  SCRIPT : CONTROL-FILES \n", str(err))
        sys.exit(1)
    finally:
        # Close connection
        impala_conn.close()


#####---------------------#####
#####=========================================#####

#####=============DEFINE PATH=============#####
MDP_EXTRACTION_CONFIG_FILE_PATH = "/app_mdp/mdp/config/extraction/mdp_extraction_config.cfg"
#####=====================================#####

#####=============DEFINE LOGGING=============#####
# logging.basicConfig(level = logging.DEBUG)
#####========================================#####

#####################=================================MAIN#=================================#####################

if __name__ == "__main__":
    try:
        print("###======= START RUN : CONTROL-FILES SCRIPT =======###")

        # Define argv[0] = <script_file>.py , argv[1] = SCHEMA_NAME, argv[2] = TABLE_NAME , argv[3]=

        if len(sys.argv) == 7:
            ######--------------Define values from INPUT PARAMETER--------------######
            schema_name = sys.argv[1].lower()
            table_name = sys.argv[2].lower()
            post_dt = sys.argv[3]  # Date format : YYYY-MM-DD
            format_date = sys.argv[4]
            path_output = sys.argv[5]
            path_file_from_extraction = sys.argv[6]  ### New

            ######--------------------------------------------------------------######
            ######------CALL FUNC : Read Database Config------######
            mdp_extraction_config = read_config_file(MDP_EXTRACTION_CONFIG_FILE_PATH)
            ######------CALL FUNC : MAIN------######
            main(
                schema_name,
                table_name,
                post_dt,
                format_date,
                mdp_extraction_config,
                path_output,
                path_file_from_extraction,
            )
            sys.exit(0)

        else:
            print("ERROR FROM  SCRIPT : CONTROL-FILES")
            print("Input arguments for this script error")
            sys.exit(1)
    except Exception as err:
        print("ERROR FROM  SCRIPT : CONTROL-FILES \n", str(err))
        sys.exit(1)
#######=============================================================================######
