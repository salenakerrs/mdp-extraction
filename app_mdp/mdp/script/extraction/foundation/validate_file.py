"""Validate Azcopy Files."""
#######======IMPORT LIBRARY======#######
# import: standard
import configparser
import os
import shlex
import subprocess
import sys

#######==========================#######

#######============================INPUT PARAMETER==================================#######
try:
    print("###======= START RUN : VALIDATE FILES SCRIPT =======###")

    # Define argv[0]= <script_file>.py , argv[1]=PATH_ON_PREM, argv[2]=PATH_ON_CLOUD
    if len(sys.argv) == 3:
        PATH_ON_PREM = sys.argv[1]  # Full Path
        PATH_ON_CLOUD = sys.argv[2]  # Path on Cloud (Exclude : AZ_URL , AZ_Contriner_name)
    else:
        print("Input arguments for this script error")
        sys.exit(1)
except Exception as e:
    print(f"Input arguments for this script error: {e}")
    sys.exit(1)

#######=============================================================================######

####---DEFINE PATH CONFIG FILE---####
AZ_COPY_CONFIG_PATH = "/app_mdp/mdp/config/extraction/azure_azcopy_config.cfg"
####-----------------------------####

#####=============DEFINE FUNCTION=============#####


#### FUNC : READ AZ CONFIG ####
def read_config_azcopy(config_file_path):
    """Function for reading azcopy configurations."""
    try:
        ### Read file azcopy config ###
        with open(config_file_path, "r") as f:
            az_copy_config_string = (
                "[dummy_section]\n" + f.read()
            )  # Add dummy section before read <.cfg> file (Note : .cfg file must have section)

        ###--Define azcopy config from config file to variable--###
        config_obj = configparser.RawConfigParser()
        config_obj.read_string(az_copy_config_string)

        ### Define azcopy config to dict ###
        azcopy_config_obj = {
            "AZ_CONTAINER_URL": config_obj["dummy_section"]["AZ_CONTAINER_URL"].replace('"', ""),
            "AZ_CONTAINER_NAME": config_obj["dummy_section"]["AZ_CONTAINER_NAME"].replace('"', ""),
            "AZ_SAS_TOKEN": config_obj["dummy_section"]["AZ_SAS_TOKEN"].replace('"', ""),
        }

        ### Return config obj. (dict) ###
        return azcopy_config_obj
    except Exception as err:
        ### Show error ###
        print(str(err))
        ### !!STOP RUN CODE!! ###
        sys.exit(1)


#### FUNC : RUN CMD ####
def run_command(command):
    """Function for running a shell command."""
    command_list = shlex.split(command)
    result = subprocess.run(command_list, capture_output=True, text=True, shell=False)
    output = result.stdout.strip()
    exit_code = result.returncode
    return output, exit_code


#### FUNC : List file from AZ [ON CLOUD] ####
def get_list_files_from_az_with_azcopy(azcopy_config_obj, path_on_az):
    """Function for getting a list of file from ADLS with Azcopy."""

    ### Define cmd azcopy list file from az path ###
    az_list_file_command = (
        'azcopy list "{az_url}/{az_container}/{path_in_container}?{az_sas_token}"'.format(
            az_url=azcopy_config_obj["AZ_CONTAINER_URL"],
            az_container=azcopy_config_obj["AZ_CONTAINER_NAME"],
            path_in_container=path_on_az,
            az_sas_token=azcopy_config_obj["AZ_SAS_TOKEN"],
        )
    )  # define string cmd azcopy list
    # print("azcopy list cmd : ",az_list_file_command)

    ### RUN CMD. ###
    az_list_file_cmd_output, cmd_exit_code = run_command(
        az_list_file_command
    )  # Call func : run cmd with <STRING>

    ### Check exit_code And show Output
    if cmd_exit_code == 0:
        # print("----Show output azcopy list----\n",az_list_file_cmd_output)

        ### Clean up output from run cmd ###
        cloud_file_list = az_list_file_cmd_output.split("\n")  # split string to list
        ### Clean up each file path to remove irrelevant information ###
        cloud_file_list = [
            row.replace("INFO: ", "").split(";")[0].split("/")[-1].strip(".")
            for row in cloud_file_list
        ]

        ### Return result (type : list) ###
        # print("----Show output azcopy list edit----\n",cloud_file_list)
        return cloud_file_list

    else:
        print("!!----Show output azcopy list [ERROR]----!!\n", az_list_file_cmd_output)
        ### !!STOP RUN CODE!! ###
        sys.exit(cmd_exit_code)


#### FUNC : List file from Staging [ON PREM] ####
def get_list_files_from_staging(path_on_local):
    """Function for getting a list of file from staging storage path."""

    ### Define cmd get list files from path in staging ###
    staging_list_file_command = f"find /{path_on_local} -type f"  # define string cmd
    # print(f'staging_list_file_command : {staging_list_file_command}')

    ### RUN CMD. ###
    staging_list_file_cmd_output, cmd_exit_code = run_command(
        staging_list_file_command
    )  # Call func : run cmd with <STRING>

    ### Check exit_code And show Output
    if cmd_exit_code == 0:
        # print("----Show output staging_list_file----\n",staging_list_file_cmd_output)

        ### Clean up output from run cmd ###
        staging_file_list = staging_list_file_cmd_output.split("\n")  # edit
        staging_file_list = [row.split("/")[-1].strip(".") for row in staging_file_list]

        ### Return result (type : list) ###
        # print("----Show output staging_list----\n",staging_file_list)
        return staging_file_list

    else:
        print(
            f"!!----Show output find /{path_on_local} -type f [ERROR]----!!\n",
            staging_list_file_cmd_output,
        )
        ### !!STOP RUN CODE!! ###
        sys.exit(cmd_exit_code)


#### FUNC : Compare Compare the lists of files between ON_PREM and ON_CLOUD ####
def compareListFile(list_file_on_cloud, list_file_on_prem):
    """Function for comparing two list of files."""

    ## Compare the lists of files and find the files that are present in one list but not the other
    file_diff_on_prem = set(list_file_on_prem) - set(list_file_on_cloud)
    file_diff_cloud = set(list_file_on_cloud) - set(list_file_on_prem)
    file_diff = file_diff_on_prem.union(file_diff_cloud)

    ### Return result ###
    # print(f'result File_diff : {file_diff} ')
    return file_diff


#### FUNC : Check set_1 is a subset of set_2 ####
def checkSubset(set_1, set_2):
    """Function for checking if a set is a subset of another set."""
    if set_1.issubset(set_2):
        # All values in set_1 are present in set_2
        return set()  # return empty set
    else:
        # Not all values in set_1 are present in set_2
        return set_1


#### FUNC : MAIN ####
def main(azcopy_config_obj):
    """Main function of validate_file."""

    ## List file in az container ##
    cloud_file_list = get_list_files_from_az_with_azcopy(azcopy_config_obj, PATH_ON_CLOUD)

    ## Split string to list with delimiter
    part_file_on_prem = PATH_ON_PREM.split(",")

    ## Get file_name from path ##
    file_name_on_prem = [os.path.basename(path) for path in part_file_on_prem]

    ## Compare the lists of files between ON_PREM and ON_CLOUD with Check ON_PREM is a subset of ON_CLOUD
    file_diff = checkSubset(set(file_name_on_prem), set(cloud_file_list))

    ## Print the comparison results ##
    if not file_diff:
        print("✔ Cloud is equal to On_Prem")
        sys.exit(0)
    else:
        print(
            f"✖ cloud is not equal to On_Prem , there are {len(file_diff)} file diff in these schema:"
        )
        print("\n".join(list({file.split("/")[0] for file in file_diff})))
        sys.exit(1)


#####=========================================#####

##################################################main##################################################

if __name__ == "__main__":
    #### Read Config ####
    azcopy_config_obj = read_config_azcopy(AZ_COPY_CONFIG_PATH)
    #### RUN FUNC : MAIN ####
    main(azcopy_config_obj)
