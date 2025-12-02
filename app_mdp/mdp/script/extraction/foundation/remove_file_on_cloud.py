"""Remove file on cloud storage."""
# import: standard
import shlex
import subprocess
import sys


#####=============DEFINE FUNCTION=============#####
####-----FUNC : Read Config file-----####
def read_config_file(path_file):
    """Function for reading azcopy configurations."""
    try:
        ## Read config file and convert to dict ##
        config_obj = {}  # define empty dict
        with open(path_file) as fp:
            for line in fp.readlines():
                tokens = line.strip().replace('"', "").split("=", maxsplit=1)
                config_obj[tokens[0]] = tokens[1]
        ## Return config (dict) ##
        return config_obj
    except Exception as err:
        print("ERROR FROM SCRIPT : CONTROL-FILES (Read Config file.) \n", str(err))
        sys.exit(1)


#### FUNC : RUN CMD ####
def run_command(command):
    """Function for running a shell command."""
    command_list = shlex.split(command)
    result = subprocess.run(command_list, capture_output=True, text=True, shell=False)
    output = result.stdout.strip()
    exit_code = result.returncode
    return output, exit_code


####---------------------------------####
#### FUNC : List file from AZ [ON CLOUD] ####
def get_list_files_from_az_with_azcopy(path_on_az, azcopy_config_obj):
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


####---------------------------------####
def remove_file_on_az(path_file_on_az, azcopy_config_obj):
    """Function for removing file on ADLS storage using Azcopy."""
    ### Define cmd azcopy list file from az path ###
    az_remove_file_command = (
        'azcopy rm "{az_url}/{az_container}/{path_file_in_container}?{az_sas_token}"'.format(
            az_url=azcopy_config_obj["AZ_CONTAINER_URL"],
            az_container=azcopy_config_obj["AZ_CONTAINER_NAME"],
            path_file_in_container=path_file_on_az,
            az_sas_token=azcopy_config_obj["AZ_SAS_TOKEN"],
        )
    )

    ### RUN CMD. ###
    az_remove_file_cmd_output, cmd_exit_code = run_command(
        az_remove_file_command
    )  # Call func : run cmd with <STRING>

    ### Check exit_code And show Output
    print("##---Show azcopy log : remove file on az---##")
    print(az_remove_file_cmd_output)
    if cmd_exit_code == 0:
        print("Remove file on az success")
    else:
        print("Remove file on az failed!")
    print("##-----------------------------------##")


####---------------------------------####
def main(az_path, file_name, post_date, format_date, azcopy_config):
    """Main function of remove_file_on_cloud."""
    ### List file in az container ###
    cloud_file_list = get_list_files_from_az_with_azcopy(az_path, azcopy_config_obj=azcopy_config)
    if len(cloud_file_list) == 0:
        print(f"PATH (ON AZ) : {az_path} is empty !")
    else:
        ### Create CMD with file name for remove file target on az ###
        pattern_replace_formatdate = "YYYYMMDD"
        len_str_pattern_replace_formatdate = len(pattern_replace_formatdate)
        date_split = post_date.split("-")
        ## Create file name for seach
        pos = file_name.find(pattern_replace_formatdate)
        if pos != -1:
            ## Replace date to format date
            replace_date_formatdate = (
                format_date.replace("YYYY", date_split[0])
                .replace("MM", date_split[1])
                .replace("M", str(int(date_split[1])))
                .replace("DD", date_split[2])
                .replace("D", str(int(date_split[2])))
            )
            ## Split file name
            filename_for_check = file_name[: pos + len_str_pattern_replace_formatdate]
            ## Replace date to file name
            filename_for_check = filename_for_check.replace(
                pattern_replace_formatdate, replace_date_formatdate
            )
        else:
            filename_for_check = file_name
        ## Check file name in az and remove file on az
        for file_name_az in cloud_file_list:
            if file_name_az.startswith(filename_for_check) and not file_name_az.endswith(".ctl"):
                az_file_path = f"{az_path}/{file_name_az}"
                print(f"Start Remove : /{az_file_path} on az ...")
                remove_file_on_az(az_file_path, azcopy_config_obj=azcopy_config)


#####====================================================================================#####

####---DEFINE PATH CONFIG FILE---####
AZ_COPY_CONFIG_PATH = "/app_mdp/mdp/config/extraction/azure_azcopy_config.cfg"
####-----------------------------####

if __name__ == "__main__":
    if len(sys.argv) == 5:
        ## Extract input from command line ##
        az_path = sys.argv[1].strip("/")
        file_name = sys.argv[2]
        post_date = sys.argv[3]
        format_date = sys.argv[4]

        ## Read az config ##
        azcopy_config = read_config_file(AZ_COPY_CONFIG_PATH)

        ## CALL MAIN ##
        main(az_path, file_name, post_date, format_date, azcopy_config)
        sys.exit(0)

    else:
        print("ERROR : Input arguments for this script error")
        sys.exit(1)
