import json, os, subprocess, time
from libs.utils.docker import DockerUtils
from libs.utils.subprocess import SubprocessUtil

MODULE_MESSAGE = 'Step B || Generating datasets in 20s with Flink batch operations!'
LOGGER_NAME = 'dataset_ingest'
DOCKER_ACCESS = '/var/run/docker.sock'

def process(logger):
    logger.info(MODULE_MESSAGE)
    SubprocessUtil.allow_execute_pyflink_flies()
    docker_util = DockerUtils(DOCKER_ACCESS)
    try:
        with open(f"{os.getenv('PROCESS_CONFIG_PATH')}") as process_config_file:
            null_logging = open("NUL","w")
            process_config_files = json.load(process_config_file).get('batch')
            process_list = process_config_files.get('process_list')
            process_sum = len(process_list)
            ops_prefix  = process_config_files.get('ops_prefix')
            for process in process_list:
                index = process_list.index(process) + 1
                process_file_path =  f"{ops_prefix}/{process}"
                command_list = list(process_config_files.get('process_command'))
                logger.debug(f"Process file path: {process_file_path}")
                command_list.append(process_file_path)
                logger.info(f"Running {process}, {index} out of {process_sum}.")
                logger.debug(command_list)
                subprocess.Popen(command_list, stdout = null_logging).wait()
                logger.info(f"Done!")
    except IOError as ioe:
        logger.error("IO exception with trace:")
        logger.exception(ioe)
    except TimeoutError() as timeout:
        logger.error("Timeout with process")
        logger.exception(timeout.strerror)
    except Exception as e:
        logger.error("Something wired happened")
        logger.exception(e)
    finally:
        logger.info("Shutting down flink batch cluster.")
        docker_util.stop_container("jobmanager")
        docker_util.stop_container("taskmanager")