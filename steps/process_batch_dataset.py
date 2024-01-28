import json, os, subprocess, docker, time
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step B || Generating datasets in 20s with Flink batch operations!'
LOGGER_NAME = 'dataset_ingest'

def process():
    logger = ColorLogger(LOGGER_NAME).get_logger()
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    logger.info(MODULE_MESSAGE)
    process_counter = 0
    try:
        with open(f"{os.getenv('PROCESS_CONFIG_PATH')}") as process_config_file:
            process_config = json.load(process_config_file).get('batch')
            process_path = process_config.get('process_command')[
                len(process_config.get('process_command')) - 1
            ]
            process_sum = len(process_config.get('process_list'))
            for process in process_config.get('process_list'):
                process_counter += 1
                process_file_path =  f"{process_path}/{process}.py"
                command_list = process_config.get('process_command')
                command_list[len(command_list) - 1] = process_file_path
                logger.info(f"Running {process}, {process_counter} out {process_sum}.")
                subprocess.Popen(
                    command_list, 
                    stdout=subprocess.PIPE,
                    shell=True
                ).wait()
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
        jobmanager_container = docker_client.containers.get("jobmanager")
        taskmanager_container = docker_client.containers.get("taskmanager")
        jobmanager_container.stop()
        taskmanager_container.stop()
        docker_client.close()