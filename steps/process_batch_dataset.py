import json, os, subprocess, time
from libs.utils.docker import DockerUtils
from libs.utils.subprocess import SubprocessUtil
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step B || Generating datasets with Apache Flink batch operations!'
DOCKER_ACCESS = '/var/run/docker.sock'

def process(logger):
    time.sleep(10)
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    SubprocessUtil.allow_execute_pyflink_flies()
    docker_util = DockerUtils(DOCKER_ACCESS)
    try:
        with open(f"{os.getenv('PROCESS_CONFIG_PATH')}") as process_config_file:
            null_logging = open("NUL","w")
            if os.getenv('LOG_LEVEL') == 'DEBUG':
                null_logging = None
            process_config_files = json.load(process_config_file).get('batch')
            process_list = process_config_files.get('process_list')
            process_sum = len(process_list)
            for process in process_list:
                index = process_list.index(process) + 1
                command_list = list(process_config_files.get('process_command'))
                command_list.append(process)
                logger.info(f"Running {process}, {index} out of {process_sum}.")
                logger.debug(command_list)
                subprocess.Popen(
                    command_list,
                    stdin=null_logging,
                    stdout=null_logging,
                    stderr=null_logging
                ).wait()
                logger.info(f"Done!")
    except IOError as ioe:
        logger.error("IO exception with trace:")
        logger.exception(ioe)
    except TimeoutError() as timeout:
        logger.error("Timeout with process")
        logger.exception(timeout.strerror)
    except Exception as e:
        logger.error("Something weird happened")
        logger.exception(e)
    finally:
        logger.info("Restarting flink cluster...")
        docker_util.restart_container("jobmanager")
        docker_util.restart_container("taskmanager")