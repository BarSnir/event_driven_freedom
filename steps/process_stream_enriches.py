import os, json, subprocess, time
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step D || Generating Apache Flink stream operations!'

def process(logger, backoff_seconds=120):
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    try:
        with open(f"{os.getenv('PROCESS_CONFIG_PATH')}") as process_config_file:
            null_logging = open("NUL","w")
            if os.getenv('LOG_LEVEL') == 'DEBUG':
                null_logging = None
            process_config_files = json.load(process_config_file).get('stream')
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
                )
                logger.info(f"Done!")
                time.sleep(backoff_seconds)
    except IOError as ioe:
        logger.error("IO exception with trace:")
        logger.exception(ioe)
    except TimeoutError() as timeout:
        logger.error("Timeout with process")
        logger.exception(timeout.strerror)