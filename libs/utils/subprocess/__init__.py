import subprocess

class SubprocessUtil:

    @staticmethod
    def allow_docker_access(access_path):
        subprocess.call(f"chmod 777 {access_path}", shell=True)
    @staticmethod
    def allow_execute_pyflink_flies():
        subprocess.call("chmod -R +x /opt/flink/ops", shell=True)
    @staticmethod
    def get_process_file_path(process_config):
        return process_config.get('process_command')[
            len(process_config.get('process_command')) - 1
        ]

