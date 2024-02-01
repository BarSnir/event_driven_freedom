import subprocess

class SubprocessUtil:

    @staticmethod
    def allow_docker_access(access_path):
        subprocess.call(f"chmod 777 {access_path}", shell=True)
    @staticmethod
    def allow_execute_pyflink_flies():
        subprocess.call("chmod -R +x /opt/flink/ops", shell=True)

