import subprocess, time

class SubprocessUtil:

    @staticmethod
    def allow_docker_access(access_path):
        subprocess.call(f"chmod 777 {access_path}", shell=True)
    @staticmethod
    def allow_execute_pyflink_flies():
        time.sleep(10)
        subprocess.call("chmod -R +x /opt/flink/ops", shell=True)
