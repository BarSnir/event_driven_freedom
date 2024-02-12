import docker

class DockerUtils:

    def __init__(self, connection_file_path):
        self.connection_file_path = connection_file_path
        self.docker_client = self._create_docker_client()

    def _create_docker_client(self):
        return docker.DockerClient(
            base_url=f'unix:/{self.connection_file_path}'
        )

    def stop_container(self, container_name):
        self.docker_client.containers.get(container_name).stop()
