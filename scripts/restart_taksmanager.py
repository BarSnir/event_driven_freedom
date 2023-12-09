import docker

docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
print('restarting task manager')
docker_client.containers.get("taskmanager").restart()