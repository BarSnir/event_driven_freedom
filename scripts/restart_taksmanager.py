import docker

docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
print('restarting task managers')
docker_client.containers.get("jobmanager").restart()
docker_client.containers.get("event_driven_freedom-taskmanager-1").restart()
docker_client.containers.get("event_driven_freedom-taskmanager-2").restart()