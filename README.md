# Description
Project that demonstrate how to migrate single source of truth to event-driven project.

# Project Architecture
https://drive.google.com/file/d/1xcJzDR0OytQTqq8ymZlrWXqOCxrBSMuW/view?usp=sharing

# Usage:
- Attention! 
    Please note that that project wrote on ARM processor,
    you might want to replace the ```platform``` field at docker-compose.yaml.
    The wanted value you might need to replace is ```linux/arm64/v8``` to ```linux/amd64```.

- Simply run command ```docker-compose up``` in repo dir.

- Relevant Port for the project are:
    ```http://localhost:8081 - Apache Flink UI```.
    ```http://localhost:9021 - Confluent control center```.
    ```http://localhost:5601 - Kibana```.
    ```http://localhost:9000 - Minio Gui```.
    ```http://localhost:7474 - Neo4j Database```.

## Failure recovery
The project is configured with checkpoints enabled, for jobs to recover from a taskmanager failure.
When a job is started, flink creates a subdir named after the jobId under `./flink-checkpoints`, and stores its last 3 checkpoints there.
Once restarted after a failure, a job connector will start consuming kafka messages from the offset defined in the checkpoint is was recovered from, and not from the one specified in the job. (i.e. `earliest`)
Refer to [flink checkpoints docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/) to learn more.

### Exploring flink failure recovery
1. Start the project `docker compose up`
2. Checkout the checkpoints ui at http://localhost:8081/#/job/running -> pick a job -> *Checkpoints* -> *History*
3. View the actual checkpoint files created per job, at `./flink-checkpoints/<job_id>/chk-num`
3. trigger an external failure: `docker restart taskmanager`
4. go to http://localhost:8081/#/task-manager -> choose the tm -> *Logs*. search for the line indicating the job was restarted from a checkpoint:
```console
org.apache.flink.runtime.state.heap.HeapRestoreOperation     [] - Finished restoring from state handle: KeyGroupsStateHandle <checkpoint details>
```

# Todo features:
- Complex price drop mechanism.
- Monitor all the stacks.
- Draw.io update.