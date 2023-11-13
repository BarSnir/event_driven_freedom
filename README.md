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