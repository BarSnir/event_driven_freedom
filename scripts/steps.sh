# !/bin/bash
echo "Pre step - Please prefrom chmod +x /var/run/docker.sock"
# bash $SCRIPTS_PATH_DIR/database_setup.sh
# echo 'Step C || Setup Debezium connector & fetching Data...'
# python3 /opt/flink/project/scripts/restart_taksmanager.py
# curl --location --request PUT 'http://kafka-connect:8083/connectors/debezium_pyflink_v1/config' \
# --header 'Content-Type: application/json' \
# --data '{
#     "name": "debezium_pyflink_v1",
#     "connector.class": "io.debezium.connector.mysql.MySqlConnector",
#     "database.history.kafka.bootstrap.servers": "broker:29092",
#     "database.history.kafka.recovery.attempts": "8",
#     "database.history.kafka.recovery.poll.interval.ms": "10000",
#     "database.history.kafka.topic": "history_pyflink_project_v1",
#     "database.history.skip.unparseable.ddl": "false",
#     "database.hostname": "db",
#     "database.include.list": "production",
#     "database.password": "password",
#     "database.port": "3306",
#     "database.server.id": "1",
#     "database.server.name": "prodcution",
#     "database.ssl.mode": "required",
#     "database.user": "root",
#     "errors.deadletterqueue.context.headers.enable": "true",
#     "errors.deadletterqueue.topic.name": "dlq-debezium",
#     "errors.deadletterqueue.topic.replication.factor": "1",
#     "errors.log.enable": "true",
#     "errors.retry.delay.max.ms": "60000",
#     "errors.retry.timeout": "300000",
#     "errors.tolerance": "all",
#     "event.processing.failure.handling.mode": "fail",
#     "include.query": "true",
#     "include.schema.changes": "true",
#     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
#     "key.converter.schemas.enable": "false",
#     "snapshot.mode": "when_needed",
#     "table.include.list": "production.Vehicles, production.MarketInfo, production.Orders, production.Statuses, production.MediaType, production.Improves, production.Images, production.AuthTypes, production.Customers",
#     "tasks.max": "1",
#     "tombstones.on.delete": "true",
#     "topic.creation.enable" : "true",
#     "topic.creation.default.partitions": "1",
#     "topic.creation.default.replication.factor": "1",
#     "topic.creation.default.cleanup.policy": "compact",  
#     "topic.creation.default.compression.type": "lz4",
#     "topic.creation.default.retention.ms": 100000000,
#     "topic.creation.inventory.partitions": 1,
#     "topic.creation.inventory.cleanup.policy": "compact",
#     "topic.creation.inventory.delete.retention.ms": 100000000,
#     "topic.creation.applicationlogs.replication.factor": 1,
#     "topic.creation.applicationlogs.partitions": 1,
#     "topic.creation.applicationlogs.cleanup.policy": "delete",
#     "topic.creation.applicationlogs.retention.ms": 100000000,
#     "topic.creation.applicationlogs.compression.type": "lz4",
#     "value.converter": "io.confluent.connect.avro.AvroConverter",
#     "value.converter.schema.registry.url": "http://schema-registry:8082",
#     "value.converter.enhanced.avro.schema.support": "false",
#     "transforms": "dropPrefix",
#     "transforms.dropPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
#     "transforms.dropPrefix.regex": "prodcution.production.(.*)",
#     "transforms.dropPrefix.replacement": "$1",
#     "delete.handling​.mode": "none",
#     "connect.meta.data": "false"
# }' &>/dev/null
# echo "Waiting for Debeizuim connector task to be done." 
# sleep 120
bash $SCRIPTS_PATH_DIR/steam_pipeline.sh
echo 'All stream jobs are running, time to setup sink connectors.'
wait