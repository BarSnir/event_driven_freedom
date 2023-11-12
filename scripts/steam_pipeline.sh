#!/bin/bash
echo 'Step D || Starting Full enrich, take a look at control center at http://localhost:9021'

# Yes the order is necessary :)
declare -a pyflink_scripts=(
    [1]="aggregate_images"
    [2]="enrich_full_order"
)

for key in "${!pyflink_scripts[@]}"; do
    echo Job ${pyflink_scripts[$key]} Is Running! $key out of 2.
   ./bin/flink run \
      --detached \
      --jobmanager jobmanager:8081 \
      -pyclientexec /usr/local/bin/python3 \
      -pyexec /usr/local/bin/python3 \
      -py /opt/flink/ops/${pyflink_scripts[$key]}.py &
    echo The job ${pyflink_scripts[$key]} is runnig!
    sleep 250;
done