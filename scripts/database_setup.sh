#!/bin/bash
sleep 30
echo 'Step A || Creating databases & tables'
python3 /opt/flink/project/main.py

echo 'Step B || Generating datasets in 20s with Flink batch opersions!'
echo 'Invited to take a look at job statuses at http://localhost:8081'
# Yes the order is necessary :)
declare -a pyflink_scripts=(
    [1]="generate_market_info"
    [2]="generate_vehicles"
    [3]="generate_orders" 
    [4]="generate_images"
    [5]="generate_customers"
    [6]="generate_auth_types"
    [7]="generate_improves"
    [8]="generate_media_type"
    [9]="generate_statuses"
)

for key in "${!pyflink_scripts[@]}"; do
    echo Job ${pyflink_scripts[$key]} Is Running! $key out of 9.
    ./bin/flink run \
      --jobmanager jobmanager:8081 \
      -pyclientexec /usr/local/bin/python3 \
      -pyexec /usr/local/bin/python3 \
      -py /opt/flink/ops/${pyflink_scripts[$key]}.py &>/dev/null
    echo The job ${pyflink_scripts[$key]} is Done! ;
done

echo "Time to kafka! PyFlink batch jobs are done!"
echo "Invited to take a look at Kafka's connect oeprtion at http://localhost:9021"