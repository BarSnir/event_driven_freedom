#!/bin/bash
sleep 30
echo 'Step A || Creating databases & tables'
python3 /opt/flink/project/main.py

echo 'Step B || Generating datasets in 20s with Flink batch opersions'
sleep 20
# Yes the order is necessary :)
declare -a pyflink_scripts=(
    "generate_market_info"
    "generate_vehicles"
    "generate_orders" 
    "generate_images"
    "generate_customers"
    "generate_auth_types"
    "generate_improves"
    "generate_media_type"
    "generate_statuses"
)
# 1> /dev/null

for script in ${pyflink_scripts[@]}; do
    ./bin/flink run \
      --jobmanager jobmanager:8081 \
      -pyclientexec /usr/local/bin/python3 \
      -pyexec /usr/local/bin/python3 \
      -py /opt/flink/ops/${script}.py 
    echo ${script} Done!
    sleep 10
done