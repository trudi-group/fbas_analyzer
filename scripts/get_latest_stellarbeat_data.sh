#!/bin/sh

if [ -z "$1" ]
then
  target_dir="test_data"
else
  target_dir=$1
fi

date_stamp=`date +%F`

nodes_link="https://api.stellarbeat.io/v1/nodes"
organizations_link="https://api.stellarbeat.io/v1/organizations"

nodes_file="${target_dir}/stellarbeat_nodes_${date_stamp}.json"
organizations_file="${target_dir}/stellarbeat_organizations_${date_stamp}.json"

fbas_analyzer_path="target/release/fbas_analyzer"

curl $nodes_link > $nodes_file
curl $organizations_link > $organizations_file

echo
echo "To analyze, try (for example):"
echo
echo "${fbas_analyzer_path} ${nodes_file} --merge-by-org ${organizations_file} -apdS --only-core-nodes"
