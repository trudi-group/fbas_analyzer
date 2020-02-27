#!/usr/bin/env python3

import subprocess

build_command = 'cargo build --release'
print('Building project to make sure we have an up to date binary: `%s`' % build_command)
subprocess.run(build_command, shell=True)

command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -o test_data/stellarbeat_organizations_2019-09-17.json -a -p"
print("Running command: '%s'" % command)
completed_process = subprocess.run(command, capture_output=True, universal_newlines=True, shell=True)

stdout_lines = completed_process.stdout.split('\n')

expected_lines = [
    'has_quorum_intersection: true',
    'minimal_quorums: [["Stellar Development Foundation","LOBSTR","SatoshiPay","COINQVEST Limited"],["Stellar Development Foundation","LOBSTR","SatoshiPay","Keybase"],["Stellar Development Foundation","LOBSTR","COINQVEST Limited","Keybase"],["Stellar Development Foundation","SatoshiPay","COINQVEST Limited","Keybase"],["LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]]',
    'minimal_blocking_sets: [["Stellar Development Foundation","LOBSTR"],["Stellar Development Foundation","SatoshiPay"],["Stellar Development Foundation","COINQVEST Limited"],["Stellar Development Foundation","Keybase"],["LOBSTR","SatoshiPay"],["LOBSTR","COINQVEST Limited"],["LOBSTR","Keybase"],["SatoshiPay","COINQVEST Limited"],["SatoshiPay","Keybase"],["COINQVEST Limited","Keybase"]]',
    'minimal_splitting_sets: [["Stellar Development Foundation","LOBSTR","SatoshiPay"],["Stellar Development Foundation","LOBSTR","COINQVEST Limited"],["Stellar Development Foundation","LOBSTR","Keybase"],["Stellar Development Foundation","SatoshiPay","COINQVEST Limited"],["Stellar Development Foundation","SatoshiPay","Keybase"],["Stellar Development Foundation","COINQVEST Limited","Keybase"],["LOBSTR","SatoshiPay","COINQVEST Limited"],["LOBSTR","SatoshiPay","Keybase"],["LOBSTR","COINQVEST Limited","Keybase"],["SatoshiPay","COINQVEST Limited","Keybase"]]',
    'top_tier: ["Stellar Development Foundation","LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]',
]

print("Checking output for expected important lines...")
for line in expected_lines:
    assert line in stdout_lines, "Missing output line: '%s'" % line

print("Test completed successfully!")
