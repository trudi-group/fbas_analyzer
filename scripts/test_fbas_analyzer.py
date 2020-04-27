#!/usr/bin/env python3

import subprocess


def main():
    cargo_test()
    cargo_build()

    test_with_organizations()
    test_with_ids()

    print("All tests completed successfully!")


def cargo_test():
    test_command_1 = 'cargo test'
    print('Running unit tests: `%s`' % test_command_1)
    subprocess.run(test_command_1, shell=True)
    test_command_2 = 'cargo test -- --ignored'
    print('Running slow unit tests: `%s`' % test_command_2)
    subprocess.run(test_command_2, shell=True)


def cargo_build():
    build_command = 'cargo build --release'
    print('Building project to make sure we have an up to date binary: `%s`' % build_command)
    subprocess.run(build_command, shell=True)


def test_with_organizations():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -o test_data/stellarbeat_organizations_2019-09-17.json -a -p"
    expected_lines = [
        'has_quorum_intersection: true',
        'minimal_quorums: [["LOBSTR","Stellar Development Foundation","COINQVEST Limited","SatoshiPay"],["LOBSTR","Stellar Development Foundation","COINQVEST Limited","Keybase"],["LOBSTR","Stellar Development Foundation","SatoshiPay","Keybase"],["LOBSTR","COINQVEST Limited","SatoshiPay","Keybase"],["Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase"]]',
        'minimal_blocking_sets: [["LOBSTR","Stellar Development Foundation"],["LOBSTR","COINQVEST Limited"],["LOBSTR","SatoshiPay"],["LOBSTR","Keybase"],["Stellar Development Foundation","COINQVEST Limited"],["Stellar Development Foundation","SatoshiPay"],["Stellar Development Foundation","Keybase"],["COINQVEST Limited","SatoshiPay"],["COINQVEST Limited","Keybase"],["SatoshiPay","Keybase"]]',
        'minimal_splitting_sets: [["LOBSTR","Stellar Development Foundation","COINQVEST Limited"],["LOBSTR","Stellar Development Foundation","SatoshiPay"],["LOBSTR","Stellar Development Foundation","Keybase"],["LOBSTR","COINQVEST Limited","SatoshiPay"],["LOBSTR","COINQVEST Limited","Keybase"],["LOBSTR","SatoshiPay","Keybase"],["Stellar Development Foundation","COINQVEST Limited","SatoshiPay"],["Stellar Development Foundation","COINQVEST Limited","Keybase"],["Stellar Development Foundation","SatoshiPay","Keybase"],["COINQVEST Limited","SatoshiPay","Keybase"]]',
        'top_tier: ["LOBSTR","Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase"]',
        ]
    run_and_check(command, expected_lines)


def test_with_ids():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -a"
    expected_lines = [
        'top_tier: [1,4,8,23,29,36,37,43,44,52,56,69,86,105,167,168,171]',
    ]
    run_and_check(command, expected_lines)


def run_and_check(command, expected_lines):
    print("Running command: '%s'" % command)
    completed_process = subprocess.run(command, capture_output=True, universal_newlines=True, shell=True)
    stdout_lines = completed_process.stdout.split('\n')

    print("Checking output for expected important lines...")
    for line in expected_lines:
        assert line in stdout_lines, "Missing output line: '%s'" % line


if __name__ == "__main__":
    main()
