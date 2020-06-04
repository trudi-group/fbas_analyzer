#!/usr/bin/env python3

import subprocess


def main():
    cargo_test()
    cargo_build()

    test_fbas_analyzer()
    test_bulk_fbas_analyzer()
    test_qsc_simulator()

    print("All tests completed successfully!")


def cargo_test():
    run_and_check_return('cargo test', 'Running unit tests')
    run_and_check_return('cargo test -- --ignored', 'Running slow unit tests')


def cargo_build():
    run_and_check_return('cargo build --release', 'Building project to make sure we have an up to date binary')


def test_fbas_analyzer():
    test_fbas_analyzer_with_organizations()
    test_fbas_analyzer_with_ids()


def test_fbas_analyzer_with_organizations():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -m test_data/stellarbeat_organizations_2019-09-17.json -a -p"
    expected_strings = [
        'has_quorum_intersection: true',
        'minimal_quorums: [["LOBSTR","Stellar Development Foundation","COINQVEST Limited","SatoshiPay"],["LOBSTR","Stellar Development Foundation","COINQVEST Limited","Keybase"],["LOBSTR","Stellar Development Foundation","SatoshiPay","Keybase"],["LOBSTR","COINQVEST Limited","SatoshiPay","Keybase"],["Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase"]]',
        'minimal_blocking_sets: [["LOBSTR","Stellar Development Foundation"],["LOBSTR","COINQVEST Limited"],["LOBSTR","SatoshiPay"],["LOBSTR","Keybase"],["Stellar Development Foundation","COINQVEST Limited"],["Stellar Development Foundation","SatoshiPay"],["Stellar Development Foundation","Keybase"],["COINQVEST Limited","SatoshiPay"],["COINQVEST Limited","Keybase"],["SatoshiPay","Keybase"]]',
        'minimal_splitting_sets: [["LOBSTR","Stellar Development Foundation","COINQVEST Limited"],["LOBSTR","Stellar Development Foundation","SatoshiPay"],["LOBSTR","Stellar Development Foundation","Keybase"],["LOBSTR","COINQVEST Limited","SatoshiPay"],["LOBSTR","COINQVEST Limited","Keybase"],["LOBSTR","SatoshiPay","Keybase"],["Stellar Development Foundation","COINQVEST Limited","SatoshiPay"],["Stellar Development Foundation","COINQVEST Limited","Keybase"],["Stellar Development Foundation","SatoshiPay","Keybase"],["COINQVEST Limited","SatoshiPay","Keybase"]]',
        'top_tier: ["LOBSTR","Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase"]',
        ]
    run_and_check_output(command, expected_strings)


def test_fbas_analyzer_with_ids():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -a"
    expected_strings = [
        'top_tier: [1,4,8,23,29,36,37,43,44,52,56,69,86,105,167,168,171]',
    ]
    run_and_check_output(command, expected_strings)


def test_bulk_fbas_analyzer():
    test_bulk_fbas_analyzer_to_stdout()


def test_bulk_fbas_analyzer_to_stdout():
    input_files = ['test_data/' + x for x in [
        'broken.json',
        'correct.json',
        'stellarbeat_nodes_2019-09-17.json',
        'stellarbeat_nodes_2020-01-16_broken_by_hand.json',
        'stellarbeat_organizations_2019-09-17.json',
    ]]
    command = 'target/release/bulk_fbas_analyzer ' + ' '.join(input_files)

    expected_strings = [
        'label,merged_by_organizations,has_quorum_intersection,top_tier_size,mbs_min,mbs_max,mbs_mean,mss_min,mss_max,mss_mean,mq_min,mq_max,mq_mean,analysis_duration',
        'broken,false,false,4,2,3',
        'correct,false,true,3,2,2,2.0,1,1,1.0,2,2,2.0',
        '2019-09-17,true,true,5,2,2,2.0,3,3,3.0,4,4,4.0',
        '2020-01-16_broken_by_hand,false,false,22,5,6,5.625,0,0,0.0,2,11,10.9413',
        ]
    run_and_check_output(command, expected_strings)


def test_qsc_simulator():
    graph = '0|1|0\n0|2|0\n1|0|0\n1|2|0\n2|0|0\n2|1|0'
    command = 'target/release/qsc_sim SimpleQsc -'

    expected = '\n'.join([
        '[',
        '  {',
        '    "publicKey": "n0",',
        '    "quorumSet": {',
        '      "threshold": 3,',
        '      "validators": [',
        '        "n0",',
        '        "n1",',
        '        "n2"',
        '      ]',
        '    }',
        '  },',
        '  {',
        '    "publicKey": "n1",',
        '    "quorumSet": {',
        '      "threshold": 3,',
        '      "validators": [',
        '        "n0",',
        '        "n1",',
        '        "n2"',
        '      ]',
        '    }',
        '  },',
        '  {',
        '    "publicKey": "n2",',
        '    "quorumSet": {',
        '      "threshold": 3,',
        '      "validators": [',
        '        "n0",',
        '        "n1",',
        '        "n2"',
        '      ]',
        '    }',
        '  }',
        ']',
        ])

    run_and_check_output(command, [expected], stdin=graph)


def run_and_check_return(command, log_message, expected_returncode=0):
    print("%s: `%s`" % (log_message, command))
    completed_process = subprocess.run(command, shell=True)
    assert completed_process.returncode == expected_returncode,\
        "Expected return code '%d', got '%d'." % (expected_returncode, completed_process.returncode)


def run_and_check_output(command, log_message='Running command', expected_strings=[], stdin=''):
    print("%s: `%s`" % (log_message, command))
    if stdin:
        print("Feeding in via STDIN:\n'''\n%s\n'''" % stdin)
    completed_process = subprocess.run(command, input=stdin, capture_output=True, universal_newlines=True, shell=True)
    stdout = completed_process.stdout
    stderr = completed_process.stderr

    print("Checking output for expected strings...")
    for expected in expected_strings:
        assert expected in stdout, '\n'.join([
            "Missing expected output string:",
            "'''",
            expected,
            "'''",
            "Full output:",
            "'''",
            stdout + "'''",
            "STDERR: '%s'" % stderr,
        ])


if __name__ == "__main__":
    main()
