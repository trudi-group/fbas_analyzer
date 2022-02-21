#!/usr/bin/env python3

import subprocess
import tempfile


def main():

    cargo_debug_build()
    cargo_test()
    test_example()

    cargo_build()
    test_fbas_analyzer()
    test_bulk_fbas_analyzer()
    test_qsc_simulator()

    print("All tests completed successfully!")


def cargo_test():
    run_and_check_return('cargo test --no-default-features', 'Running unit tests with minimal feature set')
    run_and_check_return('cargo test', 'Running unit tests with regular feature set')
    run_and_check_return('cargo test -- --ignored', 'Running slow unit tests')


def cargo_build():
    run_and_check_return('cargo build --release', 'Building project to make sure we have up-to-date binaries')


def cargo_debug_build():
    run_and_check_return('cargo build', 'Building project to make sure we have up-to-date debug binaries for some tests')


def test_example():
    command = "cargo run --release --example results_reuse"
    expected_strings = [
        'a973afb6bac6df244b45c55ff98e0f49a0f6ac4a0c6a7e1cda0844dd24ec761a', # standard form hash after removing inactive nodes
        'GA35T3723UP2XJLC2H7MNL6VMKZZIFL2VW7XHMFFJKKIA2FJCYTLKFBW', # a minimal blocking set member
    ]
    run_and_check_output(command, expected_strings=expected_strings)


def test_fbas_analyzer():
    test_fbas_analyzer_with_organizations()
    test_fbas_analyzer_with_ids()
    test_fbas_analyzer_on_broken()
    test_fbas_analyzer_on_mobilecoin()


def test_fbas_analyzer_with_organizations():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json --merge-by-org test_data/stellarbeat_organizations_2019-09-17.json -a -p"
    expected_strings = [
        'has_quorum_intersection: true',
        'minimal_quorums: [["Stellar Development Foundation","LOBSTR","SatoshiPay","COINQVEST Limited"],["Stellar Development Foundation","LOBSTR","SatoshiPay","Keybase"],["Stellar Development Foundation","LOBSTR","COINQVEST Limited","Keybase"],["Stellar Development Foundation","SatoshiPay","COINQVEST Limited","Keybase"],["LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]]',
        'minimal_blocking_sets: [["Stellar Development Foundation","LOBSTR"],["Stellar Development Foundation","SatoshiPay"],["Stellar Development Foundation","COINQVEST Limited"],["Stellar Development Foundation","Keybase"],["LOBSTR","SatoshiPay"],["LOBSTR","COINQVEST Limited"],["LOBSTR","Keybase"],["SatoshiPay","COINQVEST Limited"],["SatoshiPay","Keybase"],["COINQVEST Limited","Keybase"]]',
        'minimal_splitting_sets: [["Stellar Development Foundation"],["LOBSTR","SatoshiPay"],["LOBSTR","COINQVEST Limited"],["SatoshiPay","COINQVEST Limited"],["LOBSTR","Muyu Network","Keybase"],["Muyu Network","SatoshiPay","Keybase"],["Muyu Network","COINQVEST Limited","Keybase"],["SatoshiPay","IBM worldwire","Keybase"]]',
        'top_tier: ["Stellar Development Foundation","LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]',
        ]
    run_and_check_output(command, expected_strings=expected_strings)


def test_fbas_analyzer_with_ids():
    command = "target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -a"
    expected_strings = [
        'top_tier: [1,4,8,23,29,36,37,43,44,52,56,69,86,105,167,168,171]',
    ]
    run_and_check_output(command, expected_strings=expected_strings)


def test_fbas_analyzer_on_broken():
    command = "target/release/fbas_analyzer test_data/broken.json -a"
    expected_strings = [
        'has_quorum_intersection: false',
        'minimal_blocking_sets: [[3,4],[4,10],[3,6,10]]',
        'minimal_splitting_sets: [[]]',
        'top_tier: [3,4,6,10]',
    ]
    run_and_check_output(command, expected_strings=expected_strings)


def test_fbas_analyzer_on_mobilecoin():
    command = "target/release/fbas_analyzer test_data/mobilecoin_nodes_2021-10-22.json"
    expected_strings = [
        'symmetric_clusters: [{"threshold":8,"validators":[0,1,2,3,4,5,6,7,8,9]}]',
    ]
    run_and_check_output(command, expected_strings=expected_strings)


def test_bulk_fbas_analyzer():
    test_bulk_fbas_analyzer_to_stdout()
    test_bulk_fbas_analyzer_update_flag()


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
        'label,has_quorum_intersection,top_tier_size,mbs_min,mbs_max,mbs_mean,mss_min,mss_max,mss_mean,mq_min,mq_max,mq_mean,orgs_top_tier_size,orgs_mbs_min,orgs_mbs_max,orgs_mbs_mean,orgs_mss_min,orgs_mss_max,orgs_mss_mean,orgs_mq_min,orgs_mq_max,orgs_mq_mean,isps_top_tier_size,isps_mbs_min,isps_mbs_max,isps_mbs_mean,isps_mss_min,isps_mss_max,isps_mss_mean,isps_mq_min,isps_mq_max,isps_mq_mean,ctries_top_tier_size,ctries_mbs_min,ctries_mbs_max,ctries_mbs_mean,ctries_mss_min,ctries_mss_max,ctries_mss_mean,ctries_mq_min,ctries_mq_max,ctries_mq_mean,standard_form_hash,analysis_duration_mq,analysis_duration_mbs,analysis_duration_mss,analysis_duration_total',
        'broken,false,4,2,3',
        'correct,true,3,2,2,2.0,1,1,1.0,2,2,2.0,,,,,,,,,,,',
        '2019-09-17,true,17,4,5,4.689655172413793,2,9,3.3200934579439254,8,9,8.930232558139535,5,2,2,2.0,1,3,2.375,4,4,4.0,,,,,,,,,,,3,1,1,1.0,1,1,1.0,1,1,1.0,d7ffa370c12ea97a2c51c87b752ab89914081704b824caef660896eb68adb75d,0.',
        '2020-01-16_broken_by_hand,false,22,5,6,5.625,0,0,0.0,2,11,10.9413',
        ]
    run_and_check_output(command, expected_strings=expected_strings)

def test_bulk_fbas_analyzer_update_flag():
    input_files = ['test_data/' + x for x in [
        'broken.json',
        'correct.json',
        'stellarbeat_nodes_2020-01-16_broken_by_hand.json',
    ]]
    update_files = ['test_data/' + x for x in [
        'broken.json',
        'correct.json',
        'stellarbeat_nodes_2019-09-17.json',
        'stellarbeat_nodes_2020-01-16_broken_by_hand.json',
        'stellarbeat_organizations_2019-09-17.json',
    ]]
    tf = tempfile.NamedTemporaryFile('r+')
    daily_csv = tf.name

    command = 'target/release/bulk_fbas_analyzer ' + ' '.join(input_files)
    run_redirect_stdout_to_file_and_check_return(command, tf)

    command = 'target/release/bulk_fbas_analyzer ' + ' '.join(update_files) + ' -u -o ' + daily_csv
    expected_strings  = [
        'label,has_quorum_intersection,top_tier_size,mbs_min,mbs_max,mbs_mean,mss_min,mss_max,mss_mean,mq_min,mq_max,mq_mean,orgs_top_tier_size,orgs_mbs_min,orgs_mbs_max,orgs_mbs_mean,orgs_mss_min,orgs_mss_max,orgs_mss_mean,orgs_mq_min,orgs_mq_max,orgs_mq_mean,isps_top_tier_size,isps_mbs_min,isps_mbs_max,isps_mbs_mean,isps_mss_min,isps_mss_max,isps_mss_mean,isps_mq_min,isps_mq_max,isps_mq_mean,ctries_top_tier_size,ctries_mbs_min,ctries_mbs_max,ctries_mbs_mean,ctries_mss_min,ctries_mss_max,ctries_mss_mean,ctries_mq_min,ctries_mq_max,ctries_mq_mean,standard_form_hash,analysis_duration_mq,analysis_duration_mbs,analysis_duration_mss,analysis_duration_total',
        'broken,false,4,2,3',
        'correct,true,3,2,2,2.0,1,1,1.0,2,2,2.0,,,,,,,,,,,',
        '2020-01-16_broken_by_hand,false,22,5,6,5.625,0,0,0.0,2,11,10.9413',
        '2019-09-17,true,17,4,5,4.689655172413793,2,9,3.3200934579439254,8,9,8.930232558139535,5,2,2,2.0,1,3,2.375,4,4,4.0,,,,,,,,,,,3,1,1,1.0,1,1,1.0,1,1,1.0,d7ffa370c12ea97a2c51c87b752ab89914081704b824caef660896eb68adb75d,0.',
        ]
    run_redirect_stdout_to_file_and_check_output(command, tf, expected_strings=expected_strings)
    tf.close()

def test_qsc_simulator():
    graph = '0|1|0\n0|2|0\n1|0|0\n1|2|0\n2|0|0\n2|1|0'
    command = 'target/release/qsc_simulator AllNeighbors -'

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

    run_and_check_output(command, expected_strings=[expected], stdin=graph)


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

def run_redirect_stdout_to_file_and_check_return(command, file_descriptor, expected_returncode=0):
    completed_process = subprocess.run(command, universal_newlines=True, shell=True, stdout=file_descriptor)
    assert completed_process.returncode == expected_returncode,\
        "Expected return code '%d', got '%d'." % (expected_returncode, completed_process.returncode)

def run_redirect_stdout_to_file_and_check_output(command, file_descriptor, log_message='Running command', expected_strings=[]):
    print("%s: `%s`" % (log_message, command))
    completed_process = subprocess.run(command, universal_newlines=True, shell=True, stdout=file_descriptor)
    stderr = completed_process.stderr

    file_descriptor.seek(0)
    actual_strings = file_descriptor.read()
    print("Checking output for expected strings...")
    for expected in expected_strings:
        assert expected in actual_strings, '\n'.join([
            "Missing expected output string:",
            "'''",
            expected,
            "'''",
            "Full output:",
            "'''",
            str(actual_strings) + "\n" + "'''"
            "STDERR: '%s'" % stderr,
        ])

if __name__ == "__main__":
    main()
