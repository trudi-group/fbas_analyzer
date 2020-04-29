#!/usr/bin/env python3

import os
import subprocess

def main():
	cargo_build()
	cargo_test()

	test_graph_pipe()

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


def test_graph_pipe():
	command = 'echo -e "0|1|0\n0|2|0\n1|0|0\n1|2|0\n2|0|0\n2|1|0" | target/release/qsc_sim SimpleQsc -'

	expected_lines = [
			'[',
			'  {',
			'    "publicKey": "n0",',
			'    "quorumSet": {',
			'      "threshold": 3,',
			'      "validators": [',
			'       "n0",',
			'       "n1",',
			'       "n2"',
			'      ]',
			'    }',
			'  },',
			'  {',
			'    "publicKey": "n1",',
			'    "quorumSet": {',
			'      "threshold": 3,',
			'      "validators": [',
			'       "n0",',
			'       "n1",',
			'       "n2"',
			'      ]',
			'    }',
			'  },',
			'  {',
			'    "publicKey": "n2",',
			'    "quorumSet": {',
			'      "threshold": 3,',
			'      "validators": [',
			'       "n0",',
			'       "n1",',
			'       "n2"',
			'      ]',
			'    }',
			'  }',
			']',
			]

	run_and_check(command, expected_lines)

def run_and_check(command, expected_lines):
	print("Running command: '%s'" % repr(command))
	completed_process = subprocess.run(command, capture_output=True, universal_newlines=True, shell=True)
	stdout_lines = completed_process.stdout

	print("Checking output for expected important lines...")
	for line in expected_lines:
		assert line in stdout_lines, "Missing output line: '%s'" % line

if __name__ == "__main__":
	main()
