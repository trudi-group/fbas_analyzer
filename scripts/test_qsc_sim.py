#!/usr/bin/env python3

import subprocess
from subprocess import Popen

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
	graph = b"0|1|0\n0|2|0\n1|0|0\n1|2|0\n2|0|0\n2|1|0"
	executable = 'target/release/qsc_sim'
	args = ['SimpleQsc','-']

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

	run_and_check(executable, args, graph, expected_lines)

def run_and_check(executable, args, to_stdin, expected_lines):
	print("Running command: {} | {} {} {}" .format(repr(to_stdin), executable, args[0], '-'))
	completed_process = subprocess.Popen([executable, args[0],args[1]], stdin=subprocess.PIPE,
			stdout=subprocess.PIPE, shell=False)
	completed_process.stdin.write(to_stdin)
	stdout_lines, stderr_lines = completed_process.communicate()

	print("Checking output for expected important lines...")
	for line in expected_lines:
		assert line in stdout_lines.decode('utf-8'), "Missing output line: '%s'" % line

if __name__ == "__main__":
	main()
