#!/usr/bin/env python3

import sys
from ast import literal_eval
import csv
import json
from string import Template
from os import path


def main():
    configs_path = sys.argv[1]
    with open(configs_path) as cf:
        configs = json.load(cf)

    folder_path = path.split(configs_path)[0]

    parameters = set()
    for config in configs:
        parameters.update(config['parameters'].keys())
    parameters = sorted(list(parameters))

    csv_lines = [
            ['configname'] + parameters +
            ['ttn', 'mbmin', 'mbmax', 'mbmean', 'mimin', 'mimax', 'mimean', 'run']]

    for config in configs:
        for combination in get_combinations(config):
            try:
                filename = path.join(
                        folder_path,
                        Template(config['result_out_template']).substitute(combination))
                sys.stderr.write('Parsing ' + filename + '\n')

                line = [config['name']]
                for parameter in parameters:
                    line.append(combination[parameter])

                data = parse_result_data(filename)

                ttn = data.get('minimal_quorums')[4]
                line.append(ttn)

                [mbmin, mbmax, mbmean] = data.get('minimal_blocking_sets')[1:4]
                line.extend([mbmin, mbmax, mbmean])

                [mimin, mimax, mimean] = data.get('minimal_intersections')[1:4]
                line.extend([mimin, mimax, mimean])

                line.append(combination['run'])

                csv_lines.append(line)
            except Exception as e:
                sys.stderr.write('Error: ' + str(e) + '\n')

    csv_writer = csv.writer(sys.stdout)
    csv_writer.writerows(csv_lines)


def parse_result_data(filename):
    data = dict()
    with open(filename) as f:
        for line in f:
            [label, value] = line.split(': ')
            try:
                data[label] = literal_eval(value)
            except Exception as e:
                sys.stderr.write('Failed parsing "%s". Error: %s\n' % (value.strip(), str(e)))
    return data


# TODO this is a 1:1 copy from scripts/prepare_experiment.py ...
def get_combinations(config):
    def unroll(config, current_combination, remaining_parameters):
        combinations = []
        if remaining_parameters:
            parameter = remaining_parameters[-1]
            for value in config['parameters'][parameter]:
                current_combination[parameter] = value
                combinations.extend(unroll(config, current_combination, remaining_parameters[:-1]))
        else:
            for run in range(config['nruns']):
                current_combination['run'] = run
                combinations.append(current_combination.copy())
        return combinations
    return unroll(config, {}, sorted(config['parameters'].keys()))


if __name__ == "__main__":
    main()
