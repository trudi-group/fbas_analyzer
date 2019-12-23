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

                ttn = get_ttn(data)
                line.append(ttn)

                (mbmin, mbmax, mbmean) = get_minmaxmean_blocking_sets(data)
                line.extend([mbmin, mbmax, mbmean])

                (mimin, mimax, mimean) = get_minmaxmean_minimal_intersections(data)
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
            # json vs python fix
            value = value.replace('true', 'True')
            value = value.replace('false', 'False')
            try:
                data[label] = literal_eval(value)
            except Exception as e:
                sys.stderr.write('Failed parsing "%s". Error: %s\n' % (value.strip(), str(e)))
    return data


def get_ttn(data):
    return data.get('minimal_quorums')[1]


def get_minmaxmean_blocking_sets(data):
    return get_minmaxmean(data.get('minimal_blocking_sets'))


def get_minmaxmean_minimal_intersections(data):
    has_quorum_intersection = data.get('has_quorum_intersection')
    data_line = data.get('minimal_intersections')
    if has_quorum_intersection == False:
        return (0, 0, 0.)
    elif data_line:
        return get_minmaxmean(data_line)
    else:
        # calculate theoretical lower and upper bounds
        [ttn, mq_histogram] = data.get('minimal_quorums')[1:3]

        if sum(mq_histogram) == 1:
            lower_bound = None
            upper_bound = None
        else:

            minl1 = next(i for (i, x) in enumerate(mq_histogram) if x)  # first non-zero
            mq_histogram[minl1] -= 1
            minl2 = next(i for (i, x) in enumerate(mq_histogram) if x)  # first non-zero
            lower_bound = max(int(has_quorum_intersection), minl1 + minl2 - ttn)

            # maxl1 = len(mq_histogram) - 1
            mq_histogram[-1] -= 1
            while mq_histogram[-1] == 0:
                mq_histogram = mq_histogram[:-1]
            maxl2 = len(mq_histogram) - 1
            upper_bound = maxl2 - 1

        return (lower_bound, upper_bound, lower_bound if lower_bound == upper_bound else None)


def get_minmaxmean(data_line):
    if len(data_line) == 5:  # --describe
        [nsets, nnodes, minl, maxl, meanl] = data_line
    elif len(data_line) == 3:  # --histogram
        [nsets, nnodes, histogram] = data_line
        minl = next(i for (i, x) in enumerate(histogram) if x)  # first non-zero
        maxl = len(histogram) - 1
        assert(nsets == sum(histogram))
        meanl = sum(i * x for (i, x) in enumerate(histogram)) / nsets
    else:
        raise Exception('Unknown format for "describe".')
    return (minl, maxl, meanl)


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
