#!/usr/bin/env python3


# Makeshift tool for generating scripts for generating and analyzing lots of synthetically generated FBAS

import sys
import os
import json
from pathlib import Path
from string import Template


qsc_sim = "../../target/release/qsc_sim -vv"
fbas_analyzer = "../../target/release/fbas_analyzer -asd -vvvv"

results_to_csv = "../../scripts/results_to_csv.py"
csv_to_plots = "../../scripts/csv_to_plots.py"


def main():
    folder_path = sys.argv[1]
    nruns = 10

    if not Path(folder_path).is_dir():
        os.mkdir(folder_path)

    configs = [
        config('random-g', '-g $n SimpleRandom $k', dict(n=[10, 20, 40, 80, 160], k=[4, 10]), folder_path, nruns),
        config('smallworld', '-g $n SimpleSmallWorld $k', dict(n=[20, 30], k=[4]), folder_path, nruns),
    ]

    dump('configs.json', json.dumps(configs, indent=2), folder_path)
    dump('Makefile', build_makefile(configs), folder_path)


def config(name, sim_template, parameters, folder_path, nruns):
    result = dict()
    result['name'] = name
    result['sim_template'] = qsc_sim + " " + sim_template
    result['analyzer'] = fbas_analyzer
    result['fbas_json_template'] = fbas_json_template(name, parameters)
    result['result_out_template'] = result_out_template(name, parameters)
    result['parameters'] = parameters
    result['nruns'] = nruns
    return result


def dump(filename, data, folder_path):
    path = Path(folder_path, filename)
    assert(not path.exists())
    print("Writing %s..." % path)
    with open(path, 'w') as f:
        f.write(data)


def build_makefile(configs):
    return '\n'.join([
        build_makefile_analysis_part(configs),
        build_makefile_plots_part(),
        build_makefile_simulation_part(configs),  # last for better Makefile readability
        ])


def build_makefile_analysis_part(configs):
    targets = " ".join(map(
        lambda config: " ".join(map(
            lambda combination: Template(config['result_out_template']).substitute(combination),
            get_combinations(config))),
        configs))
    return \
"""ANALYZER := ../../target/release/fbas_analyzer -asd -vvvv

TARGETS := {0}

analysis: $(TARGETS)

clean:
\trm -f $(TARGETS)

$(TARGETS): %.result.out : %.fbas.json
\t$(ANALYZER) $< > $@
""".format(targets)


def build_makefile_plots_part():
    lines = [
            'plots:',
            '\t' + results_to_csv + ' ' + 'configs.json' + ' > results.csv',
            '\t' + csv_to_plots + ' ' + 'results.csv',
            ]
    return "\n".join(lines) + "\n"


def build_makefile_simulation_part(configs):
    lines = [
            'simulation:',
            '\tif [ `find . -name "*.fbas.json" | wc -l` -gt 0 ]; then \\',
            '\techo "There are already fbas.json files present; stopping here to avoid overwrites."; \\',
            '\texit 1; \\',
            '\tfi'
            ]
    for config in configs:
        for combination in get_combinations(config):
            command = Template(config['sim_template']).substitute(combination)
            outfile = Template(config['fbas_json_template']).substitute(combination)
            lines.append('\t' + command + ' > ' + outfile)
    return "\n".join(lines) + "\n"


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


def fbas_json_template(name, parameters):
    return base_file_template(name, parameters) + "_r${run}.fbas.json"


def result_out_template(name, parameters):
    return base_file_template(name, parameters) + "_r${run}.result.out"


def base_file_template(name, parameters):
    result = name
    for parameter_name in sorted(parameters.keys()):
        result += "_%s${%s}" % (parameter_name, parameter_name)
    return result


if __name__ == "__main__":
    main()
