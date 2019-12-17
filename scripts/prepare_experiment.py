#!/usr/bin/env python3


# Makeshift tool for generating scripts for generating and analyzing lots of synthetically generated FBAS

import sys
import os
import json
from pathlib import Path
from string import Template


qsc_sim = "target/release/qsc_sim -vv"
fbas_analyzer = "target/release/fbas_analyzer -asd -vvvv"


def main():
    folder_path = sys.argv[1]
    nruns = 4

    if not Path(folder_path).is_dir():
        os.mkdir(folder_path)

    configs = [
        config('random-g', '-g $n SimpleRandom $k', dict(n=[10, 20, 40, 80, 160], k=[4, 10]), folder_path, nruns),
        config('smallworld', '-g $n SimpleSmallWorld $k', dict(n=[20, 30], k=[4, 10]), folder_path, nruns),
    ]

    dump('generate.sh', generate_sh(configs, folder_path), folder_path)

    dump('configs.json', json.dumps(configs, indent=2), folder_path)


def config(name, sim_template, parameters, folder_path, nruns):
    result = dict()
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


def generate_sh(configs, folder_path):
    lines = ['DIR=' + folder_path, '']
    for config in configs:
        fixated_parameters = {}
        remaining_parameters = sorted(config['parameters'].keys())
        lines.extend(generate_sh_lines(config, fixated_parameters, remaining_parameters))
    return "\n".join(lines)


def generate_sh_lines(config, fixated_parameters, remaining_parameters):
    lines = []
    if remaining_parameters:
        parameter = remaining_parameters.pop()
        for value in config['parameters'][parameter]:
            fixated_parameters[parameter] = value
            lines.extend(generate_sh_lines(config, fixated_parameters, remaining_parameters))
    else:
        for run in range(config['nruns']):
            fixated_parameters['run'] = run
            command = Template(config['sim_template']).substitute(fixated_parameters)
            outfile = os.path.join('$DIR', Template(config['fbas_json_template']).substitute(fixated_parameters))
            lines.append(command + ' > ' + outfile)
    return lines


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
