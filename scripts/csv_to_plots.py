#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import sys
from os import path

plt.style.use('default')
plt.rcParams['errorbar.capsize'] = 2

configname_key = 'configname'
p1_key = 'k'
p2_key = 'n'

if len(sys.argv) > 1:
    csv_path = sys.argv[1]
    folder_path = path.split(csv_path)[0]
else:
    csv_path = 'results.csv'
    folder_path = './'

df = pd.read_csv(csv_path)

grouped = df.groupby([configname_key, p1_key, p2_key])
df = pd.concat([
    grouped['ttn', 'mbmean', 'mimean'].mean(),
    grouped['mbmin', 'mimin'].min(),
    grouped['mbmax', 'mimax'].max(),
    ], axis=1, sort=False)

[configname_values, p1_values, p2_values] = df.index.levels

for configname_value in configname_values:

    subdf = df.xs(configname_value)

    for p1_value in p1_values:

        subsubdf = subdf.xs(p1_value)

        if len(subsubdf) < 2:
            print('%s, %s=%d: skipping plot with only one entry (strange error otherwise...).'
                  % (configname_value, p1_key, p1_value))
            continue

        means = subsubdf[['mbmean', 'mimean', 'ttn']].copy()

        # if we only have upper and lower bounds for the minimal intersections
        if means['mimean'].isnull().all():
            means['mimean'] = means['mimean'].fillna(0)

        errors = [
                [means['mbmean'] - subsubdf['mbmin'], subsubdf['mbmax'] - means['mbmean']],
                [means['mimean'] - subsubdf['mimin'], subsubdf['mimax'] - means['mimean']],
                [means['ttn'] - means['ttn'], means['ttn'] - means['ttn']],
                ]

        means.plot(kind='bar', yerr=errors)
        plt.savefig(path.join(folder_path, 'plot_%s_%s%d.pdf' % (configname_value, p1_key, p1_value)))
