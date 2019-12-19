#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import sys

configname_key = 'configname'
p1_key = 'k'
p2_key = 'n'

if len(sys.argv) > 1:
    csv_path = sys.argv[1]
else:
    csv_path = 'results.csv'

df = pd.read_csv(csv_path)
df = df.groupby([configname_key, p1_key, p2_key]).mean()

del df['run']  # mean run number => makes no sense

[configname_values, p1_values, p2_values] = df.index.levels

nplots = len(configname_values) * len(p1_values)

for configname_value in configname_values:

    subdf = df.xs(configname_value)

    for p1_value in p1_values:

        subsubdf = subdf.xs(p1_value)

        means = subsubdf[['mbmean', 'mimean', 'ttn']]
        errors = [
                [subsubdf['mbmean'] - subsubdf['mbmin'], subsubdf['mbmax'] - subsubdf['mbmean']],
                [subsubdf['mimean'] - subsubdf['mimin'], subsubdf['mimax'] - subsubdf['mimean']],
                [subsubdf['ttn'] - subsubdf['ttn'], subsubdf['ttn'] - subsubdf['ttn']],
                ]

        means.plot(kind='bar', yerr=errors)
        plt.savefig('plot_%s_%s%d.pdf' % (configname_value, p1_key, p1_value))
