import os
import os.path
import json
import glob
import functools
import numpy
import matplotlib.pyplot as plt
import sys



w2s = {}
css = {}
names = []

for dirpath, dirnames, filenames in os.walk("."):
    for filename in [f for f in filenames if f.endswith(".json") and f != 'compare.json']:

        full_path=os.path.join(dirpath, filename)
        names.append(filename.split('_')[0]+filename.split('_')[1])
        with open(full_path) as json_data:
            data = json.load(json_data)
            for key, value in data.items():
                if key in w2s:
                    w2s[key].append(value['w2'])
                    css[key].append(value['cs'])
                else:
                    w2s[key] = [value['w2']]
                    css[key] = [value['cs']]


cW2 = {}
cCs = {}
for key, value in w2s.items():
    cW2[key] = {'avg': numpy.average(value), 'stdev' : numpy.std(value)}
for key, value in css.items():
    cCs[key] = {'avg': numpy.average(value), 'stdev' : numpy.std(value)}

result = {'CS' : cCs, 'W2' : cW2}

with open("compare.json", "w") as jsonoutput:
    json.dump(result, jsonoutput, indent=4)



N = len(names)
ind = numpy.arange(N)    # the x locations for the groups

width = 0.4

for key in w2s.keys():
    stage = key
    fig=plt.figure(figsize=(70, 30))
    ax1 = fig.add_subplot(111)
    p1 = ax1.bar(ind, w2s[stage], width, color='b')
    ax1.set_ylabel('w2', fontdict={'fontsize': 28})
    ax2 = ax1.twinx()
    ax2.set_ylabel('cores', fontdict={'fontsize': 28})

    p2 = ax2.bar(ind+width, css[stage], width, color='y')

    for tl in ax1.get_yticklabels():
        tl.set_size(26)
    for tl in ax2.get_yticklabels():
        tl.set_size(26)
    for tl in ax1.get_xticklabels():
        tl.set_size(26)
    #plt.ylabel('Scores')
    #plt.title('Scores by group and gender')
    plt.tick_params(axis='both', which='minor', labelsize=26)
    plt.tick_params(axis='both', which='major', labelsize=26)

    plt.xticks(ind+width/2, names)
    ax1.legend((p1[0], p2[0]), ('w2', 'cores'), prop={'size':30})
    plt.savefig('stage'+stage+'.pdf', bbox_inches='tight', dpi=1200)
