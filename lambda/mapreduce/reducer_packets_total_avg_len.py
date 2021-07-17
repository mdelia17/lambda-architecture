#!/usr/bin/env python3
"""reducer.py"""

import sys

d = {}

for line in sys.stdin:
    line = line.strip()
    src, dst, l = line.split("\t")
    try:
        l = int(l)
    except ValueError:
        continue

    if (src,dst) not in d:
        if (dst,src) not in d:
            d[src,dst] = [l, 1]
        else:
            d[dst,src][0] += l
            d[dst,src][1] += 1
    else:
        d[src,dst][0] += l
        d[src,dst][1] += 1

sorted_dict = sorted(d.items(), key=lambda item: item[1][0], reverse=True)

for t in sorted_dict:
    avg = float("{:.2f}".format((t[1][0]/t[1][1])))
    print("%s\t%s\t%i\t%f\t%i" %(t[0][0], t[0][1], t[1][0], avg, t[1][1]))

# for t in d:
#     avg = float("{:.2f}".format((d[t][0]/d[t][1])))
#     print("%s\t\t%s\t\t%i\t\t%f\t\t%i" %(t[0], t[1], d[t][0], avg, d[t][1]))