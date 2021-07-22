#!/usr/bin/env python3
"""reducer.py"""

import sys

d = {}

for line in sys.stdin:
    line = line.strip()
    address, message = line.split("\t")
    info = message.strip().split(" ")

    if info[0] == "Standard":
        if info[2] != "response":
            url = info[-1]
            domain = url.strip().split(".")
            if len(domain) > 1:
                string = domain[-2] + "." + domain[-1]
            else:
                string = domain[-1]
            if ("A",string) not in d:
                d["A",string] = {address}
            else:
                d["A",string].add(address)
        else:
            for i in range(len(info)-1):
                if info[i] == "CNAME":
                    if ("CNAME", info[i+1]) not in d:
                        d["CNAME", info[i+1]] = {address}
                    else:
                        d["CNAME", info[i+1]].add(address)
                if info[i] == "NS":
                    if ("NS", info[i+1]) not in d:
                        d["NS", info[i+1]] = {address}
                    else:
                        d["NS", info[i+1]].add(address)

sorted_dict = sorted(d.items(), key=lambda item: len(item[1]), reverse=True)

for t in sorted_dict:
    # print("%s\t%s\t%s\t%i" %(t[0][0], t[0][1], t[1], len(t[1])))
    print("%s\t%s\t%i" %(t[0][0], t[0][1], len(t[1])))