#!/usr/bin/env python3
"""mapper.py"""

import sys

for message in sys.stdin:
    line = message[12:-3]
    fields = line.strip().split(",")
    if fields[2] == "DNS":
        print("%s\t%s\t%i" % (fields[0], fields[1], int(fields[3])))