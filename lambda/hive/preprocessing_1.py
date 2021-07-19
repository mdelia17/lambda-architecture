#!/usr/bin/env python3
import sys

for message in sys.stdin:
    
    line = message[12:-3]
    fields = line.strip().split(",")
    if fields[2] == "DNS":
        # non so perché vuole 1 come stringa
        print("\t".join([fields[0], fields[1], fields[3], "1"]))