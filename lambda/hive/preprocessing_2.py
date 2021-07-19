#!/usr/bin/env python3
import sys

for message in sys.stdin:
    
    line = message[12:-3]
    fields = line.strip().split(",")
    words = fields[4].split(" ")
    if words[0] == "Standard":
        if words[2]!= "response":
            url = words[-1]
            domain = url.strip().split(".")
            if len(domain) > 1:
                    string = domain[-2] + "." + domain[-1]
            else:
                    string = domain[-1]
            print("\t".join(["A", string, fields[0]]))
        else:
                for i in range(len(words)-1):
                        if words[i] == "CNAME":
                            print("\t".join(["CNAME", words[i+1], fields[0]]))
                        if words[i] == "NS":
                            print("\t".join(["NS", words[i+1], fields[0]]))   
    