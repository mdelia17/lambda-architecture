#!/usr/bin/env python3
import sys

for message in sys.stdin:
    (src, dst, bytes, tot) = message.strip().split('\t')
    print('\t'.join([str(src), str(dst), str(bytes), str(tot)]))
    