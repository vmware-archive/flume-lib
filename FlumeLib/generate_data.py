#!/bin/python

# A simple Python script to generate a table of random integers

import random
import sys

if len(sys.argv) != 3:
	print 'Usage: python generate_data.py <num rows> <num cols>'
	sys.exit(1)

num_rows = int(sys.argv[1])
num_cols = int(sys.argv[2])

for i in range(0, num_rows):
	s = ""
	for j in range(0, num_cols):
		s += str(random.randint(0, 1000000))

		if j < num_cols - 1:
			s += '|'
	print s

