'''>UPI0000000011 status=active    MVDAITVLTAIGITVLMLLMVISGAAMIVKELNPNDIFTMQSLKFNRAVTIFKYIGLFIY    IPGTIILYATYVKSLLMKS \n'''

import multiprocessing
import sqlite3
import sys

with open('uniparc_active_chunk_1.fasta', 'r') as file:
	for line in file:
		# print(line)
		uniparcID = line.split(' ')[0][1:]
		status = line.split('\t')[0].split(' ')[1].split('=')[1]
		sequence = ''
		for _ in line.split('\t')[1:-1]:
			sequence += _
