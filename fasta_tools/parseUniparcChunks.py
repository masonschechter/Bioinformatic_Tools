'''>UPI0000000011 status=active    MVDAITVLTAIGITVLMLLMVISGAAMIVKELNPNDIFTMQSLKFNRAVTIFKYIGLFIY    IPGTIILYATYVKSLLMKS \n'''

import multiprocessing
import sqlite3
import sys

with open('uniparc_active_chunk_1', 'r') as file:
	for line in file:
		uniparcID = line.split(' ')[0][1:]
		status = line.split('\t')[0].split(' ')[1].split('=')[1]
		sequence = line.split('\t')[1:-1]
		print(sequence)
		exit()

