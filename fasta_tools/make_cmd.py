'''cat the output file into the .slurm'''

numFiles = 1994
batchSize = 24
remainder = numFiles % batchSize
highestMultiple = (numFiles - remainder)//batchSize

with open('/scratch/06538/mschecht/jobs/parseUniref100.cmd', 'w') as file:
    for i in range(highestMultiple):
        start = i*batchSize+1
        stop = (i+1)*batchSize
        file.write(f"python3 /home1/06538/mschecht/repos/bioinformatic_tools/fasta_tools/parseUnirefChunks.py {start} {stop}\n")
    file.write(f"python3 /home1/06538/mschecht/repos/bioinformatic_tools/fasta_tools/parseUnirefChunks.py {highestMultiple*batchSize+1} {numFiles}\n")
